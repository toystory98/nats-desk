package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	errMonitorHTML         = errors.New("the URL returned an HTML page, not NATS monitoring JSON. Use the NATS server's HTTP monitoring endpoint, usually on port 8222")
	errSnapshotFormat      = errors.New("monitoring snapshot format not recognized")
	errSnapshotNoConsumers = errors.New("consumer details are not available from the monitoring snapshot")
	errNoAccountReply      = errors.New("no NATS account statistics received")
)

const monitorConnectionLimit = 100

func (a *app) monitor(ctx context.Context, path string) (any, error) {
	if a.snapshot.Load() {
		v, err := a.snapshotMonitor(ctx, path)
		if err != nil && !errors.Is(err, errSnapshotNoConsumers) && ctx.Err() == nil {
			a.snapshot.Store(false)
		}
		return v, err
	}
	v, err := a.fetchMonitor(ctx, path)
	if !errors.Is(err, errMonitorHTML) {
		return v, err
	}
	doc, e := a.fetchSnapshot(ctx)
	if e != nil {
		return nil, err
	}
	s, e := snapshotView(path, doc)
	if e != nil {
		return nil, e
	}
	a.snapshot.Store(true)
	return s, nil
}

func (a *app) fetchMonitor(ctx context.Context, path string) (any, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(a.cfg.Monitor, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	res, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	mt, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	html := mt == "text/html" || bytes.HasPrefix(bytes.TrimSpace(body), []byte("<"))
	switch {
	case html && res.StatusCode != 200:
		return nil, fmt.Errorf("monitoring HTTP %d: %w", res.StatusCode, errMonitorHTML)
	case html:
		return nil, errMonitorHTML
	case res.StatusCode != 200:
		return nil, fmt.Errorf("monitoring HTTP %d", res.StatusCode)
	}
	var v any
	err = json.Unmarshal(body, &v)
	return v, err
}

func (a *app) fetchSnapshot(ctx context.Context) (map[string]any, error) {
	v, err := a.fetchMonitor(ctx, "/api/snapshot")
	if err != nil {
		return nil, err
	}
	doc, ok := v.(map[string]any)
	if !ok {
		return nil, errSnapshotFormat
	}
	return doc, nil
}

func (a *app) snapshotMonitor(ctx context.Context, path string) (any, error) {
	doc, err := a.fetchSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshotView(path, doc)
}

func snapshotView(path string, doc map[string]any) (any, error) {
	endpoint, query, _ := strings.Cut(path, "?")
	switch endpoint {
	case "/varz":
		v, ok := doc["varz"].(map[string]any)
		if !ok {
			return nil, errSnapshotFormat
		}
		if _, ok = v["in_msgs"].(float64); !ok {
			return nil, errSnapshotFormat
		}
		out := maps.Clone(v)
		out["server_id"] = v["server_name"]
		return out, nil
	case "/connz":
		clients, ok := doc["clients"].([]any)
		if !ok {
			return nil, errSnapshotFormat
		}
		conns := []any{}
		for _, item := range clients {
			c, ok := item.(map[string]any)
			if !ok {
				return nil, errSnapshotFormat
			}
			if len(conns) < monitorConnectionLimit {
				conns = append(conns, map[string]any{"name": c["name"], "lang": c["lang"], "ip": c["node"], "account": c["account"], "num_subs": c["subscriptions"], "in_msgs": c["in_msgs"], "out_msgs": c["out_msgs"], "uptime": c["uptime"]})
			}
		}
		return map[string]any{"total": len(clients), "connections": conns}, nil
	case "/jsz":
		if q, _ := url.ParseQuery(query); q.Get("consumers") == "true" {
			return nil, errSnapshotNoConsumers
		}
		j, ok := doc["jsz"].(map[string]any)
		if !ok {
			return nil, errSnapshotFormat
		}
		return j, nil
	}
	return nil, errSnapshotFormat
}

func (a *app) accountPing(ctx context.Context, subject string, body []byte) ([][]byte, error) {
	if a.nc == nil || !a.nc.IsConnected() {
		return nil, errors.New("NATS is not connected")
	}
	inbox := a.nc.NewRespInbox()
	sub, err := a.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	if err = a.nc.PublishRequest(subject, inbox, body); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	replies := [][]byte{}
	gap := time.Duration(0)
	for {
		msg, err := nextReply(ctx, sub, gap)
		if err != nil || len(msg.Data) == 0 {
			break
		}
		replies = append(replies, msg.Data)
		gap = 300 * time.Millisecond
	}
	if len(replies) == 0 {
		return nil, errNoAccountReply
	}
	return replies, nil
}

func nextReply(ctx context.Context, sub *nats.Subscription, gap time.Duration) (*nats.Msg, error) {
	if gap > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, gap)
		defer cancel()
	}
	return sub.NextMsgWithContext(ctx)
}

type accountReply struct {
	Server struct {
		Name string `json:"name"`
	} `json:"server"`
	Error *struct {
		Description string `json:"description"`
	} `json:"error"`
}

func (a *app) accountStats(ctx context.Context) (map[string]any, error) {
	replies, err := a.accountPing(ctx, "$SYS.REQ.ACCOUNT.PING.STATZ", []byte(`{"include_unused":true}`))
	if err != nil {
		return nil, err
	}
	var account string
	var inMsgs, outMsgs, conns, subs int64
	nodes := []string{}
	for _, b := range replies {
		var r struct {
			accountReply
			Data struct {
				Accounts []struct {
					Account       string `json:"acc"`
					Conns         int64  `json:"conns"`
					Subscriptions int64  `json:"num_subscriptions"`
					Received      struct {
						Msgs int64 `json:"msgs"`
					} `json:"received"`
					Sent struct {
						Msgs int64 `json:"msgs"`
					} `json:"sent"`
				} `json:"account_statz"`
			} `json:"data"`
		}
		if err = json.Unmarshal(b, &r); err != nil {
			return nil, err
		}
		if r.Error != nil {
			return nil, errors.New(r.Error.Description)
		}
		if len(r.Data.Accounts) > 0 {
			nodes = append(nodes, r.Server.Name)
		}
		for _, s := range r.Data.Accounts {
			account = s.Account
			inMsgs += s.Received.Msgs
			outMsgs += s.Sent.Msgs
			conns += s.Conns
			subs += s.Subscriptions
		}
	}
	if len(nodes) == 0 {
		return nil, errNoAccountReply
	}
	slices.Sort(nodes)
	return map[string]any{"server_id": "account:" + account + "@" + strings.Join(nodes, ","), "server_name": account, "in_msgs": inMsgs, "out_msgs": outMsgs, "connections": conns, "subscriptions": subs}, nil
}

func (a *app) accountConnections(ctx context.Context) (map[string]any, error) {
	replies, err := a.accountPing(ctx, "$SYS.REQ.ACCOUNT.PING.CONNZ", []byte(fmt.Sprintf(`{"subscriptions_detail":true,"limit":%d}`, monitorConnectionLimit)))
	if err != nil {
		return nil, err
	}
	total := 0
	conns := []json.RawMessage{}
	for _, b := range replies {
		var r struct {
			accountReply
			Data struct {
				Total       int               `json:"total"`
				Connections []json.RawMessage `json:"connections"`
			} `json:"data"`
		}
		if err = json.Unmarshal(b, &r); err != nil {
			return nil, err
		}
		if r.Error != nil {
			return nil, errors.New(r.Error.Description)
		}
		total += r.Data.Total
		for _, c := range r.Data.Connections {
			if len(conns) < monitorConnectionLimit {
				conns = append(conns, c)
			}
		}
	}
	return map[string]any{"total": total, "connections": conns}, nil
}

func (a *app) monitorOverview(ctx context.Context, out map[string]any) {
	v, err := a.monitor(ctx, "/varz")
	if err == nil {
		out["server_stats"] = v
		if c, e := a.monitor(ctx, fmt.Sprintf("/connz?subs=detail&limit=%d", monitorConnectionLimit)); e == nil {
			out["connections"] = c
		}
		return
	}
	if a.nc == nil {
		out["monitor_error"] = err.Error()
		return
	}
	var conns map[string]any
	var connsErr error
	var wg sync.WaitGroup
	wg.Go(func() { conns, connsErr = a.accountConnections(ctx) })
	stats, e := a.accountStats(ctx)
	wg.Wait()
	if e != nil {
		out["monitor_error"] = fmt.Sprintf("%v; NATS account statistics fallback failed: %v", err, e)
		return
	}
	out["server_stats"] = stats
	out["monitor_warning"] = fmt.Sprintf("Monitoring URL unavailable (%v). Showing account statistics from the NATS client connection instead.", err)
	if connsErr == nil {
		out["connections"] = conns
	}
}
