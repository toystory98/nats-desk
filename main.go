package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
)

//go:embed web/*
var assets embed.FS

type settings struct {
	NATS    string `json:"nats"`
	Monitor string `json:"monitor"`
	Listen  string `json:"listen"`
}
type app struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	cfg    settings
	client *http.Client
	hosts  *hostManager
}

func main() {
	parentPID := flag.Int("parent-pid", 0, "Exit when the desktop parent process ends")
	config := flag.String("config", "", "Optional JSON settings file")
	natsURL := flag.String("nats", "", "NATS connection URL")
	monitor := flag.String("monitor", "", "NATS monitoring URL")
	profiles := flag.String("profiles", "hosts.json", "Saved host profiles (credentials are not stored)")
	listen := flag.String("listen", "", "Loopback HTTP address")
	flag.Parse()
	cfg := settings{"nats://127.0.0.1:4222", "http://127.0.0.1:8222", "127.0.0.1:8080"}
	if *config != "" {
		b, err := os.ReadFile(*config)
		if err != nil {
			log.Fatal(err)
		}
		if err = json.Unmarshal(b, &cfg); err != nil {
			log.Fatal(err)
		}
	}
	if *natsURL != "" {
		cfg.NATS = *natsURL
	}
	if *monitor != "" {
		cfg.Monitor = *monitor
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	host, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		log.Fatal("listen must use a loopback IP, for example 127.0.0.1:8080")
	}
	nc, err := nats.Connect(cfg.NATS, nats.Name("NATS Desk"), nats.Timeout(3*time.Second), nats.MaxReconnects(-1), nats.RetryOnFailedConnect(true))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(4 * time.Second))
	if err != nil {
		log.Fatal(err)
	}
	a := &app{nc: nc, js: js, cfg: cfg, client: &http.Client{Timeout: 3 * time.Second}}
	hm, err := newHosts(a, *profiles)
	if err != nil {
		log.Fatal(err)
	}
	defer hm.close()
	root := &app{hosts: hm}
	srv := &http.Server{Addr: cfg.Listen, Handler: root.handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *parentPID > 0 {
		go func() {
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if os.Getppid() != *parentPID {
						stop()
						return
					}
				}
			}
		}()
	}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(c)
	}()
	log.Printf("NATS Desk → http://%s", cfg.Listen)
	if err = srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
func (a *app) handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /api/overview", a.overview)
	m.HandleFunc("GET /api/streams", a.streams)
	m.HandleFunc("POST /api/streams", a.saveStream)
	m.HandleFunc("POST /api/streams/import/preview", a.previewImport)
	m.HandleFunc("POST /api/streams/import", a.createImport)
	m.HandleFunc("PUT /api/streams/{name}", a.saveStream)
	m.HandleFunc("GET /api/streams/{name}", a.stream)
	m.HandleFunc("DELETE /api/streams/{name}", a.deleteStream)
	m.HandleFunc("POST /api/streams/{name}/purge", a.purgeStream)
	m.HandleFunc("GET /api/streams/{name}/messages", a.messages)
	m.HandleFunc("DELETE /api/streams/{name}/messages/{seq}", a.deleteMessage)
	m.HandleFunc("POST /api/publish", a.publish)
	m.HandleFunc("GET /api/live", a.live)
	web, _ := fs.Sub(assets, "web")
	m.Handle("/", http.FileServer(http.FS(web)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = r.Host
		}
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			http.Error(w, "Local access only", 403)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Host != r.Host || u.Scheme != "http" {
				http.Error(w, "Cross-origin request rejected", 403)
				return
			}
		}
		if strings.HasPrefix(r.URL.Path, "/api/") && r.Method != "GET" && r.Header.Get("X-Nats-Desk") != "1" {
			http.Error(w, "Missing local dashboard header", 403)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'")
		w.Header().Set("Cache-Control", "no-store")
		if strings.HasPrefix(r.URL.Path, "/api/") && r.URL.Path != "/api/live" {
			ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
			defer cancel()
			r = r.WithContext(ctx)
		}
		if a.hosts != nil {
			a.hosts.serve(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			if !canManage(a.cfg) && r.Method != "GET" && !(r.Method == "POST" && r.URL.Path == "/api/publish") {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				json.NewEncoder(w).Encode(map[string]string{"error": "Remote host: viewing and publishing only. Management operations are blocked."})
				return
			}
			if a.nc == nil && r.URL.Path != "/api/overview" && r.URL.Path != "/api/streams" {
				fail(w, errors.New("Configure and connect the NATS client URL for this host to view or publish messages"))
				return
			}
		}
		m.ServeHTTP(w, r)
	})
}
func respond(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(400)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		fail(w, err)
		return false
	}
	if d.Decode(new(any)) != io.EOF {
		fail(w, errors.New("expected one JSON value"))
		return false
	}
	return true
}
func (a *app) monitor(ctx context.Context, path string) (any, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(a.cfg.Monitor, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	res, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("monitoring HTTP %d", res.StatusCode)
	}
	var v any
	err = json.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(&v)
	return v, err
}
func (a *app) overview(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{"connected": a.nc != nil && a.nc.IsConnected(), "status": "MONITOR ONLY", "time": time.Now(), "server": a.cfg.Monitor, "can_manage": canManage(a.cfg)}
	if a.nc != nil {
		out["status"] = a.nc.Status().String()
		out["server"] = a.nc.ConnectedAddr()
	}
	v, e := a.monitor(r.Context(), "/varz")
	if e != nil {
		out["monitor_error"] = e.Error()
	} else {
		out["server_stats"] = v
	}
	v, e = a.monitor(r.Context(), "/connz?subs=1&limit=100")
	if e == nil {
		out["connections"] = v
	}
	if a.nc != nil && a.nc.IsConnected() {
		v, e = a.js.AccountInfo(nats.Context(r.Context()))
		if e != nil {
			out["jetstream_error"] = e.Error()
		} else {
			out["jetstream"] = v
		}
	}
	if a.nc == nil {
		if j, err := a.monitor(r.Context(), "/jsz"); err == nil {
			out["jetstream"] = j
		}
	}
	respond(w, out)
}
func (a *app) streams(w http.ResponseWriter, r *http.Request) {
	if a.nc == nil {
		a.monitorStreams(w, r)
		return
	}
	// Paged API requests expose errors (including missing JetStream) instead of returning a false empty list.
	list := []json.RawMessage{}
	offset := 0
	for {
		b, _ := json.Marshal(map[string]int{"offset": offset})
		msg, err := a.nc.RequestWithContext(r.Context(), "$JS.API.STREAM.LIST", b)
		if err != nil {
			fail(w, err)
			return
		}
		var page struct {
			Streams []json.RawMessage `json:"streams"`
			Total   int               `json:"total"`
			Error   *struct {
				Description string `json:"description"`
			} `json:"error"`
		}
		if err = json.Unmarshal(msg.Data, &page); err != nil {
			fail(w, err)
			return
		}
		if page.Error != nil {
			fail(w, errors.New(page.Error.Description))
			return
		}
		list = append(list, page.Streams...)
		offset += len(page.Streams)
		if offset >= page.Total || len(page.Streams) == 0 {
			break
		}
	}
	respond(w, list)
}
func version(c nats.StreamConfig) string {
	b, _ := json.Marshal(c)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (a *app) stream(w http.ResponseWriter, r *http.Request) {
	s, err := a.js.StreamInfo(r.PathValue("name"), nats.Context(r.Context()))
	if err != nil {
		fail(w, err)
		return
	}
	consumers := []*nats.ConsumerInfo{}
	for c := range a.js.ConsumersInfo(s.Config.Name, nats.Context(r.Context())) {
		consumers = append(consumers, c)
	}
	respond(w, map[string]any{"stream": s, "version": version(s.Config), "consumers": consumers})
}

type streamInput struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Subjects    []string `json:"subjects"`
	Storage     string   `json:"storage"`
	Retention   string   `json:"retention"`
	MaxAge      string   `json:"max_age"`
	MaxMsgs     int64    `json:"max_messages"`
	MaxBytes    int64    `json:"max_bytes"`
	Version     string   `json:"version"`
}

func (a *app) saveStream(w http.ResponseWriter, r *http.Request) {
	var in streamInput
	if !decode(w, r, &in) {
		return
	}
	if in.Name == "" || strings.ContainsAny(in.Name, ".*>/\\ \t\r\n") {
		fail(w, errors.New("enter a stream name without spaces, dots or wildcards"))
		return
	}
	if len(in.Subjects) == 0 {
		fail(w, errors.New("at least one subject pattern is required"))
		return
	}
	for _, s := range in.Subjects {
		if !validSubject(s, true) {
			fail(w, fmt.Errorf("invalid subject pattern: %s", s))
			return
		}
	}
	age, err := time.ParseDuration(in.MaxAge)
	if err != nil || age < 0 {
		fail(w, errors.New("max age must be a duration such as 24h or 0s"))
		return
	}
	if in.MaxMsgs < -1 || in.MaxBytes < -1 {
		fail(w, errors.New("limits must be -1 (unlimited) or nonnegative"))
		return
	}
	c := nats.StreamConfig{Name: in.Name, Replicas: 1}
	if r.Method == "PUT" {
		if r.PathValue("name") != in.Name {
			fail(w, errors.New("stream names cannot be changed"))
			return
		}
		old, e := a.js.StreamInfo(in.Name, nats.Context(r.Context()))
		if e != nil {
			fail(w, e)
			return
		}
		if version(old.Config) != in.Version {
			fail(w, errors.New("stream configuration changed; reopen the editor before saving"))
			return
		}
		c = old.Config
	}
	c.Description = in.Description
	c.Subjects = in.Subjects
	c.MaxAge = age
	c.MaxMsgs = in.MaxMsgs
	c.MaxBytes = in.MaxBytes
	switch in.Storage {
	case "file":
		c.Storage = nats.FileStorage
	case "memory":
		c.Storage = nats.MemoryStorage
	default:
		fail(w, errors.New("invalid storage"))
		return
	}
	switch in.Retention {
	case "limits":
		c.Retention = nats.LimitsPolicy
	case "interest":
		c.Retention = nats.InterestPolicy
	case "workqueue":
		c.Retention = nats.WorkQueuePolicy
	default:
		fail(w, errors.New("invalid retention"))
		return
	}
	var s *nats.StreamInfo
	if r.Method == "PUT" {
		s, err = a.js.UpdateStream(&c, nats.Context(r.Context()))
	} else {
		s, err = a.js.AddStream(&c, nats.Context(r.Context()))
	}
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, s)
}
func confirm(w http.ResponseWriter, r *http.Request, target string) bool {
	var in struct {
		Confirm string `json:"confirm"`
	}
	if !decode(w, r, &in) {
		return false
	}
	if in.Confirm != target {
		fail(w, fmt.Errorf("confirmation must match %s", target))
		return false
	}
	return true
}
func (a *app) deleteStream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !confirm(w, r, name) {
		return
	}
	if err := a.js.DeleteStream(name, nats.Context(r.Context())); err != nil {
		fail(w, err)
		return
	}
	respond(w, map[string]bool{"ok": true})
}
func (a *app) purgeStream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !confirm(w, r, name) {
		return
	}
	if err := a.js.PurgeStream(name, nats.Context(r.Context())); err != nil {
		fail(w, err)
		return
	}
	respond(w, map[string]bool{"ok": true})
}

type messageView struct {
	Sequence  uint64      `json:"sequence"`
	Subject   string      `json:"subject"`
	Time      time.Time   `json:"time"`
	Headers   nats.Header `json:"headers"`
	Payload   string      `json:"payload"`
	Encoding  string      `json:"encoding"`
	Size      int         `json:"size"`
	Truncated bool        `json:"truncated"`
}

func view(seq uint64, subject string, t time.Time, h nats.Header, b []byte) messageView {
	m := messageView{Sequence: seq, Subject: subject, Time: t, Headers: h, Size: len(b), Encoding: "text"}
	if len(b) > 65536 {
		b = b[:65536]
		m.Truncated = true
	}
	if utf8.Valid(b) {
		m.Payload = string(b)
	} else {
		m.Payload = base64.StdEncoding.EncodeToString(b)
		m.Encoding = "base64"
	}
	return m
}
func (a *app) messages(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s, err := a.js.StreamInfo(name, nats.Context(r.Context()))
	if err != nil {
		fail(w, err)
		return
	}
	start := s.State.FirstSeq
	if q := r.URL.Query().Get("start"); q != "" {
		start, err = strconv.ParseUint(q, 10, 64)
		if err != nil {
			fail(w, errors.New("invalid starting sequence"))
			return
		}
	}
	pattern := r.URL.Query().Get("subject")
	if pattern == "" {
		pattern = ">"
	}
	if !validSubject(pattern, true) {
		fail(w, errors.New("invalid subject filter"))
		return
	}
	if start < s.State.FirstSeq {
		start = s.State.FirstSeq
	}
	list := []messageView{}
	next := uint64(0)
	// GetNextMsg skips deleted sequence gaps and never creates/acks a consumer.
	for len(list) < 50 && start > 0 && start <= s.State.LastSeq {
		m, e := a.nextMessage(r.Context(), name, pattern, start)
		if errors.Is(e, nats.ErrMsgNotFound) {
			break
		}
		if e != nil {
			fail(w, e)
			return
		}
		list = append(list, view(m.Sequence, m.Subject, m.Time, m.Header, m.Data))
		start = m.Sequence + 1
		if start <= s.State.LastSeq {
			next = start
		} else {
			next = 0
		}
	}
	if len(list) < 50 {
		next = 0
	}
	respond(w, map[string]any{"messages": list, "next": next, "state": s.State})
}
func (a *app) deleteMessage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	seq, err := strconv.ParseUint(r.PathValue("seq"), 10, 64)
	if err != nil || seq == 0 {
		fail(w, errors.New("invalid message sequence"))
		return
	}
	if !confirm(w, r, fmt.Sprintf("%s/%d", name, seq)) {
		return
	}
	if err = a.js.DeleteMsg(name, seq, nats.Context(r.Context())); err != nil {
		fail(w, err)
		return
	}
	respond(w, map[string]bool{"ok": true})
}
func validSubject(s string, wild bool) bool {
	if s == "" || strings.ContainsAny(s, " \t\r\n") {
		return false
	}
	parts := strings.Split(s, ".")
	for i, p := range parts {
		if p == "" {
			return false
		}
		if wild && p == "*" {
			continue
		}
		if wild && p == ">" && i == len(parts)-1 {
			continue
		}
		if strings.ContainsAny(p, "*>") {
			return false
		}
	}
	return true
}
func (a *app) publish(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Subject string            `json:"subject"`
		Payload string            `json:"payload"`
		Headers map[string]string `json:"headers"`
		Stream  string            `json:"stream"`
	}
	if !decode(w, r, &in) {
		return
	}
	if !validSubject(in.Subject, false) {
		fail(w, errors.New("publish requires an exact subject without wildcards"))
		return
	}
	if !canManage(a.cfg) {
		if err := remotePublishAllowed(in.Subject, in.Headers); err != nil {
			fail(w, err)
			return
		}
	}
	m := nats.NewMsg(in.Subject)
	m.Data = []byte(in.Payload)
	for k, v := range in.Headers {
		if k == "" || strings.ContainsAny(k, ":\r\n") || strings.ContainsAny(v, "\r\n") {
			fail(w, errors.New("invalid header"))
			return
		}
		m.Header.Set(k, v)
	}
	if in.Stream != "" {
		ack, err := a.js.PublishMsg(m, nats.ExpectStream(in.Stream), nats.Context(r.Context()))
		if err != nil {
			fail(w, err)
			return
		}
		respond(w, ack)
	} else {
		if err := a.nc.PublishMsg(m); err != nil {
			fail(w, err)
			return
		}
		if err := a.nc.FlushWithContext(r.Context()); err != nil {
			fail(w, err)
			return
		}
		respond(w, map[string]string{"status": "published", "note": "Core NATS publish; persistence not confirmed"})
	}
}
func (a *app) live(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("subject")
	if !validSubject(pattern, true) {
		fail(w, errors.New("enter a valid subject pattern"))
		return
	}
	f, ok := w.(http.Flusher)
	if !ok {
		fail(w, errors.New("streaming unavailable"))
		return
	}
	queue := make(chan messageView, 128)
	var dropped atomic.Uint64
	sub, err := a.nc.Subscribe(pattern, func(m *nats.Msg) {
		v := view(0, m.Subject, time.Now(), m.Header, m.Data)
		select {
		case queue <- v:
		default:
			dropped.Add(1)
		}
	})
	if err != nil {
		fail(w, err)
		return
	}
	defer sub.Unsubscribe()
	sub.SetPendingLimits(256, 8<<20)
	flushCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err = a.nc.FlushWithContext(flushCtx); err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	f.Flush()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	send := func(event string, v any) error {
		http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second))
		b, _ := json.Marshal(v)
		_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
		f.Flush()
		return err
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case v := <-queue:
			if send("message", v) != nil {
				return
			}
		case <-tick.C:
			d, _ := sub.Dropped()
			if send("status", map[string]any{"dropped": dropped.Load() + uint64(max(d, 0)), "connected": a.nc.IsConnected()}) != nil {
				return
			}
		}
	}
}

// nextMessage uses the standard stream API, so browsing also works when direct
// access is disabled. next_by_subj skips holes without modifying the stream.
func (a *app) nextMessage(ctx context.Context, name, subject string, seq uint64) (*nats.RawStreamMsg, error) {
	b, _ := json.Marshal(map[string]any{"seq": seq, "next_by_subj": subject})
	m, err := a.nc.RequestWithContext(ctx, "$JS.API.STREAM.MSG.GET."+name, b)
	if err != nil {
		return nil, err
	}
	var res struct {
		Error   *nats.APIError `json:"error"`
		Message *struct {
			Subject string    `json:"subject"`
			Seq     uint64    `json:"seq"`
			Data    []byte    `json:"data"`
			Headers []byte    `json:"hdrs"`
			Time    time.Time `json:"time"`
		} `json:"message"`
	}
	if err = json.Unmarshal(m.Data, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, res.Error
	}
	if res.Message == nil {
		return nil, errors.New("empty message response")
	}
	v := res.Message
	var h nats.Header
	if len(v.Headers) > 0 {
		h, err = nats.DecodeHeadersMsg(v.Headers)
		if err != nil {
			return nil, err
		}
	}
	return &nats.RawStreamMsg{Subject: v.Subject, Sequence: v.Seq, Data: v.Data, Header: h, Time: v.Time}, nil
}
