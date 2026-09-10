package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestSubjectValidation(t *testing.T) {
	for _, tc := range []struct {
		s        string
		wild, ok bool
	}{{"orders.created", false, true}, {"orders.*", true, true}, {"orders.>", true, true}, {">", true, true}, {"orders.>", false, false}, {"orders..created", true, false}, {"orders.>.created", true, false}, {"orders. a", true, false}, {"orders.x*", true, false}, {"", true, false}} {
		if got := validSubject(tc.s, tc.wild); got != tc.ok {
			t.Errorf("subject %q: got %v", tc.s, got)
		}
	}
}
func TestMessagePreview(t *testing.T) {
	m := view(1, "a", time.Now(), nil, []byte{0xff, 0x00})
	if m.Encoding != "base64" || m.Payload != "/wA=" {
		t.Fatal(m)
	}
	m = view(1, "a", time.Now(), nil, bytes.Repeat([]byte("x"), 70000))
	if !m.Truncated || m.Size != 70000 || len(m.Payload) != 65536 {
		t.Fatal("incorrect bounded preview")
	}
}
func TestLocalBoundary(t *testing.T) {
	a := &app{}
	h := a.handler()
	for _, tc := range []struct {
		host, origin, header string
		status               int
	}{{"evil.test:8080", "", "", 403}, {"127.0.0.1:8080", "http://evil.test", "1", 403}, {"127.0.0.1:8080", "", "", 403}} {
		req := httptest.NewRequest("POST", "/api/publish", strings.NewReader(`{}`))
		req.Host = tc.host
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("X-Nats-Desk", tc.header)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Fatalf("got %d want %d", w.Code, tc.status)
		}
	}
}

func TestOverviewRequestsDetailedSubscriptions(t *testing.T) {
	detailed := false
	monitor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connz" {
			detailed = r.URL.Query().Get("subs") == "detail"
			json.NewEncoder(w).Encode(map[string]any{"connections": []any{}, "total": 0})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer monitor.Close()
	a := &app{cfg: settings{Monitor: monitor.URL}, client: monitor.Client()}
	req := httptest.NewRequest("GET", "/api/overview", nil)
	w := httptest.NewRecorder()
	a.overview(w, req)
	if w.Code != http.StatusOK || !detailed {
		t.Fatal("overview did not request detailed subscriptions", w.Code)
	}
}

func TestSummarizeConsumer(t *testing.T) {
	info := &nats.ConsumerInfo{
		Stream: "ACCOUNTING_SAP", Name: "accounting-worker",
		Config:     nats.ConsumerConfig{FilterSubject: "evt.*.accounting.>"},
		NumWaiting: 2, NumPending: 3, NumAckPending: 1,
	}
	got := summarizeConsumer("$G", info.Stream, []string{"evt.>"}, info)
	if got.Stream != "ACCOUNTING_SAP" || got.Name != "accounting-worker" || got.Mode != "pull" || got.NumWaiting != 2 || got.NumPending != 3 || got.NumAckPending != 1 {
		t.Fatalf("unexpected consumer summary: %#v", got)
	}
	if len(got.Subjects) != 1 || got.Subjects[0] != "evt.*.accounting.>" {
		t.Fatalf("unexpected consumer subjects: %#v", got.Subjects)
	}
}

func TestMonitorConsumers(t *testing.T) {
	details := false
	monitor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		details = r.URL.Path == "/jsz" && r.URL.Query().Get("consumers") == "true" && r.URL.Query().Get("config") == "true"
		io.WriteString(w, `{"account_details":[{"name":"$G","stream_detail":[{"name":"ORDERS","config":{"subjects":["orders.>"]},"consumer_detail":[{"stream_name":"ORDERS","name":"worker","config":{"filter_subject":"orders.new"},"num_waiting":2}]}]}]}`)
	}))
	defer monitor.Close()
	a := &app{cfg: settings{Monitor: monitor.URL}, client: monitor.Client()}
	req := httptest.NewRequest("GET", "/api/consumers", nil)
	req.Host = "127.0.0.1:8080"
	w := httptest.NewRecorder()
	a.handler().ServeHTTP(w, req)
	var rows []consumerSummary
	err := json.Unmarshal(w.Body.Bytes(), &rows)
	if w.Code != http.StatusOK || err != nil || !details || len(rows) != 1 || rows[0].Name != "worker" || rows[0].NumWaiting != 2 {
		t.Fatalf("unexpected monitor consumers response: status=%d details=%v rows=%#v error=%v", w.Code, details, rows, err)
	}
}

func TestJetStreamLifecycle(t *testing.T) {
	if os.Getenv("NATS_DESK_INTEGRATION") != "1" {
		t.Skip("set NATS_DESK_INTEGRATION=1 to test against local NATS")
	}
	nc, err := nats.Connect("nats://127.0.0.1:4222", nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	a := &app{nc: nc, js: js, cfg: settings{NATS: "nats://127.0.0.1:4222", Monitor: "http://127.0.0.1:8222"}, client: &http.Client{Timeout: 3 * time.Second}}
	h := a.handler()
	call := func(method, path string, body any, status int) map[string]any {
		t.Helper()
		b, _ := json.Marshal(body)
		req := httptest.NewRequest(method, path, bytes.NewReader(b))
		req.Host = "127.0.0.1:8080"
		req.Header.Set("X-Nats-Desk", "1")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != status {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		var out map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	name := fmt.Sprintf("NATSDESK_TEST_%d", time.Now().UnixNano())
	subject := "natsdesk_test." + name
	// Cleanup applies only to the unique stream this test creates.
	defer js.DeleteStream(name)
	in := streamInput{Name: name, Description: "Integration test", Subjects: []string{subject + ".>"}, Storage: "memory", Retention: "limits", MaxAge: "1h", MaxMsgs: 100, MaxBytes: 1048576}
	call("POST", "/api/streams", in, 200)
	// Set a configuration option outside the editor, then verify the edit preserves it.
	original, err := js.StreamInfo(name)
	if err != nil {
		t.Fatal(err)
	}
	original.Config.MaxMsgsPerSubject = 80
	original.Config.Discard = nats.DiscardNew
	if _, err = js.UpdateStream(&original.Config); err != nil {
		t.Fatal(err)
	}
	detail := call("GET", "/api/streams/"+name, nil, 200)
	in.Version = detail["version"].(string)
	in.Description = "Updated"
	in.Subjects = []string{subject + ".>", subject + "_extra.>"}
	call("PUT", "/api/streams/"+name, in, 200)
	updated, err := js.StreamInfo(name)
	if err != nil || updated.Config.MaxMsgsPerSubject != 80 || updated.Config.Discard != nats.DiscardNew {
		t.Fatal("editor lost unexposed config", err)
	}
	call("PUT", "/api/streams/"+name, in, 400) // stale edit rejected
	for i := 0; i < 3; i++ {
		call("POST", "/api/publish", map[string]any{"stream": name, "subject": subject + ".created", "payload": fmt.Sprintf(`{"id":%d}`, i), "headers": map[string]string{"Content-Type": "application/json"}}, 200)
	}
	subjects := call("GET", "/api/streams/"+name+"/subjects", nil, 200)
	observed := subjects["observed"].([]any)
	if len(observed) != 1 || observed[0].(map[string]any)["subject"] != subject+".created" || observed[0].(map[string]any)["messages"] != float64(3) {
		t.Fatal("observed subject list is incorrect", subjects)
	}
	call("DELETE", "/api/streams/"+name+"/messages/2", map[string]string{"confirm": "wrong"}, 400)
	call("DELETE", "/api/streams/"+name+"/messages/2", map[string]string{"confirm": name + "/2"}, 200)
	result := call("GET", "/api/streams/"+name+"/messages?subject="+subject+".>", nil, 200)
	msgs := result["messages"].([]any)
	if len(msgs) != 2 || msgs[1].(map[string]any)["sequence"].(float64) != 3 {
		t.Fatal("deleted sequence gap not handled", result)
	}
	state, err := js.StreamInfo(name)
	if err != nil || state.State.Consumers != 0 || state.State.Msgs != 2 {
		t.Fatal("browsing consumed messages or created consumers", err)
	}
	call("POST", "/api/publish", map[string]string{"stream": name, "subject": "bad.*", "payload": "bad"}, 400)
	call("POST", "/api/streams/"+name+"/purge", map[string]string{"confirm": name}, 200)
	state, err = js.StreamInfo(name)
	if err != nil || state.State.Msgs != 0 {
		t.Fatal("purge failed", err)
	}
	call("DELETE", "/api/streams/"+name, map[string]string{"confirm": name}, 200)
}

func TestLiveCapture(t *testing.T) {
	if os.Getenv("NATS_DESK_INTEGRATION") != "1" {
		t.Skip("integration disabled")
	}
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()
	a := &app{nc: nc, js: js, cfg: settings{NATS: "nats://127.0.0.1:4222", Monitor: "http://127.0.0.1:8222"}}
	srv := httptest.NewServer(a.handler())
	defer srv.Close()
	subject := fmt.Sprintf("natsdesk_live_test.%d", time.Now().UnixNano())
	client := http.Client{Timeout: 5 * time.Second}
	res, err := client.Get(srv.URL + "/api/live?subject=" + subject)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal(res.Status)
	}
	if err = nc.Publish(subject, []byte("hello-live")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8192)
	n, err := res.Body.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if !strings.Contains(string(buf[:n]), "hello-live") {
		t.Fatalf("missing live event: %s", buf[:n])
	}
}
