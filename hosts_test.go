package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRemotePolicy(t *testing.T) {
	a := &app{cfg: settings{NATS: "nats://remote.example:4222", Monitor: "https://remote.example"}}
	h := a.handler()
	for _, route := range []struct{ method, path string }{{"POST", "/api/streams"}, {"PUT", "/api/streams/TEST"}, {"DELETE", "/api/streams/TEST"}, {"POST", "/api/streams/TEST/purge"}, {"DELETE", "/api/streams/TEST/messages/1"}, {"POST", "/api/streams/import"}, {"POST", "/api/streams/import/preview"}} {
		req := httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`))
		req.Host = "127.0.0.1:8080"
		req.Header.Set("X-Nats-Desk", "1")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatalf("%s %s should be denied, got %d", route.method, route.path, w.Code)
		}
	}
	for _, s := range []string{"$JS.API.STREAM.DELETE.X", "$SYS.REQ.SERVER.PING", "_INBOX.reply"} {
		if remotePublishAllowed(s, nil) == nil {
			t.Fatal("reserved subject accepted", s)
		}
	}
	if remotePublishAllowed("orders.created", map[string]string{"nAtS-rOlLuP": "all"}) == nil {
		t.Fatal("rollup accepted")
	}
	if remotePublishAllowed("orders.created", map[string]string{"Content-Type": "application/json"}) != nil {
		t.Fatal("normal publish denied")
	}
	if canManage(a.cfg) || canManage(settings{NATS: "nats://127.0.0.1:4222", Monitor: "https://remote.example"}) {
		t.Fatal("remote classified local")
	}
	if !canManage(settings{NATS: "nats://localhost:4222", Monitor: "http://127.0.0.1:8222"}) {
		t.Fatal("local denied")
	}
}
func TestHostRoutingAndPersistence(t *testing.T) {
	monitor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"server_id": "LOCAL_FIXTURE"})
	}))
	defer monitor.Close()
	a := &app{cfg: settings{NATS: "nats://127.0.0.1:4222", Monitor: monitor.URL, Listen: "127.0.0.1:8080"}, client: monitor.Client()}
	file := filepath.Join(t.TempDir(), "hosts.json")
	hm, err := newHosts(a, file)
	if err != nil {
		t.Fatal(err)
	}
	if len(hm.profiles) != 1 || hm.profiles[0].ID != "local" {
		t.Fatal("fresh installs must only include the local profile")
	}
	remote := hostProfile{"remote-fixture", "Remote fixture", "nats://remote.example:4222", "https://remote.example"}
	hm.profiles = append(hm.profiles, remote)
	hm.apps[remote.ID] = &app{cfg: settings{NATS: remote.NATS, Monitor: remote.Monitor}, client: monitor.Client()}
	root := (&app{hosts: hm}).handler()
	request := func(method, path, host, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Host = "127.0.0.1:8080"
		req.Header.Set("X-Nats-Desk", "1")
		req.Header.Set("X-Nats-Host", host)
		w := httptest.NewRecorder()
		root.ServeHTTP(w, req)
		return w
	}
	if w := request("GET", "/api/overview", "missing", ""); w.Code != 404 {
		t.Fatal("unknown host must not fall back", w.Code)
	}
	if w := request("DELETE", "/api/streams/X", "remote-fixture", `{"confirm":"X"}`); w.Code != 403 {
		t.Fatal("dispatch bypassed remote policy", w.Code)
	}
	if w := request("GET", "/api/overview", "local", ""); !strings.Contains(w.Body.String(), "LOCAL_FIXTURE") {
		t.Fatal(w.Body.String())
	}
	w := request("POST", "/api/hosts", "local", `{"name":"Other","nats":"","monitor":"https://other.example"}`)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var p hostProfile
	json.Unmarshal(w.Body.Bytes(), &p)
	restored, err := newHosts(a, file)
	if err != nil || restored.apps[p.ID] == nil {
		t.Fatal("profile not persisted", err)
	}
	if err := validateProfile(hostProfile{Name: "Bad", NATS: "nats://user:secret@host:4222", Monitor: "https://host"}); err == nil {
		t.Fatal("credentials should not be persisted in URLs")
	}
}

func TestRemoteProfileCanPublish(t *testing.T) {
	if os.Getenv("NATS_DESK_INTEGRATION") != "1" {
		t.Skip("integration disabled")
	}
	// Use local NATS with a remote-policy configuration; never contact a real remote server.
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()
	subject := fmt.Sprintf("natsdesk_permission_test.%d", time.Now().UnixNano())
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	nc.Flush()
	a := &app{nc: nc, js: js, cfg: settings{NATS: "nats://remote.example:4222", Monitor: "https://remote.example"}}
	body, _ := json.Marshal(map[string]string{"subject": subject, "payload": "permission-test"})
	req := httptest.NewRequest("POST", "/api/publish", strings.NewReader(string(body)))
	req.Host = "127.0.0.1:8080"
	req.Header.Set("X-Nats-Desk", "1")
	w := httptest.NewRecorder()
	a.handler().ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil || string(msg.Data) != "permission-test" {
		t.Fatal("remote policy blocked ordinary publish", err)
	}
}
