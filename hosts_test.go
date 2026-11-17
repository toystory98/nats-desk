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
	routes := []struct{ method, path string }{{"POST", "/api/publish"}, {"POST", "/api/streams"}, {"PUT", "/api/streams/TEST"}, {"DELETE", "/api/streams/TEST"}, {"POST", "/api/streams/TEST/purge"}, {"DELETE", "/api/streams/TEST/messages/1"}, {"POST", "/api/streams/import"}, {"POST", "/api/streams/import/preview"}}
	for _, tc := range []struct {
		role  string
		allow map[string]bool
	}{{"reader", map[string]bool{}}, {"writer", map[string]bool{"/api/publish": true}}, {"admin", map[string]bool{"/api/publish": true, "/api/streams": true, "/api/streams/TEST": true, "/api/streams/TEST/purge": true, "/api/streams/TEST/messages/1": true, "/api/streams/import": true, "/api/streams/import/preview": true}}} {
		h := (&app{cfg: a.cfg, role: tc.role}).handler()
		for _, route := range routes {
			req := httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`))
			req.Host = "127.0.0.1:8080"
			req.Header.Set("X-Nats-Desk", "1")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if denied := w.Code == http.StatusForbidden; denied == tc.allow[route.path] {
				t.Fatalf("%s %s %s: got %d %s", tc.role, route.method, route.path, w.Code, w.Body.String())
			}
		}
	}
	for _, path := range []string{"/api/streams", "/api/streams/import"} {
		body := `{"name":"X","subjects":["x.>"],"storage":"memory","retention":"limits","max_age":"1h","max_messages":10,"max_bytes":-1}`
		if path == "/api/streams/import" {
			body = `{"yaml":"streams:\n  - name: X\n    subjects: [x.>]"}`
		}
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.Host = "127.0.0.1:8080"
		req.Header.Set("X-Nats-Desk", "1")
		w := httptest.NewRecorder()
		(&app{cfg: a.cfg, role: "admin"}).handler().ServeHTTP(w, req)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "Configure and connect") {
			t.Fatalf("admin without a NATS connection: POST %s got %d %s", path, w.Code, w.Body.String())
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
	remote := hostProfile{"remote-fixture", "Remote fixture", "nats://remote.example:4222", "https://remote.example", "reader"}
	hm.profiles = append(hm.profiles, remote)
	hm.apps[remote.ID] = &app{cfg: settings{NATS: remote.NATS, Monitor: remote.Monitor}, client: monitor.Client(), role: remote.Role}
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
	if w := request("GET", "/api/overview", "local", ""); !strings.Contains(w.Body.String(), "LOCAL_FIXTURE") || !strings.Contains(w.Body.String(), `"role":"admin"`) {
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
	w = request("PUT", "/api/hosts/"+p.ID, "local", `{"name":"Other updated","nats":"","monitor":"https://updated.example"}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Other updated") {
		t.Fatal("profile update failed", w.Code, w.Body.String())
	}
	restored, err = newHosts(a, file)
	var updated hostProfile
	if err == nil {
		for _, candidate := range restored.profiles {
			if candidate.ID == p.ID {
				updated = candidate
			}
		}
	}
	if err != nil || updated.Name != "Other updated" || updated.Monitor != "https://updated.example" {
		t.Fatal("profile update not persisted", err)
	}
	if w = request("DELETE", "/api/hosts/"+p.ID, "local", `{"confirm":"wrong"}`); w.Code != 400 {
		t.Fatal("profile delete accepted wrong confirmation", w.Code)
	}
	if w = request("DELETE", "/api/hosts/local", "local", `{"confirm":"Local NATS"}`); w.Code != 400 {
		t.Fatal("built-in local profile was deleted", w.Code)
	}
	if w = request("DELETE", "/api/hosts/"+p.ID, "local", `{"confirm":"Other updated"}`); w.Code != 200 {
		t.Fatal("profile delete failed", w.Code, w.Body.String())
	}
	restored, err = newHosts(a, file)
	if err != nil || restored.apps[p.ID] != nil {
		t.Fatal("profile delete not persisted", err)
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
	a := &app{nc: nc, js: js, cfg: settings{NATS: "nats://remote.example:4222", Monitor: "https://remote.example"}, role: "writer"}
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

func TestHostRoles(t *testing.T) {
	if defaultRole("nats://127.0.0.1:4222", "http://localhost:8222") != "admin" || defaultRole("nats://dev.example:4222", "https://dev.example") != "reader" || defaultRole("", "http://127.0.0.1:8222") != "reader" {
		t.Fatal("default roles must be admin for loopback hosts and reader otherwise")
	}
	if validateProfile(hostProfile{Name: "X", Monitor: "https://x.example", Role: "owner"}) == nil || validateProfile(hostProfile{Name: "X", Monitor: "https://x.example", Role: "writer"}) != nil {
		t.Fatal("role validation")
	}
	file := filepath.Join(t.TempDir(), "hosts.json")
	legacy := `[{"id":"local","name":"Local NATS","nats":"nats://127.0.0.1:4222","monitor":"http://127.0.0.1:8222"},{"id":"dev","name":"Dev","nats":"nats://dev.example:4222","monitor":"https://dev.example"}]`
	if err := os.WriteFile(file, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	a := &app{cfg: settings{NATS: "nats://127.0.0.1:4222", Monitor: "http://127.0.0.1:8222", Listen: "127.0.0.1:8080"}}
	hm, err := newHosts(a, file)
	if err != nil {
		t.Fatal(err)
	}
	if hm.profiles[0].Role != "admin" || hm.apps["local"].role != "admin" || hm.profiles[1].Role != "reader" || hm.apps["dev"].role != "reader" {
		t.Fatal("legacy profiles must get default roles", hm.profiles)
	}
	conn := &nats.Conn{}
	hm.apps["dev"] = &app{nc: conn, cfg: hm.apps["dev"].cfg, role: "reader"}
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
	if w := request("POST", "/api/publish", "dev", `{}`); w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "Reader access") {
		t.Fatal("reader published", w.Code, w.Body.String())
	}
	if w := request("PUT", "/api/hosts/dev", "local", `{"name":"Dev","nats":"nats://dev.example:4222","monitor":"https://dev.example","role":"owner"}`); w.Code != 400 {
		t.Fatal("invalid role accepted", w.Code)
	}
	if w := request("PUT", "/api/hosts/dev", "local", `{"name":"Dev","nats":"nats://dev.example:4222","monitor":"https://dev.example","role":"writer"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if hm.apps["dev"].role != "writer" || hm.apps["dev"].nc != conn {
		t.Fatal("role change must apply at once and keep the NATS connection")
	}
	if w := request("POST", "/api/publish", "dev", `{}`); w.Code == http.StatusForbidden {
		t.Fatal("writer blocked from publishing", w.Body.String())
	}
	if w := request("POST", "/api/streams", "dev", `{}`); w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "Writer access") {
		t.Fatal("writer managed streams", w.Code, w.Body.String())
	}
	headReq := httptest.NewRequest("HEAD", "/api/streams/TEST/messages", nil)
	headReq.Host = "127.0.0.1:8080"
	headReq.Header.Set("X-Nats-Desk", "1")
	headRec := httptest.NewRecorder()
	(&app{cfg: hm.apps["dev"].cfg, role: "reader"}).handler().ServeHTTP(headRec, headReq)
	if headRec.Code == http.StatusForbidden {
		t.Fatal("HEAD is a read and must pass the role check")
	}
	localWriter := (&app{nc: conn, cfg: a.cfg, role: "writer"}).handler()
	for _, body := range []string{`{"subject":"$JS.API.STREAM.DELETE.X","payload":""}`, `{"subject":"orders.x","payload":"","headers":{"Nats-Rollup":"all"}}`} {
		req := httptest.NewRequest("POST", "/api/publish", strings.NewReader(body))
		req.Host = "127.0.0.1:8080"
		req.Header.Set("X-Nats-Desk", "1")
		w := httptest.NewRecorder()
		localWriter.ServeHTTP(w, req)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "Admin access on a local host") {
			t.Fatal("local writer bypassed stream management through publish", body, w.Code, w.Body.String())
		}
	}
	if w := request("GET", "/api/hosts", "local", ""); !strings.Contains(w.Body.String(), `"role":"writer"`) {
		t.Fatal(w.Body.String())
	}
	restored, err := newHosts(a, file)
	if err != nil || restored.profiles[1].Role != "writer" {
		t.Fatal("role not persisted", err)
	}
}

func TestSharedConnectionSurvivesOtherProfiles(t *testing.T) {
	if os.Getenv("NATS_DESK_INTEGRATION") != "1" {
		t.Skip("integration disabled")
	}
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()
	file := filepath.Join(t.TempDir(), "hosts.json")
	twin := `[{"id":"local","name":"Local NATS","nats":"nats://127.0.0.1:4222","monitor":"http://127.0.0.1:8222"},{"id":"twin","name":"Twin","nats":"nats://127.0.0.1:4222","monitor":"http://127.0.0.1:8222","role":"reader"},{"id":"triplet","name":"Triplet","nats":"nats://127.0.0.1:4222","monitor":"http://127.0.0.1:8222","role":"writer"}]`
	if err := os.WriteFile(file, []byte(twin), 0o600); err != nil {
		t.Fatal(err)
	}
	a := &app{nc: nc, js: js, cfg: settings{NATS: "nats://127.0.0.1:4222", Monitor: "http://127.0.0.1:8222", Listen: "127.0.0.1:8080"}}
	hm, err := newHosts(a, file)
	if err != nil {
		t.Fatal(err)
	}
	defer hm.close()
	root := (&app{hosts: hm}).handler()
	request := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Host = "127.0.0.1:8080"
		req.Header.Set("X-Nats-Desk", "1")
		w := httptest.NewRecorder()
		root.ServeHTTP(w, req)
		return w
	}
	if w := request("POST", "/api/hosts/connect", `{"id":"twin","nats":"nats://127.0.0.1:4222"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if !hm.apps["local"].nc.IsConnected() || !hm.apps["triplet"].nc.IsConnected() || !hm.apps["twin"].nc.IsConnected() {
		t.Fatal("reconnecting one profile closed the connection another profile uses")
	}
	if w := request("DELETE", "/api/hosts/triplet", `{"confirm":"Triplet"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if !hm.apps["local"].nc.IsConnected() {
		t.Fatal("deleting a profile closed a shared connection")
	}
	own := hm.apps["twin"].nc
	if w := request("DELETE", "/api/hosts/twin", `{"confirm":"Twin"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if !own.IsClosed() || !hm.apps["local"].nc.IsConnected() {
		t.Fatal("a connection no profile uses must be closed")
	}
}
