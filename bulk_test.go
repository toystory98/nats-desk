package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestImportYAML(t *testing.T) {
	rows, err := parseImport("streams:\n  - name: ORDERS\n    subjects: [orders.>]\n")
	if err != nil || len(rows) != 1 || rows[0].Status != "ready" || rows[0].Storage != "file" || rows[0].MaxMsgs != -1 || rows[0].MaxAge != "0s" {
		t.Fatalf("defaults: %+v %v", rows, err)
	}
	for _, raw := range []string{"", "streams: []", "streams:\n - name: ORDERS\n   subject: [orders.>]", "streams: []\n---\nstreams: []", "streams: [", strings.Repeat("x", maxImportBytes+1), "streams:\n - name: X\n   name: Y\n   subjects: [a]"} {
		if _, err = parseImport(raw); err == nil {
			t.Fatalf("accepted invalid document: %.100s", raw)
		}
	}
	raw := "streams:\n - name: DUP\n   subjects: [a]\n - name: DUP\n   subjects: [b]\n - name: BAD\n   subjects: [a..b]\n   max_age: -1h\n   max_messages: -2\n   storage: cloud\n"
	rows, err = parseImport(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Status != "invalid" {
			t.Fatalf("accepted invalid entry: %+v", r)
		}
	}
	many := "streams:\n"
	for i := 0; i < 101; i++ {
		many += fmt.Sprintf(" - name: S%d\n   subjects: [a%d]\n", i, i)
	}
	if _, err = parseImport(many); err == nil {
		t.Fatal("accepted more than 100 streams")
	}
}

func TestBulkImportLifecycle(t *testing.T) {
	if os.Getenv("NATS_DESK_INTEGRATION") != "1" {
		t.Skip("integration disabled")
	}
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	a := &app{nc: nc, js: js, cfg: settings{NATS: "nats://127.0.0.1:4222", Monitor: "http://127.0.0.1:8222"}}
	h := a.handler()
	call := func(endpoint string, in importRequest) struct {
		Rows    []importRow `json:"rows"`
		Created int         `json:"created"`
	} {
		t.Helper()
		b, _ := json.Marshal(in)
		req := httptest.NewRequest("POST", endpoint, bytes.NewReader(b))
		req.Host = "127.0.0.1:8080"
		req.Header.Set("X-Nats-Desk", "1")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", endpoint, w.Code, w.Body.String())
		}
		var out struct {
			Rows    []importRow `json:"rows"`
			Created int         `json:"created"`
		}
		if err = json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	prefix := fmt.Sprintf("NATSDESK_BULK_TEST_%d", time.Now().UnixNano())
	names := []string{prefix + "_A", prefix + "_B", prefix + "_C"}
	for _, n := range names {
		defer js.DeleteStream(n)
	}
	raw := fmt.Sprintf("streams:\n - name: %s\n   subjects: [%s.events]\n   storage: memory\n - name: %s\n   subjects: [%s.other]\n   storage: memory\n - name: %s\n   subjects: [%s.events]\n   storage: memory\n", names[0], prefix, names[1], prefix, names[2], prefix)
	preview := call("/api/streams/import/preview", importRequest{YAML: raw})
	if len(preview.Rows) != 3 {
		t.Fatal(preview)
	}
	for _, r := range preview.Rows {
		if r.Status != "ready" {
			t.Fatal(r)
		}
	}
	if _, err = js.StreamInfo(names[0]); err != nats.ErrStreamNotFound {
		t.Fatal("preview created a stream", err)
	}
	result := call("/api/streams/import", importRequest{YAML: raw, Names: names})
	if result.Created != 2 || result.Rows[2].Status != "error" {
		t.Fatalf("expected two creations and one subject-overlap error: %+v", result)
	}
	// Retry must skip existing streams, even when the YAML changes their description.
	retry := call("/api/streams/import", importRequest{YAML: strings.Replace(raw, "   subjects:", "   description: must-not-update\n   subjects:", -1), Names: names})
	if retry.Created != 0 || retry.Rows[0].Status != "exists" || retry.Rows[1].Status != "exists" {
		t.Fatal(retry)
	}
	info, err := js.StreamInfo(names[0])
	if err != nil || info.Config.Description != "" {
		t.Fatal("retry modified existing stream", err)
	}
	// A preview followed by a server-side creation is rechecked on submit.
	info, err = js.AddStream(&nats.StreamConfig{Name: names[2], Subjects: []string{prefix + ".third"}, Storage: nats.MemoryStorage})
	if err != nil {
		t.Fatal(err)
	}
	result = call("/api/streams/import", importRequest{YAML: raw, Names: []string{names[2]}})
	if result.Created != 0 || result.Rows[2].Status != "exists" {
		t.Fatal("existing name after preview not skipped", result)
	}
}
