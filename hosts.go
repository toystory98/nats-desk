package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type hostProfile struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	NATS    string `json:"nats"`
	Monitor string `json:"monitor"`
}
type hostManager struct {
	mu       sync.RWMutex
	profiles []hostProfile
	apps     map[string]*app
	file     string
	listen   string
}

func localEndpoint(raw string) bool {
	u, e := url.Parse(raw)
	if e != nil {
		return false
	}
	host := u.Hostname()
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func canManage(cfg settings) bool { return localEndpoint(cfg.NATS) && localEndpoint(cfg.Monitor) }
func validateProfile(p hostProfile) error {
	if strings.TrimSpace(p.Name) == "" {
		return errors.New("host name is required")
	}
	for _, item := range []struct {
		value    string
		schemes  string
		optional bool
	}{{p.NATS, "nats tls ws wss", true}, {p.Monitor, "http https", false}} {
		if item.value == "" && item.optional {
			continue
		}
		u, e := url.Parse(item.value)
		if e != nil || u.Hostname() == "" || !strings.Contains(" "+item.schemes+" ", " "+u.Scheme+" ") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("use a valid endpoint URL without embedded credentials, query, or fragment")
		}
	}
	return nil
}
func (h *hostManager) save() error {
	b, e := json.MarshalIndent(h.profiles, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(h.file), ".hosts-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(f.Name(), h.file)
}
func newHosts(a *app, file string) (*hostManager, error) {
	h := &hostManager{apps: map[string]*app{}, file: file, listen: a.cfg.Listen}
	h.profiles = []hostProfile{{"local", "Local NATS", a.cfg.NATS, a.cfg.Monitor}}
	if b, e := os.ReadFile(file); e == nil {
		if e = json.Unmarshal(b, &h.profiles); e != nil {
			return nil, e
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	seen := map[string]bool{}
	for _, p := range h.profiles {
		if p.ID == "" || seen[p.ID] {
			return nil, errors.New("duplicate or missing host profile ID")
		}
		seen[p.ID] = true
		if e := validateProfile(p); e != nil {
			return nil, e
		}
		cfg := settings{p.NATS, p.Monitor, a.cfg.Listen}
		if cfg.NATS == a.cfg.NATS && cfg.Monitor == a.cfg.Monitor {
			h.apps[p.ID] = a
		} else {
			h.apps[p.ID] = &app{cfg: cfg, client: &http.Client{Timeout: 3 * time.Second}}
		}
	}
	if !seen["local"] {
		return nil, errors.New("hosts file must contain a local profile")
	}
	return h, nil
}
func (h *hostManager) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, a := range h.apps {
		if a.nc != nil {
			a.nc.Close()
		}
	}
}
func (h *hostManager) serve(w http.ResponseWriter, r *http.Request) {
	// Reuse the normal loopback/origin boundary before entering this dispatcher.
	if r.URL.Path == "/api/hosts" && r.Method == "GET" {
		h.mu.RLock()
		defer h.mu.RUnlock()
		out := []map[string]any{}
		for _, p := range h.profiles {
			a := h.apps[p.ID]
			out = append(out, map[string]any{"id": p.ID, "name": p.Name, "nats": p.NATS, "monitor": p.Monitor, "can_manage": canManage(a.cfg), "connected": a.nc != nil && a.nc.IsConnected()})
		}
		respond(w, out)
		return
	}
	if r.URL.Path == "/api/hosts" && r.Method == "POST" {
		var p hostProfile
		if !decode(w, r, &p) {
			return
		}
		if e := validateProfile(p); e != nil {
			fail(w, e)
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.profiles) >= 30 {
			fail(w, errors.New("maximum 30 profiles"))
			return
		}
		p.ID = rand.Text()
		h.profiles = append(h.profiles, p)
		if e := h.save(); e != nil {
			h.profiles = h.profiles[:len(h.profiles)-1]
			fail(w, e)
			return
		}
		h.apps[p.ID] = &app{cfg: settings{p.NATS, p.Monitor, h.listen}, client: &http.Client{Timeout: 3 * time.Second}}
		respond(w, p)
		return
	}
	if r.URL.Path == "/api/hosts/connect" && r.Method == "POST" {
		h.connect(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/hosts/") {
		id := strings.TrimPrefix(r.URL.Path, "/api/hosts/")
		if id == "" || strings.Contains(id, "/") {
			fail(w, errors.New("invalid host profile ID"))
			return
		}
		switch r.Method {
		case "PUT":
			h.updateProfile(w, r, id)
			return
		case "DELETE":
			h.deleteProfile(w, r, id)
			return
		}
	}
	id := r.Header.Get("X-Nats-Host")
	if r.URL.Path == "/api/live" {
		id = r.URL.Query().Get("host")
	}
	if id == "" {
		id = "local"
	}
	h.mu.RLock()
	a, ok := h.apps[id]
	h.mu.RUnlock()
	if !ok {
		http.Error(w, "Unknown host profile", 404)
		return
	}
	a.handler().ServeHTTP(w, r)
}

func (h *hostManager) updateProfile(w http.ResponseWriter, r *http.Request, id string) {
	var in hostProfile
	if !decode(w, r, &in) {
		return
	}
	in.ID = id
	if e := validateProfile(in); e != nil {
		fail(w, e)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	index := -1
	for i, p := range h.profiles {
		if p.ID == id {
			index = i
			break
		}
	}
	if index < 0 {
		fail(w, errors.New("unknown profile"))
		return
	}
	previous := h.profiles[index]
	h.profiles[index] = in
	if e := h.save(); e != nil {
		h.profiles[index] = previous
		fail(w, e)
		return
	}
	old := h.apps[id]
	if old == nil || previous.NATS != in.NATS || previous.Monitor != in.Monitor {
		h.apps[id] = &app{cfg: settings{in.NATS, in.Monitor, h.listen}, client: &http.Client{Timeout: 3 * time.Second}}
		if old != nil && old.nc != nil {
			old.nc.Close()
		}
	}
	respond(w, in)
}

func (h *hostManager) deleteProfile(w http.ResponseWriter, r *http.Request, id string) {
	if id == "local" {
		fail(w, errors.New("the built-in local profile cannot be deleted"))
		return
	}
	var in struct {
		Confirm string `json:"confirm"`
	}
	if !decode(w, r, &in) {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	index := -1
	for i, p := range h.profiles {
		if p.ID == id {
			index = i
			break
		}
	}
	if index < 0 {
		fail(w, errors.New("unknown profile"))
		return
	}
	if in.Confirm != h.profiles[index].Name {
		fail(w, errors.New("confirmation does not match"))
		return
	}
	previous := append([]hostProfile(nil), h.profiles...)
	h.profiles = append(h.profiles[:index], h.profiles[index+1:]...)
	if e := h.save(); e != nil {
		h.profiles = previous
		fail(w, e)
		return
	}
	old := h.apps[id]
	delete(h.apps, id)
	if old != nil && old.nc != nil {
		old.nc.Close()
	}
	respond(w, map[string]bool{"deleted": true})
}

func (h *hostManager) connect(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID       string `json:"id"`
		NATS     string `json:"nats"`
		Username string `json:"username"`
		Password string `json:"password"`
		Token    string `json:"token"`
	}
	if !decode(w, r, &in) {
		return
	}
	h.mu.RLock()
	var p hostProfile
	for _, v := range h.profiles {
		if v.ID == in.ID {
			p = v
		}
	}
	old := h.apps[in.ID]
	h.mu.RUnlock()
	if old == nil {
		fail(w, errors.New("unknown profile"))
		return
	}
	if in.NATS != "" {
		p.NATS = in.NATS
	}
	if e := validateProfile(p); e != nil {
		fail(w, e)
		return
	}
	if p.NATS == "" {
		respond(w, map[string]any{"id": p.ID, "monitor_only": true})
		return
	}
	opts := []nats.Option{nats.Name("NATS Desk"), nats.Timeout(4 * time.Second), nats.MaxReconnects(-1)}
	if in.Token != "" {
		opts = append(opts, nats.Token(in.Token))
	} else if in.Username != "" {
		opts = append(opts, nats.UserInfo(in.Username, in.Password))
	}
	nc, e := nats.Connect(p.NATS, opts...)
	if e != nil {
		fail(w, errors.New("NATS connection failed. Check the client URL, network access and credentials."))
		return
	}
	js, e := nc.JetStream(nats.MaxWait(4 * time.Second))
	if e != nil {
		nc.Close()
		fail(w, e)
		return
	}
	a := &app{nc: nc, js: js, cfg: settings{p.NATS, p.Monitor, h.listen}, client: &http.Client{Timeout: 3 * time.Second}}
	h.mu.Lock()
	defer h.mu.Unlock()
	index := -1
	for i, v := range h.profiles {
		if v.ID == p.ID {
			index = i
			break
		}
	}
	previous := h.profiles[index]
	h.profiles[index] = p
	if e = h.save(); e != nil {
		h.profiles[index] = previous
		nc.Close()
		fail(w, e)
		return
	}
	prior := h.apps[p.ID]
	h.apps[p.ID] = a
	// Existing in-flight requests keep their original app; closing its connection
	// prevents a stale live capture from silently crossing to a new endpoint.
	if prior.nc != nil {
		prior.nc.Close()
	}
	respond(w, map[string]any{"id": p.ID, "can_manage": canManage(a.cfg)})
}
func (a *app) monitorStreams(w http.ResponseWriter, r *http.Request) {
	v, e := a.monitor(r.Context(), "/jsz?accounts=true&streams=true&config=true")
	if e != nil {
		fail(w, e)
		return
	}
	b, _ := json.Marshal(v)
	var doc struct {
		Accounts []struct {
			Streams []struct {
				Name   string         `json:"name"`
				Config map[string]any `json:"config"`
				State  map[string]any `json:"state"`
			} `json:"stream_detail"`
		} `json:"account_details"`
	}
	if e = json.Unmarshal(b, &doc); e != nil {
		fail(w, e)
		return
	}
	out := []any{}
	for _, account := range doc.Accounts {
		for _, s := range account.Streams {
			if s.Config == nil {
				s.Config = map[string]any{}
			}
			if _, ok := s.Config["name"]; !ok {
				s.Config["name"] = s.Name
			}
			out = append(out, map[string]any{"config": s.Config, "state": s.State})
		}
	}
	respond(w, out)
}

func (a *app) monitorConsumers(w http.ResponseWriter, r *http.Request) {
	v, e := a.monitor(r.Context(), "/jsz?accounts=true&streams=true&consumers=true&config=true")
	if e != nil {
		fail(w, e)
		return
	}
	b, _ := json.Marshal(v)
	var doc struct {
		Accounts []struct {
			Name    string `json:"name"`
			Streams []struct {
				Name      string               `json:"name"`
				Config    nats.StreamConfig    `json:"config"`
				Consumers []*nats.ConsumerInfo `json:"consumer_detail"`
			} `json:"stream_detail"`
		} `json:"account_details"`
	}
	if e = json.Unmarshal(b, &doc); e != nil {
		fail(w, e)
		return
	}
	rows := []consumerSummary{}
	for _, account := range doc.Accounts {
		for _, stream := range account.Streams {
			for _, consumer := range stream.Consumers {
				if consumer != nil {
					rows = append(rows, summarizeConsumer(account.Name, stream.Name, stream.Config.Subjects, consumer))
				}
			}
		}
	}
	sortConsumers(rows)
	respond(w, rows)
}
func remotePublishAllowed(subject string, headers map[string]string) error {
	if strings.HasPrefix(subject, "$") || strings.HasPrefix(subject, "_") {
		return fmt.Errorf("remote publishing to reserved administration or reply subjects is blocked")
	}
	for k := range headers {
		if strings.EqualFold(strings.TrimSpace(k), "Nats-Rollup") {
			return errors.New("rollup headers are blocked for remote hosts")
		}
	}
	return nil
}
