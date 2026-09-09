package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.yaml.in/yaml/v3"
)

const maxImportBytes = 256 << 10

// Serialize imports in this local process. Existing streams are never updated.
var importLock sync.Mutex

type yamlStream struct {
	Name        string   `yaml:"name"`
	Description string   `yaml:"description"`
	Subjects    []string `yaml:"subjects"`
	Storage     string   `yaml:"storage"`
	Retention   string   `yaml:"retention"`
	MaxAge      string   `yaml:"max_age"`
	MaxMsgs     *int64   `yaml:"max_messages"`
	MaxBytes    *int64   `yaml:"max_bytes"`
}
type importRow struct {
	Name      string   `json:"name"`
	Subjects  []string `json:"subjects"`
	Storage   string   `json:"storage"`
	Retention string   `json:"retention"`
	MaxAge    string   `json:"max_age"`
	MaxMsgs   int64    `json:"max_messages"`
	MaxBytes  int64    `json:"max_bytes"`
	Status    string   `json:"status"`
	Detail    string   `json:"detail"`
	config    nats.StreamConfig
}
type importRequest struct {
	YAML  string   `json:"yaml"`
	Names []string `json:"names"`
}

func parseImport(raw string) ([]importRow, error) {
	if len(raw) > maxImportBytes {
		return nil, errors.New("YAML must be at most 256 KiB")
	}
	var doc struct {
		Streams []yamlStream `yaml:"streams"`
	}
	d := yaml.NewDecoder(strings.NewReader(raw))
	d.KnownFields(true)
	if err := d.Decode(&doc); err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, errors.New("use one YAML document with a streams list")
	}
	if len(doc.Streams) == 0 || len(doc.Streams) > 100 {
		return nil, errors.New("include between 1 and 100 entries in streams")
	}
	counts := map[string]int{}
	for _, s := range doc.Streams {
		counts[s.Name]++
	}
	rows := make([]importRow, 0, len(doc.Streams))
	for _, s := range doc.Streams {
		r := importRow{Name: s.Name, Subjects: s.Subjects, Storage: s.Storage, Retention: s.Retention, MaxAge: s.MaxAge, MaxMsgs: -1, MaxBytes: -1, Status: "ready", Detail: "Ready to create"}
		if r.Storage == "" {
			r.Storage = "file"
		}
		if r.Retention == "" {
			r.Retention = "limits"
		}
		if r.MaxAge == "" {
			r.MaxAge = "0s"
		}
		if s.MaxMsgs != nil {
			r.MaxMsgs = *s.MaxMsgs
		}
		if s.MaxBytes != nil {
			r.MaxBytes = *s.MaxBytes
		}
		c := nats.StreamConfig{Name: r.Name, Description: s.Description, Subjects: r.Subjects, Replicas: 1, MaxMsgs: r.MaxMsgs, MaxBytes: r.MaxBytes}
		issues := []string{}
		if r.Name == "" || strings.ContainsAny(r.Name, ".*>/\\ \t\r\n") {
			issues = append(issues, "invalid stream name")
		}
		if counts[r.Name] > 1 {
			issues = append(issues, "duplicate name in file")
		}
		if len(r.Subjects) == 0 {
			issues = append(issues, "at least one subject is required")
		}
		for _, subject := range r.Subjects {
			if !validSubject(subject, true) {
				issues = append(issues, "invalid subject: "+subject)
			}
		}
		age, err := time.ParseDuration(r.MaxAge)
		if err != nil || age < 0 {
			issues = append(issues, "max_age must be a nonnegative duration such as 24h or 0s")
		}
		c.MaxAge = age
		if r.MaxMsgs < -1 || r.MaxBytes < -1 {
			issues = append(issues, "limits must be -1 (unlimited) or nonnegative")
		}
		switch r.Storage {
		case "file":
			c.Storage = nats.FileStorage
		case "memory":
			c.Storage = nats.MemoryStorage
		default:
			issues = append(issues, "storage must be file or memory")
		}
		switch r.Retention {
		case "limits":
			c.Retention = nats.LimitsPolicy
		case "interest":
			c.Retention = nats.InterestPolicy
		case "workqueue":
			c.Retention = nats.WorkQueuePolicy
		default:
			issues = append(issues, "retention must be limits, interest or workqueue")
		}
		if len(issues) > 0 {
			r.Status = "invalid"
			r.Detail = strings.Join(issues, "; ")
		}
		r.config = c
		rows = append(rows, r)
	}
	return rows, nil
}
func (a *app) checkImportRow(ctx context.Context, r *importRow) {
	if r.Status != "ready" {
		return
	}
	_, err := a.js.StreamInfo(r.Name, nats.Context(ctx))
	switch {
	case err == nil:
		r.Status = "exists"
		r.Detail = "Already exists; will not be changed"
	case errors.Is(err, nats.ErrStreamNotFound):
	default:
		r.Status = "error"
		r.Detail = err.Error()
	}
}
func (a *app) previewImport(w http.ResponseWriter, r *http.Request) {
	var in importRequest
	if !decode(w, r, &in) {
		return
	}
	rows, err := parseImport(in.YAML)
	if err != nil {
		fail(w, err)
		return
	}
	for i := range rows {
		a.checkImportRow(r.Context(), &rows[i])
	}
	respond(w, map[string]any{"rows": rows})
}
func (a *app) createImport(w http.ResponseWriter, r *http.Request) {
	var in importRequest
	if !decode(w, r, &in) {
		return
	}
	rows, err := parseImport(in.YAML)
	if err != nil {
		fail(w, err)
		return
	}
	if len(in.Names) == 0 || len(in.Names) > 100 {
		fail(w, errors.New("preview the file and select ready streams first"))
		return
	}
	if !importLock.TryLock() {
		fail(w, errors.New("another import is running; preview again when it finishes"))
		return
	}
	defer importLock.Unlock()
	selected := map[string]bool{}
	for _, n := range in.Names {
		selected[n] = true
	}
	created := 0
	for i := range rows {
		row := &rows[i]
		if !selected[row.Name] {
			if row.Status == "ready" {
				row.Status = "skipped"
				row.Detail = "Not selected for creation"
			}
			continue
		}
		a.checkImportRow(r.Context(), row)
		if row.Status != "ready" {
			continue
		}
		// No UpdateStream path: an existing name is skipped, including on retries.
		_, err := a.js.AddStream(&row.config, nats.Context(r.Context()))
		if err != nil {
			row.Status = "error"
			row.Detail = err.Error() + ". Preview again to check server state before retrying."
		} else {
			row.Status = "created"
			row.Detail = "Stream created"
			created++
		}
	}
	respond(w, map[string]any{"rows": rows, "created": created})
}
