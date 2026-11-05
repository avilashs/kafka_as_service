// Package cellapi is the endpoint cell agents pull from (design §7.2). Cells
// initiate every connection; the control plane never connects into a cell.
package cellapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/avilashs/kafka_as_service/internal/metering"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// Server serves /cell/v1.
type Server struct {
	Store *store.Store
	Log   *slog.Logger
	// MaxWait caps long-poll duration; PollEvery is the server-side check interval.
	MaxWait   time.Duration
	PollEvery time.Duration
}

// DesiredResponse is the body of GET /cell/v1/desired.
type DesiredResponse struct {
	CellID          string               `json:"cellId"`
	CurrentRevision int64                `json:"currentRevision"`
	Entries         []store.DesiredEntry `json:"entries"`
}

// StatusRequest is the body of POST /cell/v1/status.
type StatusRequest struct {
	Reports []store.StatusReport `json:"reports"`
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /cell/v1/desired", s.authed(s.desired))
	mux.HandleFunc("POST /cell/v1/status", s.authed(s.status))
	mux.HandleFunc("POST /cell/v1/heartbeat", s.authed(s.heartbeat))
	mux.HandleFunc("POST /cell/v1/metering", s.authed(s.metering))
	return mux
}

// authed authenticates a per-cell bearer token. (mTLS identities replace this in M3.)
func (s *Server) authed(h func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || tok == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		cell, err := s.Store.CellByToken(r.Context(), tok)
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		h(w, r, cell)
	}
}

func (s *Server) desired(w http.ResponseWriter, r *http.Request, cell string) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	wait, _ := time.ParseDuration(r.URL.Query().Get("wait"))
	maxWait := s.MaxWait
	if maxWait == 0 {
		maxWait = 30 * time.Second
	}
	if wait > maxWait {
		wait = maxWait
	}
	every := s.PollEvery
	if every == 0 {
		every = 250 * time.Millisecond
	}
	deadline := time.Now().Add(wait)
	for {
		entries, cur, err := s.Store.DesiredSince(r.Context(), cell, since)
		if err != nil {
			s.Log.Error("desired query", "cell", cell, "err", err)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if len(entries) > 0 || time.Now().After(deadline) || cur < since {
			if entries == nil {
				entries = []store.DesiredEntry{}
			}
			writeJSON(w, DesiredResponse{CellID: cell, CurrentRevision: cur, Entries: entries})
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(every):
		}
	}
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, cell string) {
	var req StatusRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	n, err := s.Store.ReportStatus(r.Context(), cell, req.Reports)
	if err != nil {
		s.Log.Error("status report", "cell", cell, "err", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]int{"accepted": n})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request, cell string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := s.Store.Heartbeat(r.Context(), cell, body); err != nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// MeteringRequest is the body of POST /cell/v1/metering.
type MeteringRequest struct {
	Records []metering.Record `json:"records"`
}

// MeteringResponse lists what the control plane now holds (the agent may forget those) and what it refused.
type MeteringResponse struct {
	Acked    []string `json:"acked"`
	Rejected []string `json:"rejected,omitempty"`
}

func (s *Server) metering(w http.ResponseWriter, r *http.Request, cell string) {
	var req MeteringRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	acked, rejected, err := s.Store.IngestMetering(r.Context(), cell, req.Records)
	if err != nil {
		s.Log.Error("metering ingest", "cell", cell, "err", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(MeteringResponse{Acked: acked, Rejected: rejected})
}
