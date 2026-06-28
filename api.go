package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// API serves a minimal JSON-RPC 2.0 endpoint over HTTP backed by the store's read
// side. It depends only on eventReader, so it never touches Kafka or the write path.
type API struct {
	store eventReader
}

func NewAPI(store eventReader) *API { return &API{store: store} }

// --- JSON-RPC 2.0 envelope ---

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

// Standard JSON-RPC 2.0 error codes.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
)

// tokensParams is the params object of the tokens method.
type tokensParams struct {
	Start uint64 `json:"start"`
}

// tokensResult is the payload of the tokens method: the net add/delete sets covering
// the half-open range (start, latestSeq] — i.e. start < seq <= latestSeq. start echoes
// the request cursor and latestSeq is the cursor to pass back next time.
type tokensResult struct {
	Start     uint64   `json:"start"`
	New       []string `json:"new"`
	Deleted   []string `json:"deleted"`
	LatestSeq uint64   `json:"latestSeq"`
}

// ServeHTTP handles a single JSON-RPC 2.0 request over POST. Batch requests are not
// supported.
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "JSON-RPC requires POST", http.StatusMethodNotAllowed)
		return
	}

	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeRPC(w, rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: codeParseError, Message: "parse error", Data: err.Error()}})
		return
	}
	if req.JSONRPC != "2.0" {
		writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: codeInvalidRequest, Message: `"jsonrpc" must be "2.0"`}})
		return
	}

	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "latest_seq":
		seq, err := a.store.MaxSeq()
		if err != nil {
			resp.Error = &rpcError{Code: codeInternalError, Message: "internal error", Data: err.Error()}
		} else {
			resp.Result = seq
		}

	case "tokens":
		var p tokensParams
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				resp.Error = &rpcError{Code: codeInvalidParams, Message: "invalid params", Data: err.Error()}
				writeRPC(w, resp)
				return
			}
		}
		newIDs, deletedIDs, latest, err := a.store.TokenDeltas(p.Start)
		if err != nil {
			resp.Error = &rpcError{Code: codeInternalError, Message: "internal error", Data: err.Error()}
		} else {
			resp.Result = tokensResult{Start: p.Start, New: newIDs, Deleted: deletedIDs, LatestSeq: latest}
		}

	default:
		resp.Error = &rpcError{Code: codeMethodNotFound, Message: "method not found: " + req.Method}
	}
	writeRPC(w, resp)
}

func writeRPC(w http.ResponseWriter, resp rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("api: failed to write response: %v", err)
	}
}

// Serve runs the JSON-RPC HTTP server until ctx is cancelled, then shuts it down
// gracefully. It blocks, so run it in its own goroutine.
func (a *API) Serve(ctx context.Context, addr string) {
	srv := &http.Server{Addr: addr, Handler: a}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("api: shutdown error: %v", err)
		}
	}()

	log.Printf("JSON-RPC API listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("api: server error: %v", err)
	}
}
