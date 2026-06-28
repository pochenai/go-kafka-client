package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// appendToken stores one tokenEvent (id + isDeleted) through the store, like the
// consumer would, so the seq advances. It fails the test on error.
func appendToken(t *testing.T, s *boltStore, partition int, offset int64, id string, deleted bool) {
	t.Helper()
	if _, _, err := s.Append(storedEvent{Partition: partition, Offset: offset, TokenID: id, IsDeleted: deleted}); err != nil {
		t.Fatalf("Append(%s): %v", id, err)
	}
}

func TestTokenDeltasLastWins(t *testing.T) {
	s := newTestStore(t)
	// A: added then deleted -> deleted. B: deleted then re-added -> new.
	// C: added once -> new. D: added before the cursor -> excluded.
	appendToken(t, s, 0, 1, "D", false) // seq 1, before cursor
	appendToken(t, s, 0, 2, "A", false) // seq 2
	appendToken(t, s, 0, 3, "B", true)  // seq 3
	appendToken(t, s, 0, 4, "C", false) // seq 4
	appendToken(t, s, 0, 5, "A", true)  // seq 5  (A now deleted)
	appendToken(t, s, 0, 6, "B", false) // seq 6  (B now re-added)

	newIDs, deletedIDs, latest, err := s.TokenDeltas(1) // since seq 1 -> excludes D
	if err != nil {
		t.Fatalf("TokenDeltas: %v", err)
	}
	if want := []string{"B", "C"}; !reflect.DeepEqual(newIDs, want) {
		t.Errorf("new = %v, want %v", newIDs, want)
	}
	if want := []string{"A"}; !reflect.DeepEqual(deletedIDs, want) {
		t.Errorf("deleted = %v, want %v", deletedIDs, want)
	}
	if latest != 6 {
		t.Errorf("latestSeq = %d, want 6", latest)
	}
}

func TestTokenDeltasEmptyRange(t *testing.T) {
	s := newTestStore(t)
	appendToken(t, s, 0, 1, "A", false)

	newIDs, deletedIDs, latest, err := s.TokenDeltas(1) // nothing after seq 1
	if err != nil {
		t.Fatalf("TokenDeltas: %v", err)
	}
	if len(newIDs) != 0 || len(deletedIDs) != 0 {
		t.Errorf("expected empty deltas, got new=%v deleted=%v", newIDs, deletedIDs)
	}
	if latest != 1 {
		t.Errorf("latestSeq = %d, want 1 (unchanged cursor)", latest)
	}
}

// call builds a JSON-RPC 2.0 request from typed Go values (no hand-written JSON),
// posts it to the API, decodes result into out (when non-nil), and returns any
// rpc-level error. Pass nil params to omit them.
func call(t *testing.T, api *API, method string, params, out any) *rpcError {
	t.Helper()
	body, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  any    `json:"params,omitempty"`
		ID      int    `json:"id"`
	}{"2.0", method, params, 1})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return rawCall(t, api, string(body), out)
}

// rawCall posts a literal body — used to exercise malformed-input paths that a typed
// request could never produce.
func rawCall(t *testing.T, api *API, body string, out any) *rpcError {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	api.ServeHTTP(rec, req)

	var resp rpcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	if resp.Error != nil {
		return resp.Error
	}
	if out != nil {
		// resp.Result is an untyped tree; round-trip it into the caller's concrete type.
		b, _ := json.Marshal(resp.Result)
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("decode result into %T: %v", out, err)
		}
	}
	return nil
}

func TestAPILatestSeqAndTokens(t *testing.T) {
	s := newTestStore(t)
	appendToken(t, s, 0, 1, "A", false)
	appendToken(t, s, 0, 2, "B", false)
	appendToken(t, s, 0, 3, "A", true)
	api := NewAPI(s)

	// latest_seq
	var latest uint64
	if e := call(t, api, "latest_seq", nil, &latest); e != nil {
		t.Fatalf("latest_seq error: %+v", e)
	}
	if latest != 3 {
		t.Errorf("latest_seq = %d, want 3", latest)
	}

	// tokens(start=0): A added then deleted -> deleted; B -> new
	var out tokensResult
	if e := call(t, api, "tokens", tokensParams{Start: 0}, &out); e != nil {
		t.Fatalf("tokens error: %+v", e)
	}
	if want := (tokensResult{Start: 0, New: []string{"B"}, Deleted: []string{"A"}, LatestSeq: 3}); !reflect.DeepEqual(out, want) {
		t.Errorf("tokens = %+v, want %+v", out, want)
	}
}

func TestAPIErrors(t *testing.T) {
	api := NewAPI(newTestStore(t))

	if e := call(t, api, "nope", nil, nil); e == nil || e.Code != codeMethodNotFound {
		t.Errorf("unknown method: want method-not-found, got %+v", e)
	}
	if e := rawCall(t, api, `not json`, nil); e == nil || e.Code != codeParseError {
		t.Errorf("bad body: want parse error, got %+v", e)
	}
}
