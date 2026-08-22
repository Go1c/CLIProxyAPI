package auth

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type countingStore struct {
	saveCount atomic.Int32
}

type failingUpdateStore struct {
	fail atomic.Bool
}

func (s *failingUpdateStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *failingUpdateStore) Save(context.Context, *Auth) (string, error) {
	if s.fail.Load() {
		return "", errors.New("save failed")
	}
	return "", nil
}

func (s *failingUpdateStore) Delete(context.Context, string) error { return nil }

func (s *countingStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *countingStore) Save(context.Context, *Auth) (string, error) {
	s.saveCount.Add(1)
	return "", nil
}

func (s *countingStore) Delete(context.Context, string) error { return nil }

func TestWithSkipPersist_DisablesUpdatePersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "antigravity",
		Metadata: map[string]any{"type": "antigravity"},
	}

	if _, err := mgr.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("Register(skipPersist) returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 0 {
		t.Fatalf("expected 0 Save calls, got %d", got)
	}

	if _, err := mgr.Update(context.Background(), auth); err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 1 {
		t.Fatalf("expected 1 Save call, got %d", got)
	}

	ctxSkip := WithSkipPersist(context.Background())
	if _, err := mgr.Update(ctxSkip, auth); err != nil {
		t.Fatalf("Update(skipPersist) returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 1 {
		t.Fatalf("expected Save call count to remain 1, got %d", got)
	}
}

func TestWithSkipPersist_DisablesRegisterPersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "antigravity",
		Metadata: map[string]any{"type": "antigravity"},
	}

	if _, err := mgr.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("Register(skipPersist) returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 0 {
		t.Fatalf("expected 0 Save calls, got %d", got)
	}
}

func TestPersist_SkipsConfigAPIKeyAuth(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "codex:apikey:abc",
		Provider: "codex",
		Attributes: map[string]string{
			"api_key": "secret",
			"source":  "config:codex[abc]",
		},
		Metadata: map[string]any{"disable_cooling": true},
	}
	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 0 {
		t.Fatalf("expected 0 Save calls for config api key, got %d", got)
	}
	mgr.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: "codex", Model: "gpt-5", Success: true})
	if got := store.saveCount.Load(); got != 0 {
		t.Fatalf("expected MarkResult to skip persist for config api key, got %d Save calls", got)
	}
}

func TestUpdateAppliesRuntimeStateWhenPersistFails(t *testing.T) {
	store := &failingUpdateStore{}
	manager := NewManager(store, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "codex", ProxyURL: "socks5://proxy-a.example.com:443", Metadata: map[string]any{"type": "codex", "proxy_url": "socks5://proxy-a.example.com:443"}}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("Register returned error: %v", errRegister)
	}
	store.fail.Store(true)
	updated := auth.Clone()
	updated.ProxyURL = "socks5://proxy-b.example.com:443"
	updated.Metadata["proxy_url"] = updated.ProxyURL
	if _, errUpdate := manager.Update(context.Background(), updated); errUpdate != nil {
		t.Fatalf("Update returned error: %v", errUpdate)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current.ProxyURL != "socks5://proxy-b.example.com:443" {
		t.Fatalf("runtime auth = %#v, want updated proxy after ignored persist failure", current)
	}
}
