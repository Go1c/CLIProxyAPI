package auth

import (
	"context"
	"testing"
	"time"
)

type blockingListStore struct {
	items   []*Auth
	started chan struct{}
	release chan struct{}
}

func (s *blockingListStore) List(context.Context) ([]*Auth, error) {
	close(s.started)
	<-s.release
	return s.items, nil
}

func (*blockingListStore) Save(context.Context, *Auth) (string, error) { return "", nil }
func (*blockingListStore) Delete(context.Context, string) error        { return nil }

func TestManagerLoadPublishesAuthRegistryAtomically(t *testing.T) {
	store := &blockingListStore{
		items:   []*Auth{{ID: "new-auth", Provider: "codex"}},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	manager.mu.Lock()
	manager.auths["old-auth"] = &Auth{ID: "old-auth", Provider: "codex"}
	manager.mu.Unlock()

	loadDone := make(chan error, 1)
	go func() { loadDone <- manager.Load(context.Background()) }()
	<-store.started

	lookupDone := make(chan *Auth, 1)
	go func() {
		auth, _ := manager.GetByID("new-auth")
		lookupDone <- auth
	}()

	select {
	case auth := <-lookupDone:
		t.Fatalf("registry lookup completed while reload snapshot was incomplete: %#v", auth)
	case <-time.After(20 * time.Millisecond):
	}

	close(store.release)
	if errLoad := <-loadDone; errLoad != nil {
		t.Fatalf("Load returned error: %v", errLoad)
	}
	select {
	case auth := <-lookupDone:
		if auth == nil || auth.ID != "new-auth" || auth.Provider != "codex" {
			t.Fatalf("registry published unexpected auth after reload: %#v", auth)
		}
	case <-time.After(time.Second):
		t.Fatal("registry lookup remained blocked after reload completed")
	}
	if _, ok := manager.GetByID("old-auth"); ok {
		t.Fatal("old auth remained visible after atomic reload replacement")
	}
}
