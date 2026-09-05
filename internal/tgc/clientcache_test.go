package tgc

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestOldClientCannotDropReplacement(t *testing.T) {
	oldCtx, oldCancel := context.WithCancel(context.Background())
	newCtx, newCancel := context.WithCancel(context.Background())
	defer oldCancel()
	defer newCancel()
	old := &cachedClient{cancel: oldCancel}
	replacement := &cachedClient{cancel: newCancel}
	c := &clientCache{m: map[string]*cachedClient{"bot": old}}
	c.drop("bot", old)
	if oldCtx.Err() == nil {
		t.Fatal("old client was not canceled")
	}
	c.m["bot"] = replacement
	// Delayed Run exit or a second waiter observing the old failure.
	c.drop("bot", old)
	if c.m["bot"] != replacement || newCtx.Err() != nil {
		t.Fatal("old cleanup canceled the replacement connection")
	}
}

func TestClientFailureWakesAllWaiters(t *testing.T) {
	c := &cachedClient{ready: make(chan struct{})}
	want := errors.New("dial failed before Run callback")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-c.ready
			if !errors.Is(c.err, want) {
				t.Errorf("got %v, want %v", c.err, want)
			}
		}()
	}
	c.signalReady(want)
	c.signalReady(errors.New("later termination"))
	wg.Wait()
}

func TestReadyClientTerminationDoesNotRaceWithWaiters(t *testing.T) {
	c := &cachedClient{ready: make(chan struct{})}
	c.signalReady(nil)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-c.ready
			if c.err != nil {
				t.Error(c.err)
			}
		}()
	}
	c.signalReady(context.Canceled)
	wg.Wait()
}
