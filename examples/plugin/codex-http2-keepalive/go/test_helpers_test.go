package main

import (
	"encoding/json"
	"testing"
)

func withTestRuntime(t *testing.T) {
	t.Helper()
	oldRuntime := runtime
	runtime = newPluginRuntime()
	t.Cleanup(func() {
		runtime = oldRuntime
	})
}

func withHostCallStub(t *testing.T, fn func(string, any) (json.RawMessage, error)) {
	t.Helper()
	oldHostCall := hostCall
	hostCall = fn
	t.Cleanup(func() {
		hostCall = oldHostCall
	})
}

func decodeEnvelope[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if !env.OK {
		if env.Error != nil {
			t.Fatalf("envelope error: %s: %s", env.Error.Code, env.Error.Message)
		}
		t.Fatal("envelope error: unknown failure")
	}
	var out T
	if len(env.Result) > 0 && string(env.Result) != "null" {
		if err := json.Unmarshal(env.Result, &out); err != nil {
			t.Fatalf("decode envelope result: %v", err)
		}
	}
	return out
}
