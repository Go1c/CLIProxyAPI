package proxyutil

import (
	"testing"
	"time"
)

func TestAuthCircuitOpensAfterTwoHeaderTimeoutsWithinWindow(t *testing.T) {
	ResetHealthForTesting()
	t.Cleanup(ResetHealthForTesting)
	now := time.Now()
	errTimeout := NewError(CodeUpstreamHeaderTimeout, StageUpstreamHeader, true, "proxy-a", "", nil)
	if transition := RecordFailure("auth-a", errTimeout, now); transition.AuthOpened {
		t.Fatal("auth circuit opened after one timeout")
	}
	if transition := RecordFailure("auth-a", errTimeout, now.Add(time.Minute)); !transition.AuthOpened {
		t.Fatal("auth circuit did not open after two timeouts")
	}
	if blocked, _ := Blocked("auth-a", "proxy-a", now.Add(2*time.Minute)); !blocked {
		t.Fatal("auth should be isolated")
	}
}

func TestProxyCircuitOpensAndRecoversAfterTwoProbes(t *testing.T) {
	ResetHealthForTesting()
	t.Cleanup(ResetHealthForTesting)
	now := time.Now()
	errConnect := NewError(CodeConnectTimeout, StageProxyConnect, true, "proxy-a", "", nil)
	for i := 0; i < 2; i++ {
		if transition := RecordFailure("auth-a", errConnect, now.Add(time.Duration(i)*time.Second)); transition.ProxyOpened {
			t.Fatalf("proxy circuit opened after %d failures", i+1)
		}
	}
	if transition := RecordFailure("auth-b", errConnect, now.Add(2*time.Second)); !transition.ProxyOpened {
		t.Fatal("proxy circuit did not open after three consecutive failures")
	}
	if !TryBeginProbe("proxy-a", now.Add(time.Minute)) {
		t.Fatal("first half-open probe was not admitted")
	}
	if closed := RecordProbe("proxy-a", true, "EWR", 20*time.Millisecond, "", now.Add(time.Minute)); closed {
		t.Fatal("proxy circuit closed after one successful probe")
	}
	if !TryBeginProbe("proxy-a", now.Add(2*time.Minute)) {
		t.Fatal("second half-open probe was not admitted")
	}
	if closed := RecordProbe("proxy-a", true, "EWR", 18*time.Millisecond, "", now.Add(2*time.Minute)); !closed {
		t.Fatal("proxy circuit did not close after two successful probes")
	}
	if blocked, _ := Blocked("auth-a", "proxy-a", now.Add(2*time.Minute)); blocked {
		t.Fatal("proxy remained isolated after recovery")
	}
}
