package redisqueue

import (
	"strconv"
	"testing"
)

func TestQueueDropsOldestWhenItemCapExceeded(t *testing.T) {
	const queueLimit = 10000

	prevQueueEnabled := Enabled()
	SetEnabled(false)
	SetEnabled(true)
	t.Cleanup(func() {
		SetEnabled(false)
		SetEnabled(prevQueueEnabled)
	})

	for i := 0; i < queueLimit+5; i++ {
		Enqueue([]byte(strconv.Itoa(i)))
	}

	items := PopOldest(queueLimit + 10)
	if len(items) != queueLimit {
		t.Fatalf("items len = %d, want %d", len(items), queueLimit)
	}
	if string(items[0]) != "5" {
		t.Fatalf("oldest retained item = %q, want %q", string(items[0]), "5")
	}
}
