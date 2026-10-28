package taskartifact

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type openFetcherFunc func(context.Context, store.ContentKey) (fetch.Stream, error)

func (f openFetcherFunc) OpenManifest(ctx context.Context, key store.ContentKey) (fetch.Stream, error) {
	return f(ctx, key)
}

type ownedOpenStream struct {
	fetch.Stream
	closed int
}

func (*ownedOpenStream) Size() uint64   { return 4096 }
func (s *ownedOpenStream) Close() error { s.closed++; return nil }

func TestManifestOpenRecoveryPreservesScopedOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &ownedOpenStream{}
	calls := 0
	key := store.ContentKey{}
	for i := range key {
		key[i] = 0xaa
	}
	scoped := openFetcherFunc(func(got context.Context, k store.ContentKey) (fetch.Stream, error) {
		calls++
		if got != ctx || k != key {
			t.Fatal("open lost caller context or source identity")
		}
		if calls == 1 {
			return nil, errors.New("client construction: connection refused")
		}
		if calls == 2 {
			return nil, errors.New("existing connection: unexpected EOF")
		}
		return stream, nil
	})
	// Nil ProcessStorage deliberately ensures the supplied scoped capability is
	// used on every attempt, rather than reconstructed without Bundle scope.
	got, err := openArtifactStreamWithFetcher(ctx, "manifest://"+strings.Repeat("a", 64), "", nil, nil, scoped)
	if err != nil || got != stream || calls != 3 || stream.closed != 0 {
		t.Fatalf("stream=%v err=%v calls=%d closed=%d", got, err, calls, stream.closed)
	}
	if err := got.Close(); err != nil || stream.closed != 1 {
		t.Fatal("successful stream ownership lost")
	}
}

func TestManifestOpenRecoveryStopsAtPermanentError(t *testing.T) {
	permanent := readerr.Mark(errors.New("wrong tenant credentials"), false)
	calls := 0
	f := openFetcherFunc(func(context.Context, store.ContentKey) (fetch.Stream, error) { calls++; return nil, permanent })
	_, err := openArtifactStreamWithFetcher(context.Background(), "manifest://"+strings.Repeat("a", 64), "", nil, nil, f)
	if !errors.Is(err, permanent) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestManifestOpenRecoveryCancellationReleasesSuccessfulRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &ownedOpenStream{}
	f := openFetcherFunc(func(context.Context, store.ContentKey) (fetch.Stream, error) { cancel(); return s, nil })
	got, err := openArtifactStreamWithFetcher(ctx, "manifest://"+strings.Repeat("a", 64), "", nil, nil, f)
	if !errors.Is(err, context.Canceled) || got != nil || s.closed != 1 {
		t.Fatalf("stream=%v err=%v closed=%d", got, err, s.closed)
	}
}

func TestManifestOpenRecoveryCancellationInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	f := openFetcherFunc(func(context.Context, store.ContentKey) (fetch.Stream, error) {
		calls++
		cancel()
		return nil, errors.New("unavailable")
	})
	_, err := openArtifactStreamWithFetcher(ctx, "manifest://"+strings.Repeat("a", 64), "", nil, nil, f)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
