// Copyright (c) 2015-present Jeevanandam M (jeeva@myjeeva.com), All rights reserved.
// resty source code and usage is governed by a MIT style
// license that can be found in the LICENSE file.
// SPDX-License-Identifier: MIT

package resty

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func newSRVPriorityBalancer(t *testing.T, records ...*net.SRV) *SRVWeightedRoundRobin {
	t.Helper()
	balancer, err := newSRVWeightedRoundRobin("backend", "tcp", "example.invalid", "https",
		func() ([]*net.SRV, error) { return records, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := balancer.Close(); err != nil {
			t.Error(err)
		}
	})
	return balancer
}

func TestSRVPriorityPrefersPrimary(t *testing.T) {
	balancer := newSRVPriorityBalancer(t,
		&net.SRV{Target: "primary.invalid.", Port: 443, Priority: 0, Weight: 1},
		&net.SRV{Target: "backup.invalid.", Port: 443, Priority: 10, Weight: 1})
	for i := 0; i < 4; i++ {
		got, err := balancer.NextWithContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got != "https://primary.invalid:443" {
			t.Fatalf("selection %d = %q; healthy lower-priority-number primary must precede backup", i+1, got)
		}
	}
}

func TestSRVPrioritySamePriorityWeights(t *testing.T) {
	balancer := newSRVPriorityBalancer(t,
		&net.SRV{Target: "one.invalid.", Port: 443, Priority: 10, Weight: 1},
		&net.SRV{Target: "two.invalid.", Port: 443, Priority: 10, Weight: 2})
	counts := make(map[string]int)
	for i := 0; i < 30; i++ {
		got, err := balancer.NextWithContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		counts[got]++
	}
	if counts["https://one.invalid:443"] != 10 || counts["https://two.invalid:443"] != 20 {
		t.Fatalf("same-priority weighted selections = %v; want 10:20", counts)
	}
}

func TestWRRPriorityOrdinaryWeights(t *testing.T) {
	balancer, err := NewWeightedRoundRobin(time.Hour,
		&Host{BaseURL: "https://one.invalid", Weight: 1},
		&Host{BaseURL: "https://two.invalid", Weight: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer balancer.Close()
	counts := make(map[string]int)
	for i := 0; i < 30; i++ {
		got, err := balancer.NextWithContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		counts[got]++
	}
	if counts["https://one.invalid"] != 10 || counts["https://two.invalid"] != 20 {
		t.Fatalf("ordinary weighted selections = %v; want 10:20", counts)
	}
}

func TestSRVPriorityFailoverAndRecovery(t *testing.T) {
	balancer := newSRVPriorityBalancer(t,
		&net.SRV{Target: "one.invalid.", Port: 443, Priority: 0, Weight: 1},
		&net.SRV{Target: "two.invalid.", Port: 443, Priority: 0, Weight: 1},
		&net.SRV{Target: "backup.invalid.", Port: 443, Priority: 10, Weight: 1},
		&net.SRV{Target: "last.invalid.", Port: 443, Priority: 20, Weight: 1})
	next := func(want ...string) {
		t.Helper()
		got, err := balancer.NextWithContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, allowed := range want {
			if got == allowed {
				return
			}
		}
		t.Fatalf("selected %q; want an active host from %v", got, want)
	}
	fail := func(host string) {
		for i := 0; i < 5; i++ {
			balancer.Feedback(&RequestFeedback{BaseURL: host, Success: false})
		}
	}
	const one = "https://one.invalid:443"
	const two = "https://two.invalid:443"
	const backup = "https://backup.invalid:443"
	const last = "https://last.invalid:443"
	next(one, two)
	fail(one)
	next(two)
	fail(two)
	next(backup)
	fail(backup)
	next(last)
	fail(last)
	if _, err := balancer.NextWithContext(context.Background()); err != ErrNoActiveHost {
		t.Fatalf("all hosts unavailable: got %v; want %v", err, ErrNoActiveHost)
	}
	balancer.wrr.recoverHosts()
	next(one, two)
}

func TestSRVPriorityRefresh(t *testing.T) {
	primary := &net.SRV{Target: "primary.invalid.", Port: 443, Priority: 0, Weight: 1}
	backup := &net.SRV{Target: "backup.invalid.", Port: 443, Priority: 10, Weight: 1}
	balancer := newSRVPriorityBalancer(t, primary, backup)
	primary.Priority = 20
	got, err := balancer.NextWithContext(context.Background())
	if err != nil || got != "https://primary.invalid:443" {
		t.Fatalf("before refresh: got %q, %v", got, err)
	}
	if err := balancer.Refresh(); err != nil {
		t.Fatal(err)
	}
	got, err = balancer.NextWithContext(context.Background())
	if err != nil || got != "https://backup.invalid:443" {
		t.Fatalf("after priority refresh: got %q, %v", got, err)
	}
}

func TestSRVPriorityUnsortedRecords(t *testing.T) {
	balancer := newSRVPriorityBalancer(t,
		&net.SRV{Target: "last.invalid.", Port: 443, Priority: 20, Weight: 10},
		&net.SRV{Target: "primary.invalid.", Port: 443, Priority: 0, Weight: 1},
		&net.SRV{Target: "backup.invalid.", Port: 443, Priority: 10, Weight: 100})
	for i := 0; i < 4; i++ {
		got, err := balancer.NextWithContext(context.Background())
		if err != nil || got != "https://primary.invalid:443" {
			t.Fatalf("unsorted SRV selection: got %q, %v", got, err)
		}
	}
}

func TestSRVPriorityMaximum(t *testing.T) {
	balancer := newSRVPriorityBalancer(t,
		&net.SRV{Target: "primary.invalid.", Port: 443, Priority: ^uint16(0), Weight: 1})
	got, err := balancer.NextWithContext(context.Background())
	if err != nil || got != "https://primary.invalid:443" {
		t.Fatalf("maximum valid SRV priority: got %q, %v", got, err)
	}
}

func TestSRVPriorityClientRouting(t *testing.T) {
	balancer := newSRVPriorityBalancer(t,
		&net.SRV{Target: "primary.invalid.", Port: 443, Priority: 0, Weight: 1},
		&net.SRV{Target: "backup.invalid.", Port: 443, Priority: 10, Weight: 1})
	calls := 0
	client := New().SetLoadBalancer(balancer).SetTransport(feedbackRoundTripper(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Host != "primary.invalid:443" {
			t.Errorf("HTTP request routed to %q while primary is healthy", req.URL.Host)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader("ok")), Request: req}, nil
	}))
	defer client.Close()
	for i := 0; i < 2; i++ {
		response, err := client.R().Get("/jobs")
		if err != nil || response == nil || response.StatusCode() != http.StatusOK {
			t.Fatalf("HTTP request: response=%v error=%v", response, err)
		}
	}
	if calls != 2 {
		t.Fatalf("transport calls = %d; want 2", calls)
	}
}
