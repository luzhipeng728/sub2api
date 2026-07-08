package service

import (
	"context"
	"testing"
	"time"
)

type fakeCodexOpsRepo struct {
	OpsRepository
	accountTraffic []*OpsCodexAccountTraffic
	blockedReasons []*OpsCodexBlockedReason
	vistaraSummary *OpsVistaraCostSummary
}

func (f *fakeCodexOpsRepo) GetCodexAccountTraffic(ctx context.Context, since, until time.Time) ([]*OpsCodexAccountTraffic, error) {
	return f.accountTraffic, nil
}
func (f *fakeCodexOpsRepo) GetCodexBlockedReasonBreakdown(ctx context.Context, since, until time.Time) ([]*OpsCodexBlockedReason, error) {
	return f.blockedReasons, nil
}
func (f *fakeCodexOpsRepo) GetVistaraCostSummary(ctx context.Context, now time.Time) (*OpsVistaraCostSummary, error) {
	return f.vistaraSummary, nil
}

func TestOpsServiceGetCodexOverviewAggregatesAcrossAccounts(t *testing.T) {
	repo := &fakeCodexOpsRepo{
		accountTraffic: []*OpsCodexAccountTraffic{
			{AccountID: 64, SuccessCount: 100, Error429Count: 5, ErrorOtherCount: 1},
			{AccountID: 70, SuccessCount: 50, Error429Count: 0, ErrorOtherCount: 0},
		},
		blockedReasons: []*OpsCodexBlockedReason{{Reason: "rate_limit", Count: 5}},
		vistaraSummary: &OpsVistaraCostSummary{},
	}
	svc := &OpsService{opsRepo: repo}

	until := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	since := until.Add(-time.Hour)

	overview, err := svc.GetCodexOverview(context.Background(), since, until)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if overview.TotalTraffic != 156 {
		t.Fatalf("TotalTraffic = %d, want 156", overview.TotalTraffic)
	}
	if overview.SuccessCount != 150 {
		t.Fatalf("SuccessCount = %d, want 150", overview.SuccessCount)
	}
	if overview.AccountsUsed != 2 {
		t.Fatalf("AccountsUsed = %d, want 2", overview.AccountsUsed)
	}
	if overview.RPM1h != 150 {
		t.Fatalf("RPM1h = %.2f, want 150", overview.RPM1h)
	}
}
