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
		// Includes a "no_available_account" reason, which by definition can
		// only occur before an account is selected, so it never appears in
		// GetCodexAccountTraffic's account_id IS NOT NULL rows above (whose
		// 429+other blocks sum to only 6). BlockedCount must still reconcile
		// with the full sum here (5+1+3=9), not the smaller per-account sum.
		blockedReasons: []*OpsCodexBlockedReason{
			{Reason: "rate_limit", Count: 5},
			{Reason: "other", Count: 1},
			{Reason: "no_available_account", Count: 3},
		},
		vistaraSummary: &OpsVistaraCostSummary{},
	}
	svc := &OpsService{opsRepo: repo}

	until := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	since := until.Add(-time.Hour)

	overview, err := svc.GetCodexOverview(context.Background(), since, until)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if overview.SuccessCount != 150 {
		t.Fatalf("SuccessCount = %d, want 150", overview.SuccessCount)
	}
	if overview.BlockedCount != 9 {
		t.Fatalf("BlockedCount = %d, want 9 (reconciled with BlockedReasons sum, including NULL-account rows)", overview.BlockedCount)
	}
	if overview.TotalTraffic != 159 {
		t.Fatalf("TotalTraffic = %d, want 159 (SuccessCount + reconciled BlockedCount)", overview.TotalTraffic)
	}
	if overview.AccountsUsed != 2 {
		t.Fatalf("AccountsUsed = %d, want 2", overview.AccountsUsed)
	}
	if overview.RPM1h != 2.5 {
		t.Fatalf("RPM1h = %.2f, want 2.5", overview.RPM1h)
	}
}
