//go:build unit

package service

import (
	"reflect"
	"slices"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

func TestOpenAIOAuthDefaultsToTLSFingerprintEnabled(t *testing.T) {
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
	}

	if !account.IsTLSFingerprintEnabled() {
		t.Fatal("OpenAI OAuth accounts should use TLS fingerprinting by default")
	}
}

func TestOpenAIOAuthCanDisableTLSFingerprint(t *testing.T) {
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{"enable_tls_fingerprint": false},
	}

	if account.IsTLSFingerprintEnabled() {
		t.Fatal("OpenAI OAuth account should allow explicit TLS fingerprint opt-out")
	}
}

func TestAnthropicTLSFingerprintStillRequiresExplicitOptIn(t *testing.T) {
	account := &Account{
		Platform: PlatformAnthropic,
		Type:     AccountTypeOAuth,
	}

	if account.IsTLSFingerprintEnabled() {
		t.Fatal("Anthropic OAuth accounts should keep requiring explicit TLS fingerprint opt-in")
	}
}

func TestOpenAIOAuthResolvesCodexRustlsTLS13Profile(t *testing.T) {
	svc := &TLSFingerprintProfileService{}
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
	}

	profile := svc.ResolveTLSProfile(account)
	if profile == nil {
		t.Fatal("OpenAI OAuth account should resolve a TLS profile")
	}
	if profile.Name != tlsfingerprint.BuiltInCodexCLIProfileName {
		t.Fatalf("profile name = %q, want %q", profile.Name, tlsfingerprint.BuiltInCodexCLIProfileName)
	}

	wantCiphers := []uint16{0x1302, 0x1301, 0x1303}
	if !reflect.DeepEqual(profile.CipherSuites, wantCiphers) {
		t.Fatalf("cipher suites = %#v, want %#v", profile.CipherSuites, wantCiphers)
	}

	if !reflect.DeepEqual(profile.SupportedVersions, []uint16{0x0304}) {
		t.Fatalf("supported versions = %#v, want TLS 1.3 only", profile.SupportedVersions)
	}
	if !reflect.DeepEqual(profile.Curves, []uint16{29, 23, 24}) {
		t.Fatalf("curves = %#v, want Codex rustls aws-lc-rs groups", profile.Curves)
	}
	if !reflect.DeepEqual(profile.KeyShareGroups, []uint16{29}) {
		t.Fatalf("key share groups = %#v, want X25519 only", profile.KeyShareGroups)
	}
	if slices.Contains(profile.Extensions, 16) {
		t.Fatalf("extensions = %#v, should not advertise ALPN for Codex WS rustls", profile.Extensions)
	}

	wantExtensions := []uint16{0, 5, 10, 11, 13, 23, 43, 45, 51}
	gotExtensions := slices.Clone(profile.Extensions)
	slices.Sort(gotExtensions)
	if !reflect.DeepEqual(gotExtensions, wantExtensions) {
		t.Fatalf("extensions set = %#v, want %#v", gotExtensions, wantExtensions)
	}
}
