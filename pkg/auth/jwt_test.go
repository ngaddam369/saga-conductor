package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/ngaddam369/saga-conductor/pkg/auth"
)

// rsaJWK converts an RSA public key to JWK JSON representation.
func rsaJWK(kid string, pub *rsa.PublicKey) map[string]any {
	eBytes := big.NewInt(int64(pub.E)).Bytes()
	return map[string]any{
		"kty": "RSA",
		"alg": "RS256",
		"use": "sig",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(eBytes),
	}
}

// ecJWK converts an ECDSA P-256 public key to JWK JSON representation.
// It uses pub.ECDH().Bytes() to extract the uncompressed point coordinates
// without accessing the deprecated big.Int fields directly.
func ecJWK(t *testing.T, kid string, pub *ecdsa.PublicKey) map[string]any {
	t.Helper()
	ecdhKey, err := pub.ECDH()
	if err != nil {
		t.Fatalf("ECDH: %v", err)
	}
	// Bytes() returns 0x04 || X || Y (uncompressed form).
	raw := ecdhKey.Bytes()
	coordLen := (pub.Curve.Params().BitSize + 7) / 8
	x := raw[1 : 1+coordLen]
	y := raw[1+coordLen:]
	return map[string]any{
		"kty": "EC",
		"alg": "ES256",
		"use": "sig",
		"kid": kid,
		"crv": "P-256",
		"x":   base64.RawURLEncoding.EncodeToString(x),
		"y":   base64.RawURLEncoding.EncodeToString(y),
	}
}

// jwksServer returns an httptest.Server that serves the given keys as a JWKS.
func jwksServer(t *testing.T, keys ...map[string]any) *httptest.Server {
	t.Helper()
	body, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatalf("marshal JWKS: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// signRS256 mints a signed RS256 JWT with the given claims.
func signRS256(t *testing.T, priv *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign RS256: %v", err)
	}
	return s
}

// signES256 mints a signed ES256 JWT with the given claims.
func signES256(t *testing.T, priv *ecdsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign ES256: %v", err)
	}
	return s
}

func validClaims() jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{
		"sub": "test-user",
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}
}

func TestJWTValidator(t *testing.T) {
	t.Parallel()

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate EC key: %v", err)
	}

	t.Run("valid RS256 token is accepted", func(t *testing.T) {
		t.Parallel()
		srv := jwksServer(t, rsaJWK("rsa-1", &rsaKey.PublicKey))
		v := auth.NewJWTValidator(srv.URL, time.Minute)
		tok := signRS256(t, rsaKey, "rsa-1", validClaims())
		if err := v.Validate(context.Background(), tok); err != nil {
			t.Errorf("Validate: unexpected error: %v", err)
		}
	})

	t.Run("valid ES256 token is accepted", func(t *testing.T) {
		t.Parallel()
		srv := jwksServer(t, ecJWK(t, "ec-1", &ecKey.PublicKey))
		v := auth.NewJWTValidator(srv.URL, time.Minute)
		tok := signES256(t, ecKey, "ec-1", validClaims())
		if err := v.Validate(context.Background(), tok); err != nil {
			t.Errorf("Validate: unexpected error: %v", err)
		}
	})

	t.Run("empty token is rejected", func(t *testing.T) {
		t.Parallel()
		srv := jwksServer(t, rsaJWK("rsa-1", &rsaKey.PublicKey))
		v := auth.NewJWTValidator(srv.URL, time.Minute)
		if err := v.Validate(context.Background(), ""); err == nil {
			t.Error("Validate: expected error for empty token, got nil")
		}
	})

	t.Run("expired token is rejected", func(t *testing.T) {
		t.Parallel()
		srv := jwksServer(t, rsaJWK("rsa-1", &rsaKey.PublicKey))
		v := auth.NewJWTValidator(srv.URL, time.Minute)
		claims := jwt.MapClaims{
			"sub": "test-user",
			"iat": time.Now().Add(-2 * time.Hour).Unix(),
			"exp": time.Now().Add(-time.Hour).Unix(), // expired
		}
		tok := signRS256(t, rsaKey, "rsa-1", claims)
		if err := v.Validate(context.Background(), tok); err == nil {
			t.Error("Validate: expected error for expired token, got nil")
		}
	})

	t.Run("tampered signature is rejected", func(t *testing.T) {
		t.Parallel()
		srv := jwksServer(t, rsaJWK("rsa-1", &rsaKey.PublicKey))
		v := auth.NewJWTValidator(srv.URL, time.Minute)
		tok := signRS256(t, rsaKey, "rsa-1", validClaims())
		// Replace the signature segment with random bytes to corrupt it.
		parts := strings.SplitN(tok, ".", 3)
		fakeSig := make([]byte, 256)
		if _, randErr := rand.Read(fakeSig); randErr != nil {
			t.Fatalf("rand: %v", randErr)
		}
		parts[2] = base64.RawURLEncoding.EncodeToString(fakeSig)
		corrupted := strings.Join(parts, ".")
		if err := v.Validate(context.Background(), corrupted); err == nil {
			t.Error("Validate: expected error for tampered token, got nil")
		}
	})

	t.Run("unknown kid is rejected", func(t *testing.T) {
		t.Parallel()
		srv := jwksServer(t, rsaJWK("rsa-1", &rsaKey.PublicKey))
		v := auth.NewJWTValidator(srv.URL, time.Minute)
		tok := signRS256(t, rsaKey, "unknown-kid", validClaims())
		if err := v.Validate(context.Background(), tok); err == nil {
			t.Error("Validate: expected error for unknown kid, got nil")
		}
	})

	t.Run("HS256 token is rejected by algorithm whitelist", func(t *testing.T) {
		t.Parallel()
		srv := jwksServer(t, rsaJWK("rsa-1", &rsaKey.PublicKey))
		v := auth.NewJWTValidator(srv.URL, time.Minute)
		// HS256 with a random secret — should be blocked before key lookup.
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims())
		tok.Header["kid"] = "rsa-1"
		signed, err := tok.SignedString([]byte("some-hmac-secret"))
		if err != nil {
			t.Fatalf("sign HS256: %v", err)
		}
		if err := v.Validate(context.Background(), signed); err == nil {
			t.Error("Validate: expected error for HS256 token, got nil")
		}
	})

	t.Run("JWKS endpoint returns 500", func(t *testing.T) {
		t.Parallel()
		errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}))
		t.Cleanup(errSrv.Close)
		v := auth.NewJWTValidator(errSrv.URL, time.Minute)
		tok := signRS256(t, rsaKey, "rsa-1", validClaims())
		if err := v.Validate(context.Background(), tok); err == nil {
			t.Error("Validate: expected error when JWKS endpoint fails, got nil")
		}
	})

	t.Run("key is served from cache on second call", func(t *testing.T) {
		t.Parallel()
		calls := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			w.Header().Set("Content-Type", "application/json")
			body, _ := json.Marshal(map[string]any{"keys": []any{rsaJWK("rsa-1", &rsaKey.PublicKey)}})
			_, _ = w.Write(body)
		}))
		t.Cleanup(srv.Close)

		v := auth.NewJWTValidator(srv.URL, time.Minute)
		tok := signRS256(t, rsaKey, "rsa-1", validClaims())

		if err := v.Validate(context.Background(), tok); err != nil {
			t.Fatalf("first Validate: %v", err)
		}
		if err := v.Validate(context.Background(), tok); err != nil {
			t.Fatalf("second Validate: %v", err)
		}
		if calls != 1 {
			t.Errorf("JWKS fetched %d times; want 1 (second call must hit cache)", calls)
		}
	})

	t.Run("single key without kid is accepted when JWKS has one key", func(t *testing.T) {
		t.Parallel()
		// Some providers issue tokens with no kid when they only have one key.
		srv := jwksServer(t, rsaJWK("rsa-1", &rsaKey.PublicKey))
		v := auth.NewJWTValidator(srv.URL, time.Minute)
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, validClaims())
		// Deliberately omit kid from the header.
		signed, err := tok.SignedString(rsaKey)
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		if err := v.Validate(context.Background(), signed); err != nil {
			t.Errorf("Validate: unexpected error: %v", err)
		}
	})
}
