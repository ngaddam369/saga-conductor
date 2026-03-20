package auth

import (
	"context"
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// allowedAlgorithms restricts JWT verification to RS256 and ES256.
// This excludes "none" (unsigned tokens) and symmetric HS* algorithms
// (which would allow forged tokens using the JWKS public key bytes as the HMAC secret).
var allowedAlgorithms = jwt.WithValidMethods([]string{"RS256", "ES256"})

// jwk is a single entry in a JWKS document.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	// RSA fields.
	N string `json:"n"`
	E string `json:"e"`
	// EC fields.
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type jwks struct {
	Keys []jwk `json:"keys"`
}

// JWTValidator is a TokenValidator that verifies RS256 or ES256 JWTs against
// a configured JWKS endpoint. Public keys are cached with a configurable TTL
// so the endpoint is not hit on every inbound gRPC call. Key selection uses
// the JWT's "kid" header when present; if absent and the JWKS contains exactly
// one key, that key is used.
type JWTValidator struct {
	jwksURL    string
	cacheTTL   time.Duration
	httpClient *http.Client

	mu       sync.Mutex
	keyCache map[string]crypto.PublicKey
	cachedAt time.Time
}

// NewJWTValidator returns a JWTValidator that fetches public keys from jwksURL
// and caches them for cacheTTL. Pass 0 for cacheTTL to use the default (5 min).
func NewJWTValidator(jwksURL string, cacheTTL time.Duration) *JWTValidator {
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Minute
	}
	return &JWTValidator{
		jwksURL:    jwksURL,
		cacheTTL:   cacheTTL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Validate implements server.TokenValidator. It parses and verifies the JWT,
// rejecting tokens that are expired, not-yet-valid, have an unsupported
// algorithm, or whose signature cannot be verified against the JWKS.
func (v *JWTValidator) Validate(ctx context.Context, token string) error {
	if token == "" {
		return errors.New("missing token")
	}
	_, err := jwt.Parse(token, v.keyFunc(ctx), allowedAlgorithms)
	if err != nil {
		return fmt.Errorf("invalid token: %w", err)
	}
	return nil
}

func (v *JWTValidator) keyFunc(ctx context.Context) jwt.Keyfunc {
	return func(t *jwt.Token) (any, error) {
		var kid string
		if v, ok := t.Header["kid"].(string); ok {
			kid = v
		}

		// Fast path: key is already in the cache.
		key, ok := v.cachedKey(kid)
		if ok {
			return key, nil
		}

		// Cache miss or expired — fetch fresh keys.
		if err := v.refresh(ctx); err != nil {
			return nil, fmt.Errorf("refresh JWKS: %w", err)
		}

		key, ok = v.cachedKey(kid)
		if !ok {
			if kid == "" {
				return nil, errors.New("JWKS contains multiple keys and token has no kid header")
			}
			return nil, fmt.Errorf("kid %q not found in JWKS", kid)
		}
		return key, nil
	}
}

// cachedKey returns the public key for kid from the in-memory cache.
// If kid is empty and the cache holds exactly one key, that key is returned.
func (v *JWTValidator) cachedKey(kid string) (crypto.PublicKey, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.keyCache == nil || time.Since(v.cachedAt) > v.cacheTTL {
		return nil, false
	}
	if kid != "" {
		k, ok := v.keyCache[kid]
		return k, ok
	}
	// No kid: only unambiguous when the JWKS has exactly one key.
	if len(v.keyCache) == 1 {
		for _, k := range v.keyCache {
			return k, true
		}
	}
	return nil, false
}

func (v *JWTValidator) refresh(ctx context.Context) error {
	keys, err := v.fetchJWKS(ctx)
	if err != nil {
		return err
	}
	v.mu.Lock()
	v.keyCache = keys
	v.cachedAt = time.Now()
	v.mu.Unlock()
	return nil
}

func (v *JWTValidator) fetchJWKS(ctx context.Context) (_ map[string]crypto.PublicKey, retErr error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := v.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("close JWKS body: %w", closeErr)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JWKS endpoint returned %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MB cap
	if err != nil {
		return nil, fmt.Errorf("read JWKS body: %w", err)
	}
	var set jwks
	if err := json.Unmarshal(body, &set); err != nil {
		return nil, fmt.Errorf("parse JWKS: %w", err)
	}
	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		pub, err := jwkToPublicKey(k)
		if err != nil {
			return nil, fmt.Errorf("parse key %q: %w", k.Kid, err)
		}
		keys[k.Kid] = pub
	}
	return keys, nil
}

func jwkToPublicKey(k jwk) (crypto.PublicKey, error) {
	switch k.Kty {
	case "RSA":
		return jwkToRSA(k)
	case "EC":
		return jwkToEC(k)
	default:
		return nil, fmt.Errorf("unsupported key type %q", k.Kty)
	}
}

func jwkToRSA(k jwk) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("decode n: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("decode e: %w", err)
	}
	n := new(big.Int).SetBytes(nBytes)
	var e int
	for _, b := range eBytes {
		e = e<<8 | int(b)
	}
	if e == 0 {
		return nil, errors.New("RSA exponent is zero")
	}
	return &rsa.PublicKey{N: n, E: e}, nil
}

func jwkToEC(k jwk) (*ecdsa.PublicKey, error) {
	var curve elliptic.Curve
	var ecdhCurve ecdh.Curve
	switch k.Crv {
	case "P-256":
		curve, ecdhCurve = elliptic.P256(), ecdh.P256()
	case "P-384":
		curve, ecdhCurve = elliptic.P384(), ecdh.P384()
	case "P-521":
		curve, ecdhCurve = elliptic.P521(), ecdh.P521()
	default:
		return nil, fmt.Errorf("unsupported EC curve %q", k.Crv)
	}
	xBytes, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil {
		return nil, fmt.Errorf("decode x: %w", err)
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil {
		return nil, fmt.Errorf("decode y: %w", err)
	}
	// Validate the point using crypto/ecdh (the non-deprecated on-curve check).
	// Encode as uncompressed point (0x04 || X || Y), padding to the curve's
	// coordinate byte length so NewPublicKey accepts the input.
	coordLen := (curve.Params().BitSize + 7) / 8
	uncompressed := make([]byte, 1+2*coordLen)
	uncompressed[0] = 0x04
	copy(uncompressed[1+coordLen-len(xBytes):1+coordLen], xBytes)
	copy(uncompressed[1+2*coordLen-len(yBytes):], yBytes)
	if _, err := ecdhCurve.NewPublicKey(uncompressed); err != nil {
		return nil, fmt.Errorf("invalid EC point: %w", err)
	}
	return &ecdsa.PublicKey{Curve: curve, X: new(big.Int).SetBytes(xBytes), Y: new(big.Int).SetBytes(yBytes)}, nil
}
