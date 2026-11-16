package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// HTTPS callbacks are untrusted inputs. Validate DNS at the actual connection,
// dial the validated IP, retain the hostname for TLS, and disable proxies,
// connection reuse and redirects. Tests inject a separate in-memory transport.
func webhookClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{DisableKeepAlives: true, TLSHandshakeTimeout: 5 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second, MaxResponseHeaderBytes: 16 << 10,
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(address)
				if err != nil || port != "443" {
					return nil, errors.New("invalid callback address")
				}
				ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
				if err != nil || len(ips) == 0 {
					return nil, errors.New("callback DNS failed")
				}
				for _, ip := range ips {
					if !publicCallbackIP(ip) {
						return nil, errors.New("non-public callback address")
					}
				}
				return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
			},
		},
	}
}

var blockedCallbackNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

func publicCallbackIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.Zone() != "" || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, prefix := range blockedCallbackNetworks {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

func validateCallback(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 4096 || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || (u.Port() != "" && u.Port() != "443") {
		return errors.New("callback must be an HTTPS URL on port 443, without credentials or a fragment")
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || !strings.Contains(host, ".") && !strings.Contains(host, ":") {
		return errors.New("callback must use a public host")
	}
	if ip, err := netip.ParseAddr(host); err == nil && !publicCallbackIP(ip) {
		return errors.New("callback must use a public address")
	}
	return nil
}

func webhookKey(secret string) ([]byte, error) {
	if !strings.HasPrefix(secret, "whsec_") {
		return nil, errors.New("a whsec_ signing secret is required")
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil || len(key) < 24 || len(key) > 64 {
		return nil, errors.New("signing secret must decode to 24..64 bytes")
	}
	return key, nil
}

func webhookSignature(secret, id, timestamp string, body []byte) string {
	key, _ := webhookKey(secret)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(id + "." + timestamp + "."))
	_, _ = mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (h *eventHub) post(ctx context.Context, s eventSubscription, id string, body []byte) (int, []byte, error) {
	if len(body) > 262144 {
		return http.StatusRequestEntityTooLarge, nil, nil
	}
	if err := validateCallback(s.URL); err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", s.URL, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	signature := webhookSignature(s.Secret, id, stamp, body)
	if s.OldSecret != "" && s.OldUntil > time.Now().UnixMilli() {
		signature += " " + webhookSignature(s.OldSecret, id, stamp, body)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("webhook-id", id)
	req.Header.Set("webhook-timestamp", stamp)
	req.Header.Set("webhook-signature", signature)
	req.Header.Set("X-MCP-Subscription-Id", s.ID)
	resp, err := h.http.Do(req)
	if err != nil {
		return 0, nil, errors.New("callback request failed")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(data) > 4096 {
		return resp.StatusCode, nil, errors.New("invalid callback response")
	}
	return resp.StatusCode, data, nil
}
