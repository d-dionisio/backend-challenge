package auth

import (
	"errors"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	IssuerURL    string
	Audience     string
	ClientID     string
	ClientSecret string
}

func NewConfig() (Config, error) {
	c := Config{IssuerURL: os.Getenv("OIDC_ISSUER_URL"), Audience: os.Getenv("OIDC_AUDIENCE"),
		ClientID: os.Getenv("OIDC_CLIENT_ID"), ClientSecret: os.Getenv("OIDC_CLIENT_SECRET")}
	return c, c.validate()
}

func (c Config) validate() error {
	u, err := url.Parse(c.IssuerURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.HasSuffix(c.IssuerURL, "/") {
		return errors.New("OIDC_ISSUER_URL must be an absolute issuer URL without trailing slash")
	}
	if strings.TrimSpace(c.Audience) == "" || strings.TrimSpace(c.ClientID) == "" || strings.TrimSpace(c.ClientSecret) == "" {
		return errors.New("OIDC_AUDIENCE, OIDC_CLIENT_ID and OIDC_CLIENT_SECRET are required")
	}
	return nil
}
