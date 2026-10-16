package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Env      string
	Addr     string
	DataDir  string
	Location *time.Location

	DBHost, DBPort, DBName, DBUser, DBPass string
	DBMaxOpen                              int

	// Tiga rahasia terpisah: bocor satu tidak membuka yang lain.
	APISecret     string // bizbos -> go (tanda tangan request)
	AccessSecret  string // token singkat untuk browser (SSE & media)
	WebhookSecret string // go -> bizbos
	WebhookURL    string

	AllowedOrigins []string

	BlastMinDelay  time.Duration
	BlastMaxDelay  time.Duration
	BlastDailyCap  int
	QuietStartHour int
	QuietEndHour   int
	MaxParallel    int
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	loc, err := time.LoadLocation(env("APP_TIMEZONE", "Asia/Jakarta"))
	if err != nil {
		return nil, fmt.Errorf("APP_TIMEZONE tidak dikenal: %w", err)
	}

	c := &Config{
		Env:            env("APP_ENV", "production"),
		Addr:           env("HTTP_ADDR", "127.0.0.1:8090"),
		DataDir:        env("DATA_DIR", "data"),
		Location:       loc,
		DBHost:         env("DB_HOST", "127.0.0.1"),
		DBPort:         env("DB_PORT", "3306"),
		DBName:         env("DB_DATABASE", ""),
		DBUser:         env("DB_USERNAME", ""),
		DBPass:         os.Getenv("DB_PASSWORD"),
		DBMaxOpen:      envInt("DB_MAX_OPEN", 30),
		APISecret:      os.Getenv("WA_API_SECRET"),
		AccessSecret:   os.Getenv("WA_ACCESS_SECRET"),
		WebhookSecret:  os.Getenv("WA_WEBHOOK_SECRET"),
		WebhookURL:     env("WA_WEBHOOK_URL", ""),
		AllowedOrigins: splitList(env("ALLOWED_ORIGINS", "")),
		BlastMinDelay:  time.Duration(envInt("BLAST_MIN_DELAY", 8)) * time.Second,
		BlastMaxDelay:  time.Duration(envInt("BLAST_MAX_DELAY", 20)) * time.Second,
		BlastDailyCap:  envInt("BLAST_DAILY_CAP", 200),
		QuietStartHour: envInt("QUIET_START_HOUR", 21),
		QuietEndHour:   envInt("QUIET_END_HOUR", 7),
		MaxParallel:    envInt("MAX_PARALLEL_SENDS", 20),
	}
	return c, c.validate()
}

func (c *Config) validate() error {
	if c.DBName == "" || c.DBUser == "" {
		return fmt.Errorf("DB_DATABASE dan DB_USERNAME wajib diisi")
	}
	secrets := map[string]string{"WA_API_SECRET": c.APISecret, "WA_ACCESS_SECRET": c.AccessSecret, "WA_WEBHOOK_SECRET": c.WebhookSecret}
	seen := map[string]string{}
	for name, v := range secrets {
		if len(v) < 32 {
			return fmt.Errorf("%s minimal 32 karakter", name)
		}
		if other, dup := seen[v]; dup {
			return fmt.Errorf("%s dan %s tidak boleh sama", name, other)
		}
		seen[v] = name
	}
	if c.BlastMaxDelay < c.BlastMinDelay {
		c.BlastMaxDelay = c.BlastMinDelay
	}
	return nil
}

func (c *Config) IsLocal() bool { return c.Env == "local" }

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, strings.TrimRight(p, "/"))
		}
	}
	return out
}
