package store

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"go_bizbos/internal/config"
)

func Open(c *config.Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=%s",
		c.DBUser, c.DBPass, c.DBHost, c.DBPort, c.DBName, url.QueryEscape(c.Location.String()))

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.New(log.New(os.Stdout, "", log.LstdFlags), logger.Config{
			SlowThreshold:             300 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
		}),
		NowFunc: func() time.Time { return time.Now().In(c.Location).Truncate(time.Second) },
	})
	if err != nil {
		return nil, fmt.Errorf("koneksi database gagal: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(c.DBMaxOpen)
	sqlDB.SetMaxIdleConns(c.DBMaxOpen / 2)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	return db, nil
}

func NewID() string { return uuid.NewString() }

func Ptr[T any](v T) *T { return &v }
