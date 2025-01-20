package gosl

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// ConnectToDB simple wrapper for db connection with sqlx
func ConnectToDB(user, password, host, port, name string, maxOpen, maxIdle int, maxLifetime, maxIdleLifetime time.Duration) *sqlx.DB {
	db := sqlx.MustConnect("mysql", fmt.Sprintf(
		"%s:%s@(%s:%s)/%s?parseTime=true",
		user,
		password,
		host,
		port,
		name))

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLifetime)
	db.SetConnMaxIdleTime(maxIdleLifetime)

	return db
}
