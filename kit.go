package gosl

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Key int

var STACK Key = 101

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

// RunInTransaction db wrapper for transaction
func RunInTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	var err error
	callCount, ok := ctx.Value(STACK).(int)
	trxs := make([]*sqlx.Tx, 0)
	var currKey interface{}
	var injectTx func(curr interface{})
	injectTx = func(curr interface{}) {
		values := reflect.ValueOf(curr).Elem()
		keys := reflect.TypeOf(curr).Elem()
		if keys.Kind() == reflect.Struct {
			for i := 0; i < values.NumField(); i++ {
				value := values.Field(i)
				value = reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem()
				field := keys.Field(i)
				if field.Name == "key" {
					currKey = value.Interface()
				} else if field.Name != "Context" {
					q, ok := value.Interface().(*Queryable)
					if !ok {
						if tmp := value.Interface(); tmp != nil {
							kind := reflect.TypeOf(tmp).Kind()
							if kind == reflect.Pointer {
								injectTx(tmp)
							}
						}
						continue
					}
					if nil == q.db {
						err = errors.New("no active db con")
						return
					}
					if nil == q.tx {
						var tx *sqlx.Tx
						tx, err = q.db.Beginx()
						if nil != err {
							return
						}
						trxs = append(trxs, tx)
						con := make(map[string]interface{})
						con["db"] = q.db
						con["tx"] = tx
						ctx = context.WithValue(ctx, currKey, NewQueryable(con))
					}
				}
			}
		}

	}
	if !ok {
		callCount = 0
		injectTx(ctx)
		if err != nil {
			return err
		}
	}
	ctx = context.WithValue(ctx, STACK, callCount+1)
	err = fn(ctx)
	if nil != err {
		for _, trx := range trxs {
			_ = trx.Rollback()
		}
		return err
	}
	if callCount == 0 {
		for _, trx := range trxs {
			err := trx.Commit()
			if err != nil {
				for _, itrx := range trxs {
					_ = itrx.Rollback()
				}
				return fmt.Errorf("error when committing transaction: %v", err)
			}
		}
	}
	return nil
}
