package gosl

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type Kit interface {
	RunInTransaction(ctx context.Context, handler func(ctx context.Context) error, others ...any) error
	ContextSwitch(ctx context.Context, key interface{}) context.Context
	ContextReset(ctx context.Context) context.Context
}

func New(ctx context.Context) Kit {
	return &kit{
		cache: ctx.Value(SQL_KEY),
	}
}

type kit struct {
	cache interface{}
}

func (k *kit) RunInTransaction(ctx context.Context, handler func(ctx context.Context) error, others ...any) error {
	var err error
	trxs := make([]*sqlx.Tx, 0)
	goslKeyExists := false
	for _, key := range others {
		if _, ok := key.(Gosl_Key); ok {
			goslKeyExists = true
		}
	}
	if !goslKeyExists {
		others = append(others, SQL_KEY)
	}
	callCount, ok := ctx.Value(SYSTEM_STACK).(int)
	if !ok {
		callCount = 0
	}
	if callCount == 0 {
		for _, key := range others {
			if queryable, ok := ctx.Value(key).(*Queryable); ok {
				var tx *sqlx.Tx
				tx, err = queryable.db.Beginx()
				if err != nil {
					return err
				}
				con := make(map[string]interface{})
				con["db"] = queryable.db
				con["tx"] = tx
				ctx = context.WithValue(ctx, key, NewQueryable(con))
				trxs = append(trxs, tx)
			}
		}
	}
	ctx = context.WithValue(ctx, SYSTEM_STACK, callCount+1)
	err = handler(ctx)
	if err != nil {
		for _, trx := range trxs {
			_ = trx.Rollback()
		}
		return err
	} else if callCount == 0 {
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

func (k *kit) ContextSwitch(ctx context.Context, key interface{}) context.Context {
	ctx = context.WithValue(ctx, SQL_KEY, k.cache)
	toBeAssigned := ctx.Value(key)
	return context.WithValue(ctx, SQL_KEY, toBeAssigned)
}

func (k *kit) ContextReset(ctx context.Context) context.Context {
	return context.WithValue(ctx, SQL_KEY, k.cache)
}
