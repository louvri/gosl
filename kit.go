package gosl

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type Kit interface {
	RunInTransaction(ctx context.Context, handler func(ctx context.Context) (context.Context, error)) error
	ContextSwitch(ctx context.Context, key interface{}) context.Context
	ContextReset(ctx context.Context) context.Context
}

func New(ctx context.Context) Kit {
	return &kit{}
}

type kit struct {
}

func (k *kit) RunInTransaction(ctx context.Context, handler func(ctx context.Context) (context.Context, error)) error {
	var err error
	trxs := make([]*sqlx.Tx, 0)
	keys := make([]interface{}, 0)
	cache := ctx.Value(CACHE_SQL_KEY)
	if cache != nil {
		for _, key := range cache.(map[interface{}]interface{}) {
			keys = append(keys, key)
		}
	} else {
		keys = append(keys, SQL_KEY)
	}
	if len(keys) == 0 {
		return errors.New("no active key present")
	}
	var ok bool
	var stacks []*sqlx.Tx
	stacks, ok = ctx.Value(SYSTEM_STACK).([]*sqlx.Tx)
	if !ok {
		stacks = make([]*sqlx.Tx, 0)
	}
	for _, key := range keys {
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
	stacks = append(stacks, trxs...)
	ctx = context.WithValue(ctx, SYSTEM_STACK, stacks)
	ctx, err = handler(ctx)
	if err != nil {
		for _, trx := range trxs {
			_ = trx.Rollback()
		}
		return err
	} else {
		stacks, ok = ctx.Value(SYSTEM_STACK).([]*sqlx.Tx)
		if !ok {
			stacks = make([]*sqlx.Tx, 0)
		}
		for _, trx := range stacks {
			err := trx.Commit()
			if err != nil {
				for _, itrx := range stacks {
					_ = itrx.Rollback()
				}
				return fmt.Errorf("error when committing transaction: %v", err)
			}
		}
	}
	return nil
}

func (k *kit) ContextSwitch(ctx context.Context, key interface{}) context.Context {
	curr := ctx.Value(key)
	if cacheKeys := ctx.Value(CACHE_SQL_KEY); cacheKeys == nil {
		keys := make(map[interface{}]interface{})
		keys[key] = curr
		ctx = context.WithValue(ctx, CACHE_SQL_KEY, keys)
	} else {
		keys := cacheKeys.(map[interface{}]interface{})
		duplicate := false
		for ckey, value := range keys {
			if ckey == key {
				duplicate = true
				curr = value
			}
		}
		if !duplicate {
			keys[key] = curr
		}
		ctx = context.WithValue(ctx, CACHE_SQL_KEY, keys)
	}
	return context.WithValue(ctx, SQL_KEY, curr)
}

func (k *kit) ContextReset(ctx context.Context) context.Context {
	if cacheKeys := ctx.Value(CACHE_SQL_KEY); cacheKeys == nil {
		return ctx
	} else {
		keys := cacheKeys.(map[interface{}]interface{})
		return context.WithValue(ctx, SQL_KEY, keys[SQL_KEY])
	}
}
