package gosl

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type Kit interface {
	RunInTransaction(ctx context.Context, handler func(ctx context.Context) (context.Context, error)) error
	ContextSwitch(ctx context.Context, key interface{}) (context.Context, error)
	ContextReset(ctx context.Context) (context.Context, error)
}

func New(ctx context.Context) Kit {
	return &kit{}
}

type stack struct {
	Level        int
	Transactions []*sqlx.Tx
}
type kit struct {
}

func (k *kit) RunInTransaction(ctx context.Context, handler func(ctx context.Context) (context.Context, error)) error {
	var err error
	level := 1
	stacks, ok := ctx.Value(SYSTEM_STACK).([]stack)
	if ok && len(stacks) > 0 {
		level = stacks[len(stacks)-1].Level + 1
	}
	ctx, err = transact(ctx, level)
	if err != nil {
		return err
	}
	ctx, err = handler(ctx)
	stacks, ok = ctx.Value(SYSTEM_STACK).([]stack)
	if !ok {
		return errors.New("no active transaction")
	}
	if err != nil {
		for _, stck := range stacks {
			for _, tx := range stck.Transactions {
				_ = tx.Rollback()
			}
		}
		return err
	} else if level == 1 {
		for _, stck := range stacks {
			for _, tx := range stck.Transactions {
				err := tx.Commit()
				if err != nil {
					for _, istck := range stacks {
						for _, itx := range istck.Transactions {
							_ = itx.Rollback()
						}
					}
					return fmt.Errorf("error when committing transaction: %v", err)
				}
			}

		}
	}
	return nil
}

func (k *kit) ContextSwitch(ctx context.Context, key any) (context.Context, error) {
	var err error
	ctx = context.WithValue(ctx, ACTIVE_SQL_KEY, key)
	curr := ctx.Value(key)
	if cacheKeys := ctx.Value(CACHE_SQL_KEY); cacheKeys == nil {
		keys := make(map[any]any)
		keys["PRIMARY"] = ctx.Value(SQL_KEY)
		keys[key] = curr
		stacks, ok := ctx.Value(SYSTEM_STACK).([]stack)
		if ok && len(stacks) > 0 {
			ctx, err = transact(ctx, stacks[len(stacks)-1].Level)
			if err != nil {
				return ctx, err
			}
		}
		ctx = context.WithValue(ctx, CACHE_SQL_KEY, keys)
	} else {
		keys := cacheKeys.(map[any]any)
		duplicate := false
		for ckey, value := range keys {
			if ckey == key {
				duplicate = true
				curr = value
			}
		}
		if !duplicate {
			keys[key] = curr
			stacks, ok := ctx.Value(SYSTEM_STACK).([]stack)
			if ok && len(stacks) > 0 {
				ctx, err = transact(ctx, stacks[len(stacks)-1].Level)
				if err != nil {
					return ctx, err
				}
			}
		}
		ctx = context.WithValue(ctx, CACHE_SQL_KEY, keys)
	}
	return context.WithValue(ctx, SQL_KEY, curr), nil
}

func (k *kit) ContextReset(ctx context.Context) (context.Context, error) {
	var err error
	stacks, ok := ctx.Value(SYSTEM_STACK).([]stack)
	if ok && len(stacks) > 0 {
		ctx, err = transact(ctx, stacks[len(stacks)-1].Level)
		if err != nil {
			return ctx, err
		}
	}
	ctx = context.WithValue(ctx, ACTIVE_SQL_KEY, SQL_KEY)
	if cacheKeys := ctx.Value(CACHE_SQL_KEY); cacheKeys == nil {
		return ctx, nil
	} else {
		keys := cacheKeys.(map[any]any)
		if keys["PRIMARY"] != nil {
			return context.WithValue(ctx, SQL_KEY, keys["PRIMARY"]), nil
		} else {
			return context.WithValue(ctx, SQL_KEY, keys[SQL_KEY]), nil
		}

	}
}

func transact(ctx context.Context, level int) (context.Context, error) {
	if queryable, ok := ctx.Value(SQL_KEY).(*Queryable); ok {
		if queryable.tx == nil {
			var tx *sqlx.Tx
			tx, err := queryable.db.Beginx()
			if err != nil {
				return ctx, err
			}
			con := make(map[string]any)
			con["db"] = queryable.db
			con["tx"] = tx
			ctx = context.WithValue(ctx, SQL_KEY, NewQueryable(con))
			stacks, ok := ctx.Value(SYSTEM_STACK).([]stack)
			if ok && len(stacks) > level-1 && stacks[level-1].Level == level-1 {
				stacks[level-1].Transactions = append(stacks[level-1].Transactions, tx)
			} else {
				stacks = make([]stack, 0)
				stacks = append(stacks, stack{
					Level:        level,
					Transactions: []*sqlx.Tx{tx},
				})
			}
			ctx = context.WithValue(ctx, SYSTEM_STACK, stacks)
			return ctx, nil
		} else {
			return ctx, nil
		}
	}
	return ctx, errors.New("key is not active")
}
