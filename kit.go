package gosl

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

type Kit interface {
	RunInTransaction(ctx context.Context, handler func(ctx context.Context) (context.Context, error)) (context.Context, error)
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

func (k *kit) RunInTransaction(ctx context.Context, handler func(ctx context.Context) (context.Context, error)) (context.Context, error) {
	var err error
	level := 1
	depth, ok := ctx.Value(SYSTEM_CALLBACK_DEPTH).(int)
	if ok && depth > 0 {
		level = depth + 1
	}
	ctx = context.WithValue(ctx, SYSTEM_CALLBACK_DEPTH, level)
	ctx, err = transact(ctx, level)
	if err != nil {
		return ctx, err
	}
	ctx, err = handler(ctx)
	stacks, ok := ctx.Value(SYSTEM_STACK).([]stack)
	if !ok {
		return ctx, errors.New("no active transaction")
	}
	if err != nil {
		for _, stck := range stacks {
			for _, tx := range stck.Transactions {
				_ = tx.Rollback()
			}
		}
		return ctx, err
	} else if level == 1 {
		for _, stck := range stacks {
			for _, tx := range stck.Transactions {
				err = tx.Commit()
				if err != nil {
					break
				}
			}
		}
		if err != nil {
			for _, stck := range stacks {
				for _, tx := range stck.Transactions {
					_ = tx.Rollback()
				}
			}
			return ctx, err
		}
	}
	return ctx, nil
}

func (k *kit) ContextSwitch(ctx context.Context, key any) (context.Context, error) {
	var err error
	ctx = context.WithValue(ctx, ACTIVE_SQL_KEY, key)
	var curr any
	if tmp, ok := ctx.Value(key).(*Queryable); ok {
		tmp.key = key
		curr = tmp
	} else if tmp, ok := ctx.Value(key).(Queryable); ok {
		tmp.key = key
		curr = tmp
	} else {
		curr = ctx.Value(key)
	}

	if cacheKeys := ctx.Value(CACHE_SQL_KEY); cacheKeys == nil {
		keys := make(map[any]any)
		keys["PRIMARY"] = ctx.Value(SQL_KEY)
		keys[key] = curr
		ctx = context.WithValue(ctx, CACHE_SQL_KEY, keys)
	} else {
		keys := cacheKeys.(map[any]any)
		duplicate := false
		for ckey := range keys {
			if ckey == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			keys[key] = curr
		}
		ctx = context.WithValue(ctx, CACHE_SQL_KEY, keys)
	}
	ctx = context.WithValue(ctx, SQL_KEY, curr)
	depth, ok := ctx.Value(SYSTEM_CALLBACK_DEPTH).(int)
	if ok && depth > 0 {
		ctx, err = transact(ctx, depth)
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

func (k *kit) ContextReset(ctx context.Context) (context.Context, error) {
	var err error
	depth, ok := ctx.Value(SYSTEM_CALLBACK_DEPTH).(int)
	if ok && depth > 0 {
		ctx, err = transact(ctx, depth)
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
			found := -1
			for i := 0; ok && i < len(stacks); i++ {
				if stacks[i].Level == level {
					found = i
					break
				}
			}
			if found != -1 {
				stacks[found].Transactions = append(stacks[found].Transactions, tx)
			} else {
				if !ok {
					stacks = make([]stack, 0)
				}
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
