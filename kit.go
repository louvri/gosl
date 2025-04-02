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
	//inject
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok {
		_ctx = Hijack(ctx)
	}

	depth, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(int)
	if ok && depth > 0 {
		level = depth + 1
	}
	ctx, err = transact(ctx, level)
	if err != nil {
		return ctx, err
	}
	ctx, err = handler(ctx)
	// re-inject
	_ctx, ok = ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok {
		_ctx = Hijack(ctx)
	}
	stacks, ok := _ctx.Get(SYSTEM_STACK).([]stack)
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
	var curr *Queryable
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok {
		_ctx = Hijack(ctx)
	}
	_ctx.Set(CURRENT_SQL_KEY, key)
	if tmp, ok := _ctx.Get(key).(*Queryable); ok {
		tmp.key = key
		curr = tmp
	} else if tmp, ok := _ctx.Get(key).(Queryable); ok {
		curr = &tmp
	}
	curr.key = key
	if cacheKeys := _ctx.Get(CACHE_SQL_KEY); cacheKeys == nil {
		keys := make(map[any]any)
		if q, ok := ctx.Value(SQL_KEY).(*Queryable); !ok {
			if q, ok = _ctx.Base().Value(SQL_KEY).(*Queryable); ok {
				_ctx.Set(PRIMARY_SQL_KEY, q)
			}
		} else {
			_ctx.Set(PRIMARY_SQL_KEY, q)
		}
		keys[key] = curr
		_ctx.Set(CACHE_SQL_KEY, keys)
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
		_ctx.Set(CACHE_SQL_KEY, keys)
	}
	_ctx.Set(SQL_KEY, curr)
	depth, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(int)
	if ok && depth > 0 {
		ctx, err = transact(ctx, depth)
		if err != nil {
			return ctx, err
		}
		// re-inject
		_ctx, ok = ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
		if !ok {
			_ctx = Hijack(ctx)
		}
	}
	return context.WithValue(context.Background(), INTERNAL_CONTEXT, _ctx), nil
}

func (k *kit) ContextReset(ctx context.Context) (context.Context, error) {
	var err error
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok {
		_ctx = Hijack(ctx)
	}
	depth, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(int)
	if ok && depth > 0 {
		ctx, err = transact(ctx, depth)
		if err != nil {
			return ctx, err
		}
	}
	_ctx.Set(CURRENT_SQL_KEY, SQL_KEY)
	_ctx.Set(SQL_KEY, _ctx.Get(PRIMARY_SQL_KEY))
	return context.WithValue(context.Background(), INTERNAL_CONTEXT, _ctx), nil
}

func transact(ctx context.Context, level int) (context.Context, error) {
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok {
		_ctx = Hijack(ctx)
	}
	if queryable, ok := _ctx.Get(SQL_KEY).(*Queryable); ok {
		if queryable.tx == nil {
			var tx *sqlx.Tx
			tx, err := queryable.db.Beginx()
			if err != nil {
				return ctx, err
			}
			con := make(map[string]any)
			con["db"] = queryable.db
			con["tx"] = tx
			_ctx.Set(SQL_KEY, NewQueryable(con))
			stacks, ok := _ctx.Get(SYSTEM_STACK).([]stack)
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
			_ctx.Set(SYSTEM_STACK, stacks)
			_ctx.Set(SYSTEM_CALLBACK_DEPTH, level)
			return context.WithValue(context.Background(), INTERNAL_CONTEXT, _ctx), nil
		} else {
			return ctx, nil
		}
	}
	return ctx, errors.New("key is not active")
}

func QueryableFromContext(ctx context.Context) *Queryable {
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	var q *Queryable
	if ok {
		q, ok = _ctx.Get(SQL_KEY).(*Queryable)

	} else {
		q, ok = ctx.Value(SQL_KEY).(*Queryable)
	}
	if !ok {
		return nil
	}
	return q
}
