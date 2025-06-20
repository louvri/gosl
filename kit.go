package gosl

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

type Kit interface {
	RunInTransaction(ctx context.Context, handler func(ctx context.Context) error) error
	ContextSwitch(ctx context.Context, key interface{}) error
	ContextReset(ctx context.Context) error
}

func New(ctx context.Context) (context.Context, Kit) {
	if _, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext); ok {
		return ctx, &kit{}
	} else {
		base := Hijack(ctx)
		ctx = context.WithValue(ctx, INTERNAL_CONTEXT, base)
		return ctx, &kit{}
	}

}

type stack struct {
	Level        int
	Transactions []*sqlx.Tx
}
type kit struct {
}

func (k *kit) RunInTransaction(ctx context.Context, handler func(ctx context.Context) error) error {
	var err error
	level := 1
	//inject
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok {
		return errors.New("failed_to_instantiate")
	}
	var depth int
	ref, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(*int)
	if ok && ref != nil {
		depth = *ref
		if depth > 0 {
			level = depth + 1
		}
	}
	ctx, err = transact(ctx, level)
	if err != nil {
		return err
	}
	err = handler(ctx)
	// re-inject
	_ctx, ok = ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok {
		_ctx = Hijack(ctx)
	}
	stacks, ok := _ctx.Get(SYSTEM_STACK).([]stack)
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
			return err
		}
	}
	return nil
}

func (k *kit) ContextSwitch(ctx context.Context, key any) error {
	var err error
	var curr *Queryable
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok {
		return errors.New("failed_to_instantiate")
	}
	_ctx.Set(CURRENT_SQL_KEY, key)
	cacheKeys := _ctx.Get(CACHE_SQL_KEY)
	var keys map[any]any
	if cacheKeys == nil {
		keys = make(map[any]any)
	} else {
		keys = cacheKeys.(map[any]any)
	}

	if tmp, ok := keys[key].(*Queryable); ok {
		curr = tmp
	} else {
		if tmp, ok := _ctx.Get(key).(*Queryable); ok {
			curr = tmp
		} else {
			return errors.New("not found")
		}
	}
	// curr.key = key

	_ctx.Set(SQL_KEY, curr)
	var depth int
	ref, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(*int)
	if ok && ref != nil {
		depth = *ref
		if depth > 0 {
			ctx, err = transact(ctx, depth)
			if err != nil {
				return err
			}
			// re-inject
			_ctx, ok = ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
			if !ok {
				_ctx = Hijack(ctx)
			}
		}
		if curr, ok := _ctx.Get(SQL_KEY).(*Queryable); ok {
			if keys[key] != curr {
				keys[key] = curr
				_ctx.Set(CACHE_SQL_KEY, keys)
			}

		}
	}
	return nil
}

func (k *kit) ContextReset(ctx context.Context) error {
	var err error
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)

	if !ok {
		return errors.New("failed_to_instantiate")
	}
	_ctx.Set(CURRENT_SQL_KEY, SQL_KEY)
	_ctx.Set(SQL_KEY, _ctx.Get(PRIMARY_SQL_KEY))
	var depth int
	ref, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(*int)
	if ok && ref != nil {
		depth = *ref
		if depth > 0 {
			_, err = transact(ctx, depth)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func transact(ctx context.Context, level int) (context.Context, error) {
	var ok bool
	var _ctx *InternalContext
	var queryable *Queryable

	if _ctx, ok = ctx.Value(INTERNAL_CONTEXT).(*InternalContext); ok {
		if queryable, ok = _ctx.Get(SQL_KEY).(*Queryable); !ok {
			return ctx, errors.New("key is not active")
		}
	}

	if queryable.tx == nil {
		var tx *sqlx.Tx
		tx, err := queryable.db.Beginx()
		if err != nil {
			return ctx, err
		}
		con := make(map[string]any)
		con["db"] = queryable.db
		con["tx"] = tx
		newQueryable := NewQueryable(con)
		_ctx.Set(SQL_KEY, newQueryable)

		if exists := _ctx.Get(PRIMARY_SQL_KEY); exists == nil {
			_ctx.Set(PRIMARY_SQL_KEY, newQueryable)
		}

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
		_ctx.Set(SYSTEM_CALLBACK_DEPTH, &level)
		return context.WithValue(context.Background(), INTERNAL_CONTEXT, _ctx), nil

	} else {
		return ctx, nil
	}

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
