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
	base := SetBase(ctx)
	if exists := ctx.Value(INTERNAL_CONTEXT); exists != nil {
		if internal, ok := exists.(*InternalContext); !ok || internal == nil {
			ctx = context.WithValue(ctx, INTERNAL_CONTEXT, base)
		}
	} else {
		exists = base
		ctx = context.WithValue(ctx, INTERNAL_CONTEXT, exists)
	}
	return ctx, &kit{}
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
	if !ok || _ctx.Base() == nil || _ctx.IsPropertiesNill() {
		_ctx = Hijack(ctx)
	}

	var depth int
	ref, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(*int)
	if ok && ref != nil {
		depth = *ref
		if depth > 0 {
			level = depth + 1
		}
	}
	err = transact(ctx, level)
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
	if !ok || _ctx.Base() == nil || _ctx.IsPropertiesNill() {
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
	var depth int
	ref, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(*int)
	if ok && ref != nil {
		depth = *ref
		if depth > 0 {
			err = transact(ctx, depth)
			if err != nil {
				return err
			}
			// re-inject
			_ctx, ok = ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
			if !ok {
				_ctx = Hijack(ctx)
			}
		}
	}
	if exist := ctx.Value(INTERNAL_CONTEXT); exist != nil {
		if tmp, ok := exist.(*InternalContext); ok {
			tmp = _ctx
			_ = context.WithValue(ctx, INTERNAL_CONTEXT, tmp)
		}
	}
	return nil
}

func (k *kit) ContextReset(ctx context.Context) error {
	var err error
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok || _ctx.Base() == nil || _ctx.IsPropertiesNill() {
		_ctx = Hijack(ctx)
	}
	var depth int
	ref, ok := _ctx.Get(SYSTEM_CALLBACK_DEPTH).(*int)
	if ok && ref != nil {
		depth = *ref
		if depth > 0 {
			err = transact(ctx, depth)
			if err != nil {
				return err
			}
		}
	}
	_ctx.Set(CURRENT_SQL_KEY, SQL_KEY)
	_ctx.Set(SQL_KEY, _ctx.Get(PRIMARY_SQL_KEY))
	if exist := ctx.Value(INTERNAL_CONTEXT); exist != nil {
		if tmp, ok := exist.(*InternalContext); ok {
			tmp = _ctx
			_ = context.WithValue(ctx, INTERNAL_CONTEXT, tmp)
		}
	}
	return nil
}

func transact(ctx context.Context, level int) error {
	_ctx, ok := ctx.Value(INTERNAL_CONTEXT).(*InternalContext)
	if !ok || _ctx.Base() == nil || _ctx.IsPropertiesNill() {
		_ctx = Hijack(ctx)
	}
	if queryable, ok := _ctx.Get(SQL_KEY).(*Queryable); ok {
		if queryable.tx == nil {
			var tx *sqlx.Tx
			tx, err := queryable.db.Beginx()
			if err != nil {
				return err
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
			_ctx.Set(SYSTEM_CALLBACK_DEPTH, &level)
			if exist := ctx.Value(INTERNAL_CONTEXT); exist != nil {
				if tmp, ok := exist.(*InternalContext); ok {
					tmp = _ctx
					_ = context.WithValue(ctx, INTERNAL_CONTEXT, tmp)
				}
			}
			return nil
		} else {
			return nil
		}
	}
	return errors.New("key is not active")
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
