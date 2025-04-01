package gosl

import "context"

type InternalContext struct {
	base       context.Context
	properites map[Gosl_Key]any
}

func Hijack(ctx context.Context) *InternalContext {
	return &InternalContext{
		base: ctx,
		properites: map[Gosl_Key]any{
			SQL_KEY:               ctx.Value(SQL_KEY),
			CACHE_SQL_KEY:         ctx.Value(CACHE_SQL_KEY),
			CURRENT_SQL_KEY:       ctx.Value(CURRENT_SQL_KEY),
			PRIMARY_SQL_KEY:       ctx.Value(PRIMARY_SQL_KEY),
			SYSTEM_STACK:          ctx.Value(SYSTEM_STACK),
			SYSTEM_CALLBACK_DEPTH: ctx.Value(SYSTEM_CALLBACK_DEPTH),
		},
	}
}
func (i *InternalContext) Base() context.Context {
	return i.base
}

func (i *InternalContext) Get(key any) any {
	switch key {
	case SQL_KEY:
		return i.properites[SQL_KEY]
	case CACHE_SQL_KEY:
		return i.properites[CACHE_SQL_KEY]
	case CURRENT_SQL_KEY:
		return i.properites[CURRENT_SQL_KEY]
	case PRIMARY_SQL_KEY:
		return i.properites[PRIMARY_SQL_KEY]
	case SYSTEM_STACK:
		return i.properites[SYSTEM_STACK]
	case SYSTEM_CALLBACK_DEPTH:
		return i.properites[SYSTEM_CALLBACK_DEPTH]
	}
	return i.base.Value(key)
}

func (i *InternalContext) Set(key, value any) {
	switch key {
	case SQL_KEY:
		i.properites[SQL_KEY] = value
	case CACHE_SQL_KEY:
		i.properites[CACHE_SQL_KEY] = value
	case CURRENT_SQL_KEY:
		i.properites[CURRENT_SQL_KEY] = value
	case PRIMARY_SQL_KEY:
		i.properites[PRIMARY_SQL_KEY] = value
	case SYSTEM_STACK:
		i.properites[SYSTEM_STACK] = value
	case SYSTEM_CALLBACK_DEPTH:
		i.properites[SYSTEM_CALLBACK_DEPTH] = value
	}
}
