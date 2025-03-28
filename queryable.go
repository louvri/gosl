package gosl

import (
	"context"
	_sql "database/sql"

	"github.com/jmoiron/sqlx"
)

type Queryable struct {
	db  *sqlx.DB
	tx  *sqlx.Tx
	key interface{}
}

func NewQueryable(db interface{}, keys ...any) *Queryable {
	var key any
	if len(keys) > 0 && keys[0] != nil {
		key = keys[0]
	}
	tmp, ok := db.(map[string]interface{})
	if ok {
		db, ok := tmp["db"].(*sqlx.DB)
		if !ok {
			return &Queryable{}
		}
		tx, ok := tmp["tx"].(*sqlx.Tx)
		if !ok {
			return &Queryable{}
		}
		return &Queryable{
			db:  db,
			tx:  tx,
			key: key,
		}
	}
	return &Queryable{
		db:  db.(*sqlx.DB),
		key: key,
	}
}

// DB return db
func (qtx *Queryable) DB() *sqlx.DB {
	return qtx.db
}

func (qtx *Queryable) Key() interface{} {
	return qtx.key
}

// BindNamed ...
func (qtx *Queryable) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	if qtx.tx != nil {
		return qtx.tx.BindNamed(query, arg)
	}
	return qtx.db.BindNamed(query, arg)
}

// DriverName ...
func (qtx *Queryable) DriverName() string {
	if qtx.tx != nil {
		return qtx.tx.DriverName()
	}
	return qtx.db.DriverName()
}

// Get ...
func (qtx *Queryable) Get(dest interface{}, query string, args ...interface{}) error {
	if qtx.tx != nil {
		return qtx.tx.Get(dest, query, args...)
	}
	return qtx.db.Get(dest, query, args...)
}

// GetContext ...
func (qtx *Queryable) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if qtx.tx != nil {
		return qtx.tx.GetContext(ctx, dest, query, args...)
	}
	return qtx.db.GetContext(ctx, dest, query, args...)
}

// Exec ...
func (qtx *Queryable) Exec(query string, args ...interface{}) (_sql.Result, error) {
	if qtx.tx != nil {
		return qtx.tx.Exec(query, args...)
	}
	return qtx.db.Exec(query, args...)
}

// ExecContext ...
func (qtx *Queryable) ExecContext(ctx context.Context, query string, args ...interface{}) (_sql.Result, error) {
	if qtx.tx != nil {
		return qtx.tx.ExecContext(ctx, query, args...)
	}
	return qtx.db.ExecContext(ctx, query, args...)
}

// MustExec ...
func (qtx *Queryable) MustExec(query string, args ...interface{}) _sql.Result {
	if qtx.tx != nil {
		return qtx.tx.MustExec(query, args...)
	}
	return qtx.db.MustExec(query, args...)
}

// MustExecContext ...
func (qtx *Queryable) MustExecContext(ctx context.Context, query string, args ...interface{}) _sql.Result {
	if qtx.tx != nil {
		return qtx.tx.MustExecContext(ctx, query, args...)
	}
	return qtx.db.MustExecContext(ctx, query, args...)
}

// NamedExec ...
func (qtx *Queryable) NamedExec(query string, arg interface{}) (_sql.Result, error) {
	if qtx.tx != nil {
		return qtx.tx.NamedExec(query, arg)
	}
	return qtx.db.NamedExec(query, arg)
}

// NamedExecContext ...
func (qtx *Queryable) NamedExecContext(ctx context.Context, query string, arg interface{}) (_sql.Result, error) {
	if qtx.tx != nil {
		return qtx.tx.NamedExecContext(ctx, query, arg)
	}
	return qtx.db.NamedExecContext(ctx, query, arg)
}

// NamedQuery ...
func (qtx *Queryable) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	if qtx.tx != nil {
		return qtx.tx.NamedQuery(query, arg)
	}
	return qtx.db.NamedQuery(query, arg)
}

// PrepareNamed ...
func (qtx *Queryable) PrepareNamed(query string, withTransaction bool) (*sqlx.NamedStmt, error) {
	if withTransaction && qtx.tx != nil {
		return qtx.tx.PrepareNamed(query)
	}
	return qtx.db.PrepareNamed(query)
}

// PrepareNamedContext ...
func (qtx *Queryable) PrepareNamedContext(ctx context.Context, query string, withTransaction bool) (*sqlx.NamedStmt, error) {
	if withTransaction && qtx.tx != nil {
		return qtx.tx.PrepareNamedContext(ctx, query)
	}
	return qtx.db.PrepareNamedContext(ctx, query)
}

// Preparex ...
func (qtx *Queryable) Preparex(query string, withTransaction bool) (*sqlx.Stmt, error) {
	if withTransaction && qtx.tx != nil {
		return qtx.tx.Preparex(query)
	}
	return qtx.db.Preparex(query)
}

// PreparexContext ...
func (qtx *Queryable) PreparexContext(ctx context.Context, query string, withTransaction bool) (*sqlx.Stmt, error) {
	if withTransaction && qtx.tx != nil {
		return qtx.tx.PreparexContext(ctx, query)
	}
	return qtx.db.PreparexContext(ctx, query)
}

// QueryRowx ...
func (qtx *Queryable) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	if qtx.tx != nil {
		return qtx.tx.QueryRowx(query, args...)
	}
	return qtx.db.QueryRowx(query, args...)
}

// QueryRowxContext ...
func (qtx *Queryable) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	if qtx.tx != nil {
		return qtx.tx.QueryRowxContext(ctx, query, args...)
	}
	return qtx.db.QueryRowxContext(ctx, query, args...)
}

// Queryx ...
func (qtx *Queryable) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	if qtx.tx != nil {
		return qtx.tx.Queryx(query, args...)
	}
	return qtx.db.Queryx(query, args...)
}

// QueryxContext ...
func (qtx *Queryable) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	if qtx.tx != nil {
		return qtx.tx.QueryxContext(ctx, query, args...)
	}
	return qtx.db.QueryxContext(ctx, query, args...)
}

// Rebind ...
func (qtx *Queryable) Rebind(query string) string {
	if qtx.tx != nil {
		return qtx.tx.Rebind(query)
	}
	return qtx.db.Rebind(query)
}

// Select ...
func (qtx *Queryable) Select(dest interface{}, query string, args ...interface{}) error {
	if qtx.tx != nil {
		return qtx.tx.Select(dest, query, args...)
	}
	return qtx.db.Select(dest, query, args...)
}

// SelectContext ...
func (qtx *Queryable) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if qtx.tx != nil {
		return qtx.tx.SelectContext(ctx, dest, query, args...)
	}
	return qtx.db.SelectContext(ctx, dest, query, args...)
}

// NamedQueryRowx run BindNamed then QueryRowx
func (qtx *Queryable) NamedQueryRowx(query string, args interface{}) (*sqlx.Row, error) {
	if qtx.tx != nil {
		query, args2, err := qtx.tx.BindNamed(query, args)
		if err != nil {
			return nil, err
		}

		row := qtx.tx.QueryRowx(query, args2...)
		return row, nil
	}
	query, args2, err := qtx.db.BindNamed(query, args)

	if err != nil {
		return nil, err
	}

	row := qtx.db.QueryRowx(query, args2...)
	return row, nil

}

// NamedQueryRowxContext run BindNamed then QueryRowxContext
func (qtx *Queryable) NamedQueryRowxContext(ctx context.Context, query string, args interface{}) (*sqlx.Row, error) {
	if qtx.tx != nil {
		query, args2, err := qtx.tx.BindNamed(query, args)

		if err != nil {
			return nil, err
		}

		row := qtx.tx.QueryRowxContext(ctx, query, args2...)
		return row, nil
	}
	query, args2, err := qtx.db.BindNamed(query, args)

	if err != nil {
		return nil, err
	}

	row := qtx.db.QueryRowxContext(ctx, query, args2...)
	return row, nil
}

func (qtx *Queryable) Stmtx(stmt interface{}) *sqlx.Stmt {
	if qtx.tx == nil {
		return nil
	} else {
		return qtx.tx.Stmtx(stmt)
	}
}

func (qtx *Queryable) StmtxContext(ctx context.Context, stmt interface{}) *sqlx.Stmt {
	if qtx.tx == nil {
		return nil
	} else {
		return qtx.tx.StmtxContext(ctx, stmt)
	}
}
