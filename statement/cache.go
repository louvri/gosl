package statement

import (
	"time"

	"github.com/jmoiron/sqlx"
)

type Cache struct {
	stmt       *sqlx.Stmt
	inUse      bool
	timestamp  time.Time
	allowEvict bool
}

func (c *Cache) Set(stmt *sqlx.Stmt, allowEvict, inUse bool) {
	c.stmt = stmt
	c.timestamp = time.Now().UTC()
	c.allowEvict = allowEvict
	c.inUse = inUse
}

func (c *Cache) Close() error {
	if c.allowEvict {
		return c.stmt.Close()
	}
	return nil
}

func (c *Cache) Statement() *sqlx.Stmt {
	return c.stmt
}

func (c *Cache) InUse() bool {
	return c.inUse
}
