package statement_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/louvri/gosl/statement"
	"github.com/stretchr/testify/assert"
)

func TestStatementBuildAndGet(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := WrapDB()

	query := "SELECT * FROM users WHERE id = ?"
	mock.ExpectPrepare(query)

	stmtCache := statement.New(10, 5*time.Minute)

	// Build a new statement
	cache, err := stmtCache.Build("user_query", query, sqlxDB, true, false)
	assert.NoError(t, err)
	assert.NotNil(t, cache)
	assert.NotNil(t, cache.Statement())

	// Get should return same cache
	retrieved, err := stmtCache.Mount("user_query")
	assert.NoError(t, err)
	assert.Equal(t, cache, retrieved)
}

func TestSetInUseFlag(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	sqlxDB := WrapDB()
	query := "SELECT * FROM users"
	mock.ExpectPrepare(query)

	stmtCache := statement.New(10, 1*time.Minute)
	cache, err := stmtCache.Build("key", query, sqlxDB, true, false)
	assert.NoError(t, err)

	assert.False(t, cache.InUse())

	stmtCache.Set("key", true)

	updated, _ := stmtCache.Get("key")
	assert.True(t, updated.InUse())
}

func TestEvictionClosesStatement(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	sqlxDB := WrapDB()

	stmtCache := statement.New(1, 100*time.Millisecond) // set low size and short TTL

	query1 := "SELECT * FROM one"
	query2 := "SELECT * FROM two"

	mock.ExpectPrepare(query1)
	c1, err := stmtCache.Build("q1", query1, sqlxDB, true, false)
	assert.NoError(t, err)
	assert.NotNil(t, c1)

	mock.ExpectPrepare(query2)
	_, err = stmtCache.Build("q2", query2, sqlxDB, true, false) // should evict q1
	assert.NoError(t, err)

	// Wait for eviction to occur
	time.Sleep(200 * time.Millisecond)

	// q1 should be gone
	_, err = stmtCache.Mount("q1")
	assert.Error(t, err)
}

func TestEvictionSkipWhenInUse(t *testing.T) {
	// db, _, _ := sqlmock.New()
	// defer db.Close()

	sqlxDB := WrapDB()
	defer sqlxDB.Close()
	stmtCache := statement.New(10, 100*time.Millisecond)
	query1 := "SELECT * FROM one"
	query2 := "SELECT * FROM two"

	// mock.ExpectPrepare(query1)
	_, err := stmtCache.Build("q1", query1, sqlxDB, true, true) // inUse = true
	assert.NoError(t, err)

	// mock.ExpectPrepare(query2)
	_, err = stmtCache.Build("q2", query2, sqlxDB, true, false) // attempt to evict q1
	assert.NoError(t, err)

	before := time.Now()
	// Wait to allow possible eviction
	time.Sleep(200 * time.Millisecond)

	// q1 should still exist
	var wg sync.WaitGroup
	for i := 0; i < 1-000-000; i++ {
		wg.Add(1)
		go func(counter int) {
			defer wg.Done()
			_, err = stmtCache.Mount("q1")
			if err != nil {
				panic(err.Error())
			}
		}(i)
	}
	wg.Wait()
	fmt.Println("execution time: ", time.Since(before))
	fmt.Println("done")
}

func WrapDB() *sqlx.DB {
	db, err := sqlx.Connect("mysql", fmt.Sprintf(
		"%s:%s@(%s:%s)/%s",
		"root",
		"abcd",
		"localhost",
		"3306",
		"stmtTest"))
	if err != nil {
		panic(err)
	}
	return db
}
