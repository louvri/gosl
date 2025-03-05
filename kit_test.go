package gosl_test

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/louvri/gosl"
)

type TestKey int

var TKey TestKey = 13

func TestRunInTransaction(t *testing.T) {
	ctx := context.WithValue(context.Background(),
		gosl.SQL_KEY,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"test_1",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	kit := gosl.New(ctx)
	queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	_, err := queryable.ExecContext(ctx, "DELETE FROM `hello`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = queryable.ExecContext(ctx, "DELETE FROM `hello_2`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	ctx, err = kit.ContextReset(ctx)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) (context.Context, error) {
			queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
			_, err := queryable.ExecContext(ctx, "INSERT INTO `hello` VALUES('tiga')")
			if err != nil {
				return ctx, err
			}
			_, err = queryable.ExecContext(ctx, "INSERT INTO `hello_2` VALUES('empat')")
			if err != nil {
				return ctx, err
			}
			return ctx, nil
		},
	)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
}

func TestConsecutiveRunInTransaction(t *testing.T) {
	ctx := context.WithValue(context.Background(),
		gosl.SQL_KEY,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"test_1",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	kit := gosl.New(ctx)
	queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	_, err := queryable.ExecContext(ctx, "DELETE FROM `hello`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = queryable.ExecContext(ctx, "DELETE FROM `hello_2`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) (context.Context, error) {
			queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
			_, err := queryable.ExecContext(ctx, "INSERT INTO `hello` VALUES('tiga')")
			if err != nil {
				return ctx, err
			}
			_, err = queryable.ExecContext(ctx, "INSERT INTO `hello_2` VALUES('empat')")
			if err != nil {
				return ctx, err
			}
			return ctx, nil
		},
	)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) (context.Context, error) {
			queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
			_, err := queryable.ExecContext(ctx, "INSERT INTO `hello` VALUES('tiga')")
			if err != nil {
				return ctx, err
			}
			_, err = queryable.ExecContext(ctx, "INSERT INTO `hello_2` VALUES('empat')")
			if err != nil {
				return ctx, err
			}
			return ctx, nil
		},
	)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
}

func TestRunInTransactionWithSwitchContext(t *testing.T) {
	ctx := context.WithValue(context.Background(),
		gosl.SQL_KEY,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"test_1",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	ctx = context.WithValue(ctx,
		TKey,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"test_2",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	kit := gosl.New(ctx)
	queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	_, err := queryable.ExecContext(ctx, "DELETE FROM `hello`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	if ctx, err := kit.ContextSwitch(ctx, TKey); err == nil {
		if queryable, ok := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable); ok {
			_, err = queryable.ExecContext(ctx, "DELETE FROM `world`")
			if err != nil {
				log.Fatal(err.Error())
				t.Fail()
			}
			ctx, err = kit.ContextReset(ctx)
			if err != nil {
				log.Fatal(err.Error())
				t.Fail()
			}
			_, err = kit.RunInTransaction(
				ctx,
				func(ctx context.Context) (context.Context, error) {
					queryable, ok = ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
					if !ok {
						t.Fatal("failed to get queryable")
					}
					_, err := queryable.ExecContext(ctx, "INSERT INTO `hello` VALUES('sepuluh')")
					if err != nil {
						return ctx, err
					}
					if ctx, err = kit.ContextSwitch(ctx, TKey); err == nil {
						queryable, ok = ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
						if !ok {
							t.Fatal("failed to get queryable")
						}
					}
					_, err = queryable.ExecContext(ctx, "INSERT INTO `world` VALUES('empat')")
					if err != nil {
						return ctx, err
					}
					return ctx, nil
				},
			)
			if err != nil {
				log.Fatal(err.Error())
				t.Fail()
			}
		}
	}

}

func TestNestedRunInTransaction(t *testing.T) {
	ctx := context.WithValue(context.Background(),
		gosl.SQL_KEY,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"test_1",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	ctx = context.WithValue(ctx,
		TKey,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"test_2",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	kit := gosl.New(ctx)
	queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	_, err := queryable.ExecContext(ctx, "DELETE FROM `hello`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = queryable.ExecContext(ctx, "DELETE FROM `hello_2`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	ctx, err = kit.ContextReset(ctx)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) (context.Context, error) {
			queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
			_, err := queryable.ExecContext(ctx, "INSERT INTO `hello` VALUES('tigabelas')")
			if err != nil {
				return ctx, err
			}
			_, err = queryable.ExecContext(ctx, "INSERT INTO `hello_2` VALUES('empatbelas')")
			if err != nil {
				return ctx, err
			}
			ctx, err = kit.RunInTransaction(
				ctx,
				func(ctx context.Context) (context.Context, error) {
					queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
					_, err := queryable.ExecContext(ctx, "INSERT INTO `hello` VALUES('tigabelasbelas')")
					if err != nil {
						return ctx, err
					}
					var ok bool
					if ctx, err = kit.ContextSwitch(ctx, TKey); err == nil {
						queryable, ok = ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
						if !ok {
							t.Fatal("failed to get queryable")
						}
					}
					_, err = queryable.ExecContext(ctx, "INSERT INTO `world` VALUES('empat')")
					if err != nil {
						return ctx, err
					}
					return ctx, nil
				})
			return ctx, err
		},
	)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
}

func TestNestedRunInTransactionWithFailAtTheEnd(t *testing.T) {
	ctx := context.WithValue(context.Background(),
		gosl.SQL_KEY,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"test_1",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	kit := gosl.New(ctx)
	queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	_, err := queryable.ExecContext(ctx, "DELETE FROM `hello`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = queryable.ExecContext(ctx, "DELETE FROM `hello_2`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	ctx, err = kit.ContextReset(ctx)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	_, err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) (context.Context, error) {
			queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
			_, err := queryable.ExecContext(ctx, "INSERT INTO `hello` VALUES('satutigabelas')")
			if err != nil {
				return ctx, err
			}
			_, err = queryable.ExecContext(ctx, "INSERT INTO `hello_2` VALUES('satuempatbelas')")
			if err != nil {
				return ctx, err
			}

			ctx, err = kit.RunInTransaction(
				ctx,
				func(ctx context.Context) (context.Context, error) {
					queryable := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
					_, err := queryable.ExecContext(ctx, "INSERT INTO `hello` VALUES('satutigabelasbelas')")
					if err != nil {
						return ctx, err
					}
					return ctx, errors.New("fail deliberately")
				},
			)
			return ctx, err
		},
	)
	if err == nil {
		log.Fatal("should failed but not")
		t.Fail()
	}
}
