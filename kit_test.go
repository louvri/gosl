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

func TestContextSwitch(t *testing.T) {
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
	_, err := kit.ContextSwitch(ctx, TKey)
	if err != nil {
		t.Fatal(err)
	}
	_, err = kit.ContextReset(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

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
	err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) error {
			var queryable *gosl.Queryable
			ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
			if ok {
				queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			} else {
				t.Fatal("sql not initiated")
			}
			_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello` VALUES('tiga')")
			if err != nil {
				return err
			}
			_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_2` VALUES('empat')")
			if err != nil {
				return err
			}
			return nil
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
	err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) error {
			var queryable *gosl.Queryable
			ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
			if ok {
				queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			} else {
				t.Fatal("sql not initiated")
			}
			_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello` VALUES('tiga')")
			if err != nil {
				return err
			}
			_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_2` VALUES('empat')")
			if err != nil {
				return err
			}
			return nil
		},
	)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) error {
			var queryable *gosl.Queryable
			ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
			if ok {
				queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			} else {
				t.Fatal("sql not initiated")
			}
			_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello` VALUES('tiga')")
			if err != nil {
				return err
			}
			_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_2` VALUES('empat')")
			if err != nil {
				return err
			}
			return nil
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
	var queryable *gosl.Queryable
	queryable, ok := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	if !ok {
		t.Fatal("sql is not initated")
	}
	_, err := queryable.ExecContext(ctx, "DELETE FROM `hello`")
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}
	if ctx, err := kit.ContextSwitch(ctx, TKey); err == nil {
		var queryable *gosl.Queryable
		ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
		if ok {
			queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
		} else {
			t.Fatal("sql not initiated")
		}
		_, err = queryable.ExecContext(ictx.Base(), "DELETE FROM `world`")
		if err != nil {
			log.Fatal(err.Error())
			t.Fail()
		}
		ctx, err = kit.ContextReset(ctx)
		if err != nil {
			log.Fatal(err.Error())
			t.Fail()
		}
		err = kit.RunInTransaction(
			ctx,
			func(ctx context.Context) error {
				var queryable *gosl.Queryable
				ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
				if ok {
					queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
				} else {
					t.Fatal("sql not initiated")
				}
				_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello` VALUES('sepuluh')")
				if err != nil {
					return err
				}
				if ctx, err = kit.ContextSwitch(ctx, TKey); err != nil {
					t.Fatal("failed to get queryable")
				}
				ictx, ok = ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
				if ok {
					queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
				} else {
					t.Fatal("sql not initiated")
				}
				_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `world` VALUES('empat')")
				if err != nil {
					return err
				}
				return nil
			},
		)
		if err != nil {
			log.Fatal(err.Error())
			t.Fail()
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
	var queryable *gosl.Queryable
	var ok bool
	queryable, ok = ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	if !ok {
		t.Fatal("sql not initated")
	}
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
	err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) error {
			var queryable *gosl.Queryable
			ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
			if ok {
				queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			} else {
				t.Fatal("sql not initiated")
			}
			_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello` VALUES('tigabelas')")
			if err != nil {
				return err
			}
			_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_2` VALUES('empatbelas')")
			if err != nil {
				return err
			}
			err = kit.RunInTransaction(
				ctx,
				func(ctx context.Context) error {
					var queryable *gosl.Queryable
					ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
					if ok {
						queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
					} else {
						t.Fatal("sql not initiated")
					}
					_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello` VALUES('tigabelasbelas')")
					if err != nil {
						return err
					}
					if ctx, err = kit.ContextSwitch(ctx, TKey); err == nil {
						ictx, ok = ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
						if ok {
							queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
						} else {
							t.Fatal("sql not initiated")
						}
					}
					_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `world` VALUES('empat')")
					if err != nil {
						return err
					}
					return nil
				})
			return err
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
			"testTx",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	kit := gosl.New(ctx)
	var queryable *gosl.Queryable
	queryable, ok := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	if !ok {
		t.Fatal("sql not initiated")
	}
	_, err := queryable.ExecContext(ctx, "DELETE FROM `hello_1`")
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
	err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) error {
			var queryable *gosl.Queryable
			ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
			if ok {
				queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			} else {
				t.Fatal("sql not initiated")
			}
			_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_1` (data) VALUES('satutigabelas')")
			if err != nil {
				return err
			}
			_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_2` (data) VALUES('satuempatbelas')")
			if err != nil {
				return err
			}

			err = kit.RunInTransaction(
				ctx,
				func(ctx context.Context) error {
					var queryable *gosl.Queryable
					ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
					if ok {
						queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
					} else {
						t.Fatal("sql not initiated")
					}
					_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_1` (data) VALUES('satutigabelasbelas')")
					if err != nil {
						return err
					}
					return errors.New("fail deliberately")
				},
			)
			return err
		},
	)
	if err != nil {
		log.Fatal("should failed but not")
		t.Fail()
	}
}
