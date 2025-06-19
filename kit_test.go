package gosl_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/louvri/gosl"
)

type TestKey int

var TKey TestKey = 13
var TKey2 TestKey = 114

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
			"test_1",
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
			_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello` VALUES('satutigabelas')")
			if err != nil {
				return err
			}
			_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_2` VALUES('satuempatbelas')")
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
					_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello` VALUES('satutigabelasbelas')")
					if err != nil {
						return err
					}
					return errors.New("fail deliberately")
				},
			)
			return err
		},
	)
	if err == nil {
		log.Fatal("should failed but not")
		t.Fail()
	}
}

func TestNestedRunInTransactionWithSwitchContextWithError(t *testing.T) {
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
	ctx = context.WithValue(ctx,
		TKey,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"testTx2",
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

	if ctx, err = kit.ContextSwitch(ctx, TKey); err == nil {
		var queryable *gosl.Queryable

		ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
		if ok {
			queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			ctx = ictx.Base()
		} else {
			ref := ctx.Value(gosl.SQL_KEY)
			if ref == nil {
				err = errors.New("database is not initialized")
				t.Log(err.Error())
				t.FailNow()
			}
			queryable = ref.(*gosl.Queryable)
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
	}

	ctx, err = kit.ContextReset(ctx)
	if err != nil {
		log.Fatal(err.Error())
		t.Fail()
	}

	service1 := func(ctx context.Context) error {
		kit := gosl.New(ctx)
		err := kit.RunInTransaction(ctx, func(ctx context.Context) error {
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
			return nil
		})
		return err
	}

	service2 := func(ctx context.Context) error {
		kit := gosl.New(ctx)
		err := kit.RunInTransaction(ctx, func(ctx context.Context) error {
			if ctx, err = kit.ContextSwitch(ctx, TKey); err == nil {
				var queryable *gosl.Queryable
				ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
				if ok {
					queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
					ctx = ictx.Base()
				} else {
					ref := ctx.Value(gosl.SQL_KEY)
					if ref == nil {
						err = errors.New("database is not initialized")
						t.Log(err.Error())
						t.FailNow()
					}
					queryable = ref.(*gosl.Queryable)
				}
				_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_1` (data) VALUES('satutigabelas')")
				if err != nil {
					return err
				}
				_, err = queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_2` (data) VALUES('satuempatbelas')")
				if err != nil {
					return err
				}
			}
			return nil
		})
		return err
	}

	err = kit.RunInTransaction(
		ctx,
		func(ctx context.Context) error {
			err := service1(ctx)
			if err != nil {
				return err
			}

			err = service2(ctx)
			if err != nil {
				return err
			}

			ctx, err = kit.ContextReset(ctx)
			if err != nil {
				log.Fatal(err.Error())
				t.Fail()
			}

			// ctx, err = kit.RunInTransaction(
			// 	ctx,
			// 	func(ctx context.Context) (context.Context, error) {
			// 		var queryable *gosl.Queryable
			// 		ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
			// 		if ok {
			// 			queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			// 		} else {
			// 			t.Fatal("sql not initiated")
			// 		}
			// 		_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_1` (data) VALUES('satutigabelasbelas')")
			// 		if err != nil {
			// 			return ctx, err
			// 		}

			// 		if ctx, err = kit.ContextSwitch(ctx, TKey); err == nil {
			// 			var queryable *gosl.Queryable
			// 			ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
			// 			if ok {
			// 				queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			// 			} else {
			// 				ref := ctx.Value(gosl.SQL_KEY)
			// 				if ref == nil {
			// 					err = errors.New("database is not initialized")
			// 					t.Log(err.Error())
			// 					t.FailNow()
			// 				}
			// 				queryable = ref.(*gosl.Queryable)
			// 			}
			// 			_, err := queryable.ExecContext(ictx.Base(), "INSERT INTO `hello_1` (data) VALUES('satutigabelasbelas')")
			// 			if err != nil {
			// 				return ctx, err
			// 			}
			// 		}
			// 		return ctx, nil
			// 		// return errors.New("fail deliberately")
			// 	},
			// )
			return errors.New("fail deliberately")
		},
	)
	if err == nil {
		log.Fatal("should failed but not")
		t.Fail()
	}
}

func TestNestedRunInTransactionWithSwitchContext(t *testing.T) {
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
	ctx = context.WithValue(ctx,
		TKey,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"testTx2",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))
	ctx = context.WithValue(ctx,
		TKey2,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"testTx3",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))

	if err := Reset(ctx); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	if err := EP(ctx); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestNestedRunInTransactionWithSwitchContextAndError(t *testing.T) {
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
	ctx = context.WithValue(ctx,
		TKey,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"testTx2",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))

	if err := Reset(ctx); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	if err := EPWError(ctx); err == nil {
		t.Log("should error")
		t.FailNow()
	}
}

func TestNestedRunInTransactionWithSwitchContextAndErrorConsecutive(t *testing.T) {
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
	ctx = context.WithValue(ctx,
		TKey,
		gosl.NewQueryable(gosl.ConnectToDB(
			"root",
			"abcd",
			"localhost",
			"3306",
			"testTx2",
			1,
			1,
			2*time.Minute,
			2*time.Minute,
		)))

	if err := Reset(ctx); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	if err := EPWError(ctx); err == nil {
		t.Log("should error")
		t.FailNow()
	}

	if err := EP(ctx); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func SVC2Error(ctx context.Context) (context.Context, error) {
	kit := gosl.New(ctx)
	err := kit.RunInTransaction(ctx, func(ctx context.Context) error {
		if ctx, err := kit.ContextSwitch(ctx, TKey); err == nil {
			err = Repo(ctx, "SWITCHsatutigabelas2")
			if err != nil {
				return err
			}

		}
		if ctx, err := kit.ContextReset(ctx); err == nil {
			err = Repo(ctx, "RESETsatutigabelas2")
			if err != nil {
				return err
			}
		}
		return errors.New("forced error")
	})
	return ctx, err
}

func EPWError(ctx context.Context) error {
	kit := gosl.New(ctx)
	err := kit.RunInTransaction(
		ctx,
		func(ctx context.Context) error {
			err := SVC1(ctx)
			if err != nil {
				return err
			}

			ctx, err = SVC2Error(ctx)
			if err != nil {
				return err
			}
			return err
		},
	)
	return err
}

func EP(ctx context.Context) error {
	kit := gosl.New(ctx)
	err := kit.RunInTransaction(
		ctx,
		func(ctx context.Context) error {
			err := SVC1(ctx)
			if err != nil {
				return err
			}

			err = SVC2(ctx)
			if err != nil {
				return err
			}
			return err
		},
	)
	return err
}

func SVC1(ctx context.Context) error {
	kit := gosl.New(ctx)
	err := kit.RunInTransaction(ctx, func(ctx context.Context) error {
		if ctx, err := kit.ContextSwitch(ctx, TKey); err == nil {
			err = Repo(ctx, "SWITCHsatutigabelas1")
			if err != nil {
				return err
			}
		}
		if ctx, err := kit.ContextSwitch(ctx, TKey2); err == nil {
			err = Repo(ctx, "SWITCH2satutigabelas1")
			if err != nil {
				return err
			}
		}
		if ctx, err := kit.ContextReset(ctx); err == nil {
			err = Repo(ctx, "RESETsatutigabelas1")
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func SVC2(ctx context.Context) error {
	kit := gosl.New(ctx)
	err := kit.RunInTransaction(ctx, func(ctx context.Context) error {
		if ctx, err := kit.ContextSwitch(ctx, TKey); err == nil {
			err = Repo(ctx, "SWITCHsatutigabelas2")
			if err != nil {
				return nil
			}

		}
		if ctx, err := kit.ContextSwitch(ctx, TKey2); err == nil {
			err = Repo(ctx, "SWITCH2satutigabelas1")
			if err != nil {
				return err
			}
		}
		if ctx, err := kit.ContextReset(ctx); err == nil {
			err = Repo(ctx, "RESETsatutigabelas2")
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func Repo(ctx context.Context, data string) error {
	kit := gosl.New(ctx)
	err := kit.RunInTransaction(ctx, func(ctx context.Context) error {
		var queryable *gosl.Queryable
		ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
		if ok {
			queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
		} else {
			ref := ctx.Value(gosl.SQL_KEY)
			if ref == nil {
				err := errors.New("database is not initialized")
				return err
			}
			queryable = ref.(*gosl.Queryable)
		}
		_, err := queryable.ExecContext(ctx, fmt.Sprintf("INSERT INTO `hello_1` (data) VALUES('%s')", data))
		if err != nil {
			return err
		}
		_, err = queryable.ExecContext(ctx, fmt.Sprintf("INSERT INTO `hello_2` (data) VALUES('%s')", data))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func Reset(ctx context.Context) error {
	kit := gosl.New(ctx)
	var queryable *gosl.Queryable
	queryable, ok := ctx.Value(gosl.SQL_KEY).(*gosl.Queryable)
	if !ok {
		return errors.New("sql not initiated")
	}
	_, err := queryable.ExecContext(ctx, "DELETE FROM `hello_1`")
	if err != nil {
		return err
	}
	_, err = queryable.ExecContext(ctx, "DELETE FROM `hello_2`")
	if err != nil {
		return err
	}

	if ctx, err = kit.ContextSwitch(ctx, TKey); err == nil {
		var queryable *gosl.Queryable

		ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
		if ok {
			queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			ctx = ictx.Base()
		} else {
			ref := ctx.Value(gosl.SQL_KEY)
			if ref == nil {
				err = errors.New("database is not initialized")
				return err
			}
			queryable = ref.(*gosl.Queryable)
		}

		_, err := queryable.ExecContext(ctx, "DELETE FROM `hello_1`")
		if err != nil {
			return err
		}
		_, err = queryable.ExecContext(ctx, "DELETE FROM `hello_2`")
		if err != nil {
			return err
		}
	}

	if ctx, err = kit.ContextSwitch(ctx, TKey2); err == nil {
		var queryable *gosl.Queryable

		ictx, ok := ctx.Value(gosl.INTERNAL_CONTEXT).(*gosl.InternalContext)
		if ok {
			queryable = ictx.Get(gosl.SQL_KEY).(*gosl.Queryable)
			ctx = ictx.Base()
		} else {
			ref := ctx.Value(gosl.SQL_KEY)
			if ref == nil {
				err = errors.New("database is not initialized")
				return err
			}
			queryable = ref.(*gosl.Queryable)
		}

		_, err := queryable.ExecContext(ctx, "DELETE FROM `hello_1`")
		if err != nil {
			return err
		}
		_, err = queryable.ExecContext(ctx, "DELETE FROM `hello_2`")
		if err != nil {
			return err
		}
	}

	return nil

}
