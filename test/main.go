package main

import "fmt"

func main() {
	a := 1

	l1 := func(a int) {
		a = 8
	}

	l2 := func(a int) {
		a = 10
	}

	l1(a)
	l2(a)
	fmt.Println(a)
}
