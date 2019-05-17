package main

import (
	"fmt"

	"github.com/256dpi/turing"
)

type observer struct {}

func (*observer) Init() {
	fmt.Printf("==> Init\n")
}

func (*observer) Process(i turing.Instruction) bool {
	fmt.Printf("==> %+v\n", i)
	return true
}

