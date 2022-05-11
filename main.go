package main

import (
	"github.com/damoon/lab-gostreams/stream"
)

func main() {
	usernames := stream.From([]string{"a", "bv"})
	// strLength := func(s string) int { return len(s) }
	// length_of_usernames := stream.Map(usernames, strLength)
	// stream.Print(length_of_usernames)
	_ = usernames
}
