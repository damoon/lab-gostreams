package main

import (
	"log"
	"time"
)

type Message struct {
	value  int
	commit bool
}

func main() {

	start := time.Now()

	ch1 := make(chan Message)

	go func() {
		for i := 0; i < 1000; i++ {
			for i := 0; i < 10000; i++ {
				ch1 <- Message{value: i}
			}
			ch1 <- Message{commit: true}
		}
		close(ch1)
	}()

	ch2 := make(chan Message)

	go func() {
		for m := range ch1 {
			if m.commit {
				ch2 <- Message{commit: true}
				continue
			}
			ch2 <- Message{value: m.value}
		}
		close(ch2)
	}()

	msgCount := 0
	ctlCount := 0
	for m := range ch2 {
		if m.commit {
			ctlCount++
			continue
		}
		msgCount++
	}

	log.Printf("msgCount: %d", msgCount)
	log.Printf("ctlCount: %d", ctlCount)

	log.Printf("now: %v", time.Since(start))
}
