package main

import (
	"log"
	"time"
)

func main() {

	start := time.Now()

	ctl1 := make(chan interface{})
	msg1 := make(chan int)

	go func() {
		for i := 0; i < 1_000; i++ {
			for i := 0; i < 10_000; i++ {
				msg1 <- i
			}
			ctl1 <- struct{}{}
		}
		close(ctl1)
		close(msg1)
	}()

	ctl2 := make(chan interface{})
	msg2 := make(chan int)
	go func() {
		for {
			select {
			case m, ok := <-msg1:
				if !ok {
					msg1 = nil
				} else {
					msg2 <- m
				}
			case c, ok := <-ctl1:
				if !ok {
					ctl1 = nil
				} else {
					for len(msg1) > 0 {
						m := <-msg1
						msg2 <- m
					}
					ctl2 <- c
				}
			}
			if ctl1 == nil && msg1 == nil {
				break
			}
		}
		close(ctl2)
		close(msg2)
	}()

	msgCount := 0
	ctlCount := 0
	for {
		select {
		case _, ok := <-msg2:
			if !ok {
				msg2 = nil
			} else {
				msgCount++
			}
		case _, ok := <-ctl2:
			if !ok {
				ctl2 = nil
			} else {
				for len(msg2) > 0 {
					<-msg2
					msgCount++
				}
				ctlCount++
			}
		}
		if ctl2 == nil && msg2 == nil {
			break
		}
	}

	log.Printf("msgCount: %d", msgCount)
	log.Printf("ctlCount: %d", ctlCount)

	log.Printf("now: %v", time.Since(start))
}
