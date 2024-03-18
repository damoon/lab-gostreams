package main

import "log"

func main() {

	ctl1 := make(chan interface{})
	msg1 := make(chan int)

	go func() {
		for i := 0; i < 10; i++ {
			for i := 0; i < 1000; i++ {
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
					continue
				}
				msg2 <- m
			case c, ok := <-ctl1:
				if !ok {
					ctl1 = nil
					continue
				}
				for len(msg1) > 0 {
					m := <-msg1
					msg2 <- m
				}
				ctl2 <- c
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
				continue
			}
			msgCount++
		case _, ok := <-ctl2:
			if !ok {
				ctl2 = nil
				continue
			}
			for len(msg2) > 0 {
				<-msg2
				msgCount++
			}
			ctlCount++
		}
		if ctl2 == nil && msg2 == nil {
			break
		}
	}

	log.Printf("msgCount: %d", msgCount)
	log.Printf("ctlCount: %d", ctlCount)
}
