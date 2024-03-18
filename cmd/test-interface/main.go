package main

import (
	"log"
	"time"
)

type Iterator[T any] interface {
	Next() bool
	Value() T
	Paused() bool
}

type OutputIterator[T any] interface {
	Iterator[T]
	SetValue(x T)
	Pause()
	Close()
}

type Stream[T any] struct {
	ch  chan Message[T]
	msg Message[T]
}

type Message[T any] struct {
	value T
	pause bool
}

func (s Stream[T]) Next() bool {
	msg, more := <-s.ch
	s.msg = msg
	return more
}

func (s Stream[T]) Value() T {
	return s.msg.value
}

func (s Stream[T]) SetValue(x T) {
	s.ch <- Message[T]{
		value: x,
	}
}

func (s Stream[T]) Pause() {
	s.ch <- Message[T]{
		pause: true,
	}
}

func (s Stream[T]) Paused() bool {
	return s.msg.pause
}

func (s Stream[T]) Close() {
	close(s.ch)
}

func NewStream[T any]() Stream[T] {
	ch := make(chan Message[T])
	return Stream[T]{ch: ch}
}

func main() {

	start := time.Now()

	s1 := NewStream[int]()

	go func() {
		for i := 0; i < 1000; i++ {
			for i := 0; i < 10000; i++ {
				s1.SetValue(i)
			}
			s1.Pause()
		}
		s1.Close()
	}()

	s2 := NewStream[int]()

	go func() {
		for s1.Next() {
			if s1.Paused() {
				s2.Pause()
				continue
			}
			s2.SetValue(s1.Value())
		}
		s2.Close()
	}()

	msgCount := 0
	ctlCount := 0
	for s2.Next() {
		if s2.Paused() {
			ctlCount++
			continue
		}
		msgCount++
	}

	log.Printf("msgCount: %d", msgCount)
	log.Printf("ctlCount: %d", ctlCount)

	log.Printf("now: %v", time.Since(start))
}
