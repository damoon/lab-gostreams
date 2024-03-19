package stream

import "log"

type ControlMsg int64

const (
	buffer            = 1_000
	Drain  ControlMsg = iota
)

type Record[K, V any] struct {
	controlMsg ControlMsg
	Key        *K
	Value      *V
}

type Stream[K, V any] struct {
	c chan *Record[K, V]
}

func NewStream[K, V any]() (Stream[K, V], func(*Record[K, V]), func()) {
	c := make(chan *Record[K, V], buffer)
	s := Stream[K, V]{
		c,
	}
	emmit := func(m *Record[K, V]) {
		c <- m
	}
	flush := func() {
		c <- &Record[K, V]{
			controlMsg: Drain,
		}
	}
	return s, emmit, flush
}

func (s Stream[K, V]) Close() {
	close(s.c)
}

func From[V any](list []V) Stream[int, V] {
	new, emmit, flush := NewStream[int, V]()

	go func() {
		for i, value := range list {
			emmit(&Record[int, V]{Key: &i, Value: &value})
		}
		flush()
		new.Close()
	}()

	return new
}

func ToStream[K, V any](list []Record[K, V]) Stream[K, V] {
	new, emmit, flush := NewStream[K, V]()

	go func() {
		for _, msg := range list {
			emmit(&msg)
		}
		flush()
		new.Close()
	}()

	return new
}

func (s Stream[K, V]) transform(transformer func(m *Record[K, V], emmit func(*Record[K, V]))) Stream[K, V] {
	return transform(s, transformer)
}

func transform[K1, V1, K2, V2 any](s Stream[K1, V1], transformer func(m *Record[K1, V1], emmit func(*Record[K2, V2]))) Stream[K2, V2] {
	new, emmit, flush := NewStream[K2, V2]()

	go func() {
		for e := range s.c {
			if e.controlMsg == Drain {
				flush()
				continue
			}
			transformer(e, emmit)
		}
		close(new.c)
	}()

	return new
}

func (s Stream[K, V]) Print() {
	s.process(func(m *Record[K, V]) {
		log.Println(m)
	})
}

func (s Stream[K, V]) Foreach(task func(m *Record[K, V])) {
	s.process(task)
}

func (s Stream[K, V]) Peek(peeker func(m *Record[K, V])) Stream[K, V] {
	return s.transform(func(m *Record[K, V], emmit func(*Record[K, V])) {
		peeker(m)
		emmit(m)
	})
}

func (s Stream[K, V]) process(processor func(m *Record[K, V])) {
	go func() {
		for e := range s.c {
			if e.controlMsg == Drain {
				continue
			}
			processor(e)
		}
	}()
}

func (s Stream[K, V]) Filter(filter func(m *Record[K, V]) bool) Stream[K, V] {
	p := func(m *Record[K, V], emmit func(*Record[K, V])) {
		if filter(m) {
			emmit(m)
		}
	}
	return s.transform(p)
}

func (s Stream[K, V]) Map(map_ func(m *Record[K, V]) *Record[K, V]) Stream[K, V] {
	return s.transform(func(m *Record[K, V], emmit func(*Record[K, V])) {
		m2 := map_(m)
		emmit(m2)
	})
}

func Map[K1, V1, K2, V2 any](s Stream[K1, V1], map_ func(m *Record[K1, V1]) *Record[K2, V2]) Stream[K2, V2] {
	return transform(s, func(m *Record[K1, V1], emmit func(*Record[K2, V2])) {
		m2 := map_(m)
		emmit(m2)
	})
}

func (s Stream[K, V]) Validate() (int, int) {
	msgCount := 0
	ctlCount := 0

	for e := range s.c {
		if e.controlMsg == Drain {
			ctlCount++
			continue
		}

		msgCount++
	}

	return msgCount, ctlCount
}
