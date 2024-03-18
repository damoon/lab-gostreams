package main

import "log"

type Envelope[K, V any] struct {
	Flush   *Flush
	Message *Message[K, V]
}

type Flush struct{}

type Message[K, V any] struct {
	k K
	v V
}

type Stream[K, V any] struct {
	c chan *Envelope[K, V]
}

func NewStream[K, V any]() (Stream[K, V], func(*Message[K, V]), func()) {
	c := make(chan *Envelope[K, V])
	s := Stream[K, V]{
		c,
	}
	emmit := func(m *Message[K, V]) {
		c <- &Envelope[K, V]{
			Message: m,
		}
	}
	flush := func() {
		c <- &Envelope[K, V]{
			Flush: &Flush{},
		}
	}
	return s, emmit, flush
}

func (s Stream[K, V]) Close() {
	close(s.c)
}

func (s Stream[K, V]) transform(p func(m *Message[K, V], emmit func(*Message[K, V]))) Stream[K, V] {
	return transform(s, p)
}

func transform[K1, V1, K2, V2 any](s Stream[K1, V1], p func(m *Message[K1, V1], emmit func(*Message[K2, V2]))) Stream[K2, V2] {
	new, emmit, flush := NewStream[K2, V2]()

	go func() {
		for e := range s.c {
			if e.Message != nil {
				p(e.Message, emmit)
				continue
			}
			if e.Flush != nil {
				flush()
				continue
			}
		}
		close(new.c)
	}()

	return new
}

func (s Stream[K, V]) Filter(filter func(m *Message[K, V]) bool) Stream[K, V] {
	p := func(m *Message[K, V], emmit func(*Message[K, V])) {
		if filter(m) {
			emmit(m)
		}
	}
	return s.transform(p)
}

func (s Stream[K, V]) Map(map_ func(m *Message[K, V]) *Message[K, V]) Stream[K, V] {
	return s.transform(func(m *Message[K, V], emmit func(*Message[K, V])) {
		m2 := map_(m)
		emmit(m2)
	})
}

func Map[K1, V1, K2, V2 any](s Stream[K1, V1], map_ func(m *Message[K1, V1]) *Message[K2, V2]) Stream[K2, V2] {
	return transform(s, func(m *Message[K1, V1], emmit func(*Message[K2, V2])) {
		m2 := map_(m)
		emmit(m2)
	})
}

func (s Stream[K, V]) Count() int {
	c := 0

	for e := range s.c {
		if e.Message != nil {
			c++
		}
	}

	return c
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	source, emmit, flush := NewStream[int, int]()
	go func() {
		c := 0
		for i := 0; i < 1_000; i++ {
			for j := 0; j < 10_000; j++ {
				c++
				emmit(&Message[int, int]{
					k: c,
					v: c,
				})
			}
			flush()
		}
		source.Close()
	}()

	isEven := func(m *Message[int, int]) bool {
		return m.v%2 == 0
	}

	evenNumbers := source.Filter(isEven)

	count := evenNumbers.Count()

	log.Printf("count: %d", count)

	return nil
}
