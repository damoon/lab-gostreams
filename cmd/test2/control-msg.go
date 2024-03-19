package main

import (
	"log"
	"os"
	"time"

	"github.com/pkg/profile"
)

type ControlMsg int64

const (
	buffer            = 1_000
	Drain  ControlMsg = iota
)

type Record[K, V any] struct {
	ControlMsg ControlMsg
	k          *K
	v          *V
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
			ControlMsg: Drain,
		}
	}
	return s, emmit, flush
}

func (s Stream[K, V]) Close() {
	close(s.c)
}

func (s Stream[K, V]) transform(p func(m *Record[K, V], emmit func(*Record[K, V]))) Stream[K, V] {
	return transform(s, p)
}

func transform[K1, V1, K2, V2 any](s Stream[K1, V1], transformation func(m *Record[K1, V1], emmit func(*Record[K2, V2]))) Stream[K2, V2] {
	new, emmit, flush := NewStream[K2, V2]()

	go func() {
		for e := range s.c {
			if e.ControlMsg == Drain {
				flush()
				continue
			}
			transformation(e, emmit)
		}
		close(new.c)
	}()

	return new
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
		if e.ControlMsg == Drain {
			ctlCount++
			continue
		}

		msgCount++
	}

	return msgCount, ctlCount
}

func main() {
	switch os.Getenv("PROFILE") {
	case "cpu":
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
	case "trace":
		defer profile.Start(profile.TraceProfile, profile.ProfilePath(".")).Stop()
	}

	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	start := time.Now()

	source, emmit, flush := NewStream[int, int]()
	go func() {
		c := 0
		for i := 0; i < 1_000; i++ {
			for j := 0; j < 10_000; j++ {
				c++
				d := c
				emmit(&Record[int, int]{
					k: &d,
					v: &d,
				})
			}
			flush()
		}
		source.Close()
	}()

	// isEven := func(m *Record[int, int]) bool {
	// 	return (*m.v)%2 == 0
	// }
	true_ := func(m *Record[int, int]) bool {
		return true
	}

	evenNumbers := source.Filter(true_)

	msgCount, ctlCount := evenNumbers.Validate()

	log.Printf("msgCount: %d", msgCount)
	log.Printf("ctlCount: %d", ctlCount)

	log.Printf("now: %v", time.Since(start))

	return nil
}
