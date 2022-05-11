package stream

type ControlMsg int64

const (
	buffer            = 10
	Drain  ControlMsg = iota
)

type Stream[K, V any] struct {
	msg chan Message[K, V]
	ctl chan ControlMsg
}

type Message[K, V any] struct {
	key   K
	value V
}

func From[V any](list []V) Stream[int, V] {
	msgs := make(chan Message[int, V], buffer)
	ctl := make(chan ControlMsg)

	go func() {
		for key, value := range list {
			msg := Message[int, V]{key, value}
			msgs <- msg
		}
		close(msgs)
		close(ctl)
	}()

	return Stream[int, V]{msgs, ctl}
}

func FromSlice[K, V any](list []Message[K, V]) Stream[K, V] {
	msgs := make(chan Message[K, V], buffer)
	ctl := make(chan ControlMsg)

	go func() {
		for _, msg := range list {
			msgs <- msg
		}
		close(msgs)
		close(ctl)
	}()

	return Stream[K, V]{msgs, ctl}
}

func Branch[K, V any](s Stream[K, V], distribution []func(Message[K, V]) bool) []Stream[K, V] {
	return []Stream[K, V]{}
}

func Filter[K, V any](s Stream[K, V], selector func(Message[K, V]) bool) Stream[K, V] {
	msg := make(chan Message[K, V], buffer)
	ctl := make(chan ControlMsg)
	stream := Stream[K, V]{
		msg,
		ctl,
	}

	go func() {
		for {
			select {
			case m, ok := <-s.msg:
				stream.msg <- m
				if !ok {
					s.msg = nil
				}
			case d, ok := <-s.ctl:
				for len(s.msg) > 0 {
					m := <-s.msg
					stream.msg <- m
				}

				stream.ctl <- d
				if !ok {
					s.ctl = nil
				}
			}

			if s.msg == nil && s.ctl == nil {
				break
			}
		}

		close(msg)
		close(ctl)
	}()

	return stream
}

func NotFilter[K, V any](s Stream[K, V], selector func(Message[K, V]) bool) Stream[K, V] {
	not := func(m Message[K, V]) bool {
		return !selector(m)
	}
	return Filter(s, not)
}

/*
func FlatMap[K, V any](s Stream[K, V], mapper func(Message[K, V]) []Message[K, V]) Stream[K, V] {
	return Stream[K, V]{}
}

func FlatMapValues[K, V any](s Stream[K, V], mapper func(V) []V) Stream[K, V] {
	return Stream[K, V]{}
}

func Foreach[K, V any](s Stream[K, V], foreach func(Message[K, V])) {
}

func GroupByKey[K, V any](s Stream[K, V]) Stream[K, V] {
	return Stream[K, V]{}
}

func GroupBy[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}

func Cogroup[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}

func Map[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}

func MapValues[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}

func Merge[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}

func Peek[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}

func Print[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}

func SelectKey[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}

func Repartition[K, V any](s Stream[K, V], foreach func(Message[K, V])) Stream[K, V] {
	return Stream[K, V]{}
}
*/
