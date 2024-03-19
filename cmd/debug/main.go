package main

import (
	"log"
	"os"
	"time"

	"github.com/damoon/lab-gostreams/stream"
	"github.com/pkg/profile"
)

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

	source, emmit, flush := stream.NewStream[int, int]()
	go func() {
		c := 0
		for i := 0; i < 1_000; i++ {
			for j := 0; j < 10_000; j++ {
				c++
				d := c
				emmit(&stream.Record[int, int]{
					Key:   &d,
					Value: &d,
				})
			}
			flush()
		}
		source.Close()
	}()

	// isEven := func(m *Record[int, int]) bool {
	// 	return (*m.v)%2 == 0
	// }
	true_ := func(m *stream.Record[int, int]) bool {
		return true
	}

	evenNumbers := source.Filter(true_)

	msgCount, ctlCount := evenNumbers.Validate()

	log.Printf("msgCount: %d", msgCount)
	log.Printf("ctlCount: %d", ctlCount)

	log.Printf("now: %v", time.Since(start))

	return nil
}
