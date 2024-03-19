debug:
	go run ./bin/debug

profile-cpu:
	PROFILE=cpu make debug
	go tool pprof -http=:8080 cpu.pprof

profile-mem:
	PROFILE=mem make debug
	go tool pprof -http=:8080 mem.pprof

profile-trace:
	PROFILE=trace make debug
	go tool trace trace.out
