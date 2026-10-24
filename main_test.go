package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWorkersProcessEveryJobOnce(t *testing.T) {
	for _, count := range []int{0, 1, 17} {
		t.Run(fmt.Sprintf("jobs_%d", count), func(t *testing.T) {
			// Небуферизированные каналы проверяют, что буферы не скрывают deadlock.
			jobs := make(chan Job)
			results := make(chan Result)
			var wg sync.WaitGroup
			wg.Add(workerCount)
			for i := 0; i < workerCount; i++ {
				go worker(jobs, results, &wg)
			}
			go func() {
				defer close(jobs)
				for id := 1; id <= count; id++ {
					// Одинаковые URL — разные задания, их нельзя терять при сборе.
					jobs <- Job{ID: id, URL: "https://example.com"}
				}
			}()
			go func() {
				wg.Wait()
				close(results)
			}()
			seen := make(map[int]bool)
			for result := range results {
				if result.Job.ID < 1 || result.Job.ID > count || seen[result.Job.ID] {
					t.Errorf("unexpected or repeated job ID: %d", result.Job.ID)
				}
				seen[result.Job.ID] = true
				if result.Job.URL != "https://example.com" || result.Status != processed {
					t.Errorf("incorrect result: %+v", result)
				}
				if result.Duration < 100*time.Millisecond {
					t.Errorf("duration too short: %s", result.Duration)
				}
			}
			if len(seen) != count {
				t.Errorf("received %d results, want %d", len(seen), count)
			}
		})
	}
}
