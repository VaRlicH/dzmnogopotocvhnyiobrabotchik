package main

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"text/tabwriter"
	"time"
)

const (
	workerCount = 5
	processed   = "обработан"
)

type Job struct {
	ID  int
	URL string
}

type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
}

// simulateRequest имитирует запрос с задержкой от 100 до 500 мс.
func simulateRequest() {
	// Функции верхнего уровня math/rand безопасны для нескольких горутин.
	time.Sleep(time.Duration(100+rand.Intn(401)) * time.Millisecond)
}

func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		start := time.Now()
		simulateRequest()
		results <- Result{
			Job:      job,
			Status:   processed,
			Duration: time.Since(start),
		}
	}
}

func main() {
	urls := []string{
		"https://example.com",
		"https://go.dev",
		"https://pkg.go.dev",
		"https://github.com",
		"https://gitlab.com",
		"https://www.wikipedia.org",
		"https://www.mozilla.org",
		"https://www.rust-lang.org",
		"https://www.python.org",
		"https://www.kernel.org",
		"https://example.com/about",
		"https://example.com/contact",
	}

	start := time.Now()
	// Буферы ограничены числом воркеров, а не размером списка URL.
	jobs := make(chan Job, workerCount)
	results := make(chan Result, workerCount)

	// Fan-out: фиксированный пул читает задания из общего канала.
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker(jobs, results, &wg)
	}

	// Отдельный отправитель позволяет main сразу собирать результаты.
	go func() {
		defer close(jobs)
		for i, url := range urls {
			jobs <- Job{ID: i + 1, URL: url}
		}
	}()

	// Канал результатов закрывается только после завершения всех отправителей.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Fan-in: все воркеры отправляют результаты в один канал.
	// Только main изменяет слайс, поэтому мьютекс не нужен.
	allResults := make([]Result, 0, len(urls))
	for result := range results {
		allResults = append(allResults, result)
	}
	elapsed := time.Since(start)

	// Порядок завершения произвольный; отчёт выводим в порядке входных URL.
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Job.ID < allResults[j].Job.ID
	})
	printReport(allResults, len(urls), elapsed)
}

func printReport(results []Result, expected int, elapsed time.Duration) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tURL\tСтатус\tВремя")
	var total time.Duration
	successful := 0
	for _, result := range results {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n",
			result.Job.ID, result.Job.URL, result.Status, result.Duration.Round(time.Millisecond))
		total += result.Duration
		if result.Status == processed {
			successful++
		}
	}
	w.Flush()
	var average time.Duration
	if len(results) > 0 {
		average = total / time.Duration(len(results))
	}
	fmt.Printf("\nВоркеров: %d\nЗаданий: %d\nОбработано: %d\nУспешных операций: %d\n",
		workerCount, expected, len(results), successful)
	fmt.Printf("Среднее время запроса: %s\nСумма времени запросов: %s\nОбщее время работы пула: %s\n",
		average.Round(time.Millisecond), total.Round(time.Millisecond), elapsed.Round(time.Millisecond))
}
