package main

import (
	"fmt"
	"sync"
	//"time"

	"github.com/aluprince/concurrent-job-queue/src/producers"
	"github.com/aluprince/concurrent-job-queue/src/workers"
)

const NumberOfJobs int = 100
const DurationSec int = 50
const QueueCapacity int = 10
const NumberOfWorkers int = 1

func main() {
	fmt.Printf(">>> MAIN >>>")

	jobs := producer.GenerateJobs(NumberOfJobs, DurationSec) //Slice
	jobQueue := make(chan producer.Job, QueueCapacity)       //buffer for the specified queue capacity


	// Start Workers
	var workerGroup sync.WaitGroup
	workerGroup.Add(NumberOfWorkers)
	for w := 0; w < NumberOfWorkers; w++ {
		fmt.Printf(">>Workers Started\n")
		go worker.Worker(w, jobQueue, &workerGroup)
	}

	for _, job := range jobs {
		jobQueue <- job
	}


	close(jobQueue)
	workerGroup.Wait()
}
