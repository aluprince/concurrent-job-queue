package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/aluprince/concurrent-job-queue/src/producers"
	"github.com/aluprince/concurrent-job-queue/src/workers"
	"os"
	"os/signal"
	"context"
	"syscall"
)

const NumberOfJobs int = 100
const DurationSec int = 50
const QueueCapacity int = 10
const NumberOfWorkers int = 10
const MaxRetries int = 2
//var Fail int= 20




func main() {
	//Listening for OFF signals 
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()


	start := time.Now()


	jobs := producer.GenerateJobs(NumberOfJobs, DurationSec) //Slice
	//fmt.Printf(">>Jobs: %v", jobs)
	jobQueue := make(chan producer.Job, QueueCapacity)       //buffer for the specified queue capacity
	resultChan := make(chan producer.JobResult, NumberOfJobs)


	// Start Workers
	var workerGroup sync.WaitGroup
	workerGroup.Add(NumberOfWorkers)
	for w := 0; w < NumberOfWorkers; w++ {
		//fmt.Printf(">>Workers Started\n")
		go worker.Worker(w, jobQueue, resultChan, &workerGroup)
	}

	// Close resultChan after workers are done
	go func(){
		workerGroup.Wait()
		close(resultChan)
	}()

	// Producers(producing jobs)
	go func(){
		defer close(jobQueue)
		for _, job := range jobs {
			fmt.Printf("Producing job #%d\n", job.ID)
			select {
			case <- ctx.Done():
				workerGroup.Wait()
				fmt.Println("\nShutdown signal received. Cleaning up...")
				return
			case jobQueue <- job:
				fmt.Printf("Queued job #%d\n", job.ID)
			}
		}
	}()

	// consume and collects results from the result chan
	failedJobs := []producer.JobResult{}
	for res := range resultChan {
		if res.Success != true {
			failedJobs = append(failedJobs, res)
		}
	}
	
	NumberOfJobsCompleted := NumberOfJobs - len(failedJobs)
	

	elapsed := time.Since(start)
	fmt.Printf("Completed jobs: %d. Failed jobs count: %d. Total time: %v\n", NumberOfJobsCompleted, len(failedJobs), elapsed)
}
