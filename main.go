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
				fmt.Println("\nShutdown signal received. Cleaning up...")
				return
			case jobQueue <- job:
				fmt.Printf("Queued job #%d\n", job.ID)
			}
		}
	}()

	// consume and collects results from the result chan
	failedJobs := []producer.JobResult{}
	successJobs := []producer.JobResult{}
	var interruptedJobs []producer.JobResult

	for res := range resultChan {
		if !res.Success {
			failedJobs = append(failedJobs, res)
		}else {
			successJobs = append(successJobs, res)
		}
	}
	

	elapsed := time.Since(start)
	fmt.Printf("[First Batch] Completed jobs: %d. Failed jobs count: %d. Total time: %v\n", len(successJobs), len(failedJobs), elapsed)

	// Retry Logic Batch 2
	attempt := 0
	for len(failedJobs) > 0 && attempt < MaxRetries {
		attempt ++ 

		failedJobQueue := make(chan producer.JobResult, QueueCapacity)
		retryResultChan := make(chan producer.JobResult, len(failedJobs))
		interruptedChan := make(chan producer.JobResult, len(failedJobs))

		var retryWorkers sync.WaitGroup
		retryWorkers.Add(NumberOfWorkers)
		for w:=0; w < NumberOfWorkers; w++ {
			go worker.RetryWorker(w, failedJobQueue, retryResultChan, &retryWorkers)
		}

		// Wait for the workers to finish
		go func(){
			retryWorkers.Wait()
			close(retryResultChan)
		}()

		go func(){
			defer close(failedJobQueue)
			defer close(interruptedChan)
			for i, job := range failedJobs {
				fmt.Printf("Retrying job #%d\n", job.JobID)
				select {
				case <- ctx.Done():
					fmt.Println("\n[RETRY] Shutdown signal received. Cleaning up...")

					for _, interruptedJob := range failedJobs[i:] {
						interruptedChan <- interruptedJob
					}
					return
				case failedJobQueue <- job:
					fmt.Printf("[RETRIED]: Queued job #%d\n", job.JobID)
				}
			}
		}()


		for job := range interruptedChan {
    		interruptedJobs = append(interruptedJobs, job)
		}

		var stillFailing []producer.JobResult
		for res := range retryResultChan{
			fmt.Printf(">>> Retry Result -> JobID: %d | Success: %v\n", res.JobID, res.Success)
			if !res.Success {
				stillFailing = append(stillFailing, res)
            	fmt.Printf("[Retry Attempt %d] Job #%d FAILED\n", attempt, res.JobID)
			} else {
				successJobs = append(successJobs, res)
				fmt.Printf("[Retry Attempt %d] Job #%d SUCCESS\n", attempt, res.JobID)
			}
		}

		failedJobs = stillFailing
	}

	finalTimeElapsed := time.Since(start)
	fmt.Printf("\n=== FINAL RESULTS ===\n")
	fmt.Printf("Successful jobs: %d\n", len(successJobs))
	fmt.Printf("Interrupted jobs: %d\n", len(interruptedJobs))
	fmt.Printf("Permanently failed jobs: %d\n", len(failedJobs))
	fmt.Printf("Total time: %v\n", finalTimeElapsed)
}
