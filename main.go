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
const NumberOfWorkers int = 5

func main() {
	//Listening for OFF signals 
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()


	start := time.Now()

	jobs := producer.GenerateJobs(NumberOfJobs, DurationSec) //Slice
	jobQueue := make(chan producer.Job, QueueCapacity)       //buffer for the specified queue capacity


	// Start Workers
	var workerGroup sync.WaitGroup
	workerGroup.Add(NumberOfWorkers)
	for w := 0; w < NumberOfWorkers; w++ {
		//fmt.Printf(">>Workers Started\n")
		go worker.Worker(w, jobQueue, &workerGroup)
	}

	
	for _, job := range jobs {
		fmt.Printf("Producing job #%d\n", job.ID)
		select {
		case <-ctx.Done():
			//Stop Producing Jobs
			fmt.Println(">>>Producer Stopping...")
			close(jobQueue)
			workerGroup.Wait()
			return
		case jobQueue <- job:
		 	fmt.Printf("Queued job #%d\n", job.ID) 
		}
	}

	<-ctx.Done() //wait for signal
	fmt.Println("\nShutdown signal received. Cleaning up...")

	close(jobQueue)
	workerGroup.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Completed %d jobs in %v\n", NumberOfJobs, elapsed)
}
