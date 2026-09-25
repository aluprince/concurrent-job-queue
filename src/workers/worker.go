package worker

import (
	"fmt"
	"sync"
	"time"
	"math/rand/v2"

	"github.com/aluprince/concurrent-job-queue/src/producers"
)

func Worker(id int, jobs <-chan producer.Job, results chan<- producer.JobResult, workers *sync.WaitGroup) {
	defer workers.Done()

	for job := range jobs {
		success := rand.IntN(100) >= 20 // 80 percent success
		if !job.Fail {
			fmt.Printf("Worker #%d started job #%d\n", id, job.ID)
			time.Sleep(2 * time.Second)
			fmt.Printf("Worker #%d finished job #%d\n", id, job.ID)
			results <- producer.JobResult{JobID: job.ID, Success: success, Retries: 1}
		} else {
			fmt.Printf("Worker #%d FAILED job #%d\n", id, job.ID)
			results <- producer.JobResult{JobID: job.ID, Success: success, Retries: 1}
		}
	}
}

func RetryWorker(workerID int, jobs <- chan producer.JobResult, results chan <-producer.JobResult, retryWg *sync.WaitGroup) {
	defer retryWg.Done()

	for job := range jobs {
		success := rand.IntN(100) >= 80
		if job.Success == false {
			fmt.Printf("Retried Worker #%d Attempted job #%d\n", workerID, job.JobID)
			time.Sleep(2 * time.Second)
			fmt.Printf("Worker %d Completed job #%d\n", workerID, job.JobID)
			results <- producer.JobResult{JobID: job.JobID, Success: success, Retries: job.Retries+1}
		} else {
			fmt.Printf("Worker #%d FAILED job #%d\n", workerID, job.JobID)
			results <- producer.JobResult{JobID: job.JobID, Success: success, Retries: job.Retries+1}
		}
	}
}