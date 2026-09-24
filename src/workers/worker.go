package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/aluprince/concurrent-job-queue/src/producers"
)

func Worker(id int, jobs <-chan producer.Job, results chan<- producer.JobResult, workers *sync.WaitGroup) {
	defer workers.Done()

	for job := range jobs {
		if !job.Fail {
			fmt.Printf("Worker #%d started job #%d\n", id, job.ID)
			time.Sleep(2 * time.Second)
			fmt.Printf("Worker #%d finished job #%d\n", id, job.ID)
			results <- producer.JobResult{JobID: job.ID, Success: true}
		} else {
			fmt.Printf("Worker #%d FAILED job #%d\n", id, job.ID)
			results <- producer.JobResult{JobID: job.ID, Success: false}
		}
	}
}
