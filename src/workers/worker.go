package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/aluprince/concurrent-job-queue/src/producers"
)

func Worker(id int, jobs <-chan producer.Job, workers *sync.WaitGroup) {
	defer workers.Done()

	for job := range jobs {
		//Simulate Work
		fmt.Printf("Worker #%d started job #%d\n", id, job.ID)

		time.Sleep(2 * time.Second)

		fmt.Printf("Worker #%d finished job #%d\n", id, job.ID)

	}
}
