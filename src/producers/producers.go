package producer

import (
	//"log"
	"time"
	"math/rand/v2"
)

const failRate int = 20


type Job struct {
    ID       int
    Duration time.Duration
	Fail bool
}

type JobResult struct {
	JobID int
	Success bool
	Retries int
}


// Generating Job
func GenerateJobs(NumberOfJobs int, DurationSec int) []Job {
	
	duration := time.Duration(DurationSec) * time.Microsecond
	jobList := make([]Job, 0, NumberOfJobs)

	for index := range NumberOfJobs{
		randomBoolValue := rand.IntN(100) < failRate
		jobs := Job{index, duration, randomBoolValue}
		jobList = append(jobList, jobs)	
	}
	
	return jobList
}