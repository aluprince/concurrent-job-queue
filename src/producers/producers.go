package producer

import (
	//"log"
	"time"
)



type Job struct {
    ID       int
    Duration time.Duration
}


// Generating Job
func GenerateJobs(NumberOfJobs int, DurationSec int) []Job {
	//log.Printf(">>Producing Jobs")

	duration := time.Duration(DurationSec) * time.Microsecond
	jobList := make([]Job, 0, NumberOfJobs)

	for index := range NumberOfJobs{
		jobs := Job{index, duration}
		jobList = append(jobList, jobs)	
	}
	
	return jobList
}