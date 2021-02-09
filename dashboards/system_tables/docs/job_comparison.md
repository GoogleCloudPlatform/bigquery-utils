# Job Comparison Report
The Job Comparison Report gives an overview of how to compare job performance of two jobs, given two job IDs. 
This is meant to allow for a side-by-side troubleshooting for understanding why one query may have performed much slower than other.

The report allows you to compare the jobs' overall performance as well as the state of BigQuery for your organization while each job ran.

All timestamps in this report are in UTC.

This report leverages the [job comparison statistics query](../sql/job_comparison_statistics.sql) and the [concurrency query](../sql/job_concurrency_comparison.sql).

### Job ID Inputs
This report allows users to enter in two job IDs: one from a fast job and one from a slow job. Please note that these should be similar jobs, such as scheduled job run at different timeframes or jobs that read in different partitions from the same underlying source table.
When a user adds an ID, all charts and scorecards on the page will be updated to use those filters. 

Below is an example of where to input your job IDs.

### Side-by-side Job Comparison 
This table shows the output job statistics for two jobs. It displays key performance indicators, such as which reservation it used, duration time, total bytes processed, total slots used, total shuffle used, and more. 
This table allows the user to identify which metrics may be causing performance bottlenecks.

![Side-by-side Job Comparison](../images/job_comparison/side_by_side_comparison.png)

### Concurrency Comparison
This section allows a user to compare the reservation's respective state while each job ran. The user can also adjust the `time interval` dropdown to adjust how data is aggregated.

### Concurrent Active Jobs in Same Project
This time seres graph breaks down the count of active and pending jobs which executed at the same time as both the slow and fast job. Analyzing the job count at each time period can indicate changes in demand for slots, as a high concurrency can mean less slots are available for each job, therefore impacting performance.

![Job Concurrency](../images/job_comparison/concurrent_jobs.png)

### Concurrent Active Projects in Same Reservation
This time series graph breaks down the count of active projects and amount of slots available for each job. For Y projects, each project will receive 1/Y of the reservation's total slots. 
Analyzing the project count at each time frame can indicate changes in demand for slots, as a high concurrency can mean that less slots are available for each project, therefore impacting performance.


![Project Concurrency](../images/job_comparison/concurrent_projects.png)

