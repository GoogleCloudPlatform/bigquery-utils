UPDATE `nikunjbhartia-test-clients.bqtestdataset.job_stats_nonpartitioned`
SET updation_ts = CURRENT_TIMESTAMP()
    status = 'Updated'
WHERE id = @id