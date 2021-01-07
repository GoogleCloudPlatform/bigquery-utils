#!/usr/bin/env bash

~/.local/apache-jmeter-5.3/bin/jmeter -n -l /tmp/sample_jmeter_log \
-t BigQuery-BI-and-ELT.jmx \
-Jproject_id=${PROJECT_ID} \
-Jpdt_csv_path=pdt_queries.csv \
-Jbq_public_csv_path=bigquery_public_data_bi_queries.csv \
-Jerror_csv_path=errors.csv \
-Jpdt_num_users=1 \
-Jbq_public_num_users=5 \
-Jnum_loops=1 \
-Jnum_slots=${SLOT_COUNT:=2000} \
-Jrun_id="test_run_$(date +%s)" \
-Jthread_duration=60 \
-Jramp_time=0;
