Year,Institutional_sector_name,Institutional_sector_code,Descriptor,SNA08TRANS,Asset_liability_code,Status,Values

export GOOGLE_APPLICATION_CREDENTIALS=/Users/nikunjbhartia/projects/sharechat/dataflow-jobs-templates/credentials/service_account.json
cat $GOOGLE_APPLICATION_CREDENTIALS
cbt createtable financial_data_v2
cbt createfamily financial_data_v2 fd
cbt createfamily financial_data_v2 fd2
cbt createfamily financial_data_v2 fd3


cbt set financial_data_v2 r1 fd:c1="test-value1"
cbt set financial_data_v2 r1 fd:c2="test-value2"
cbt set financial_data_v2 r2 fd2:c3="test-value3"
cbt set financial_data_v2 r2 fd2:c4="test-value4"


cbt set financial_data_v2 r3 fd3:c5="test-value2"
cbt set financial_data_v2 r4 fd3:c6="test-value2"
cbt set financial_data_v2 r5 fd2:c7="test-value2"
cbt set financial_data_v2 r6 fd2:c8="test-value2"

cbt read financial_data_v2

mvn package exec:exec \
    -DCsvImport \
    -Dbigtable.projectID=nikunjbhartia-mycloud \
    -Dbigtable.instanceID=bigtable-us-instance \
    -DinputFile="gs://us_bucket_nikunj/sample_financial.csv" \
    -Dheaders="Year,Institutional_sector_name,Institutional_sector_code,Descriptor,SNA08TRANS,Asset_liability_code,Status,Values" \
    -Dbigtable.table="financial_data_v2"


mvn package exec:exec \
    -DCsvImport \
    -Dbigtable.projectID=nikunjbhartia-mycloud \
    -Dbigtable.instanceID=bigtable-us-instance \
    -DinputFile="gs://us_bucket_nikunj/sample.csv" \
    -Dheaders="key,a,b" \
    -Dbigtable.table="financial_data_v2"


mvn package exec:exec \
    -DBigQueryBigtableTransfer \
    -Dbigtable.projectID=nikunjbhartia-mycloud \
    -Dbigtable.instanceID=bigtable-us-instance \
    -Dgs="gs://us_bucket_nikunj/" \
    -Dbq.query='SELECT name, age, quantity,timestamp, customer_id, customer_name FROM `nikunjbhartia-mycloud.datafusion_demo.denormalized_purchases` LIMIT 1000' \
    -Dbigtable.table="financial_data_v2"