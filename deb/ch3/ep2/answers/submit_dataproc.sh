gcloud dataproc jobs submit pyspark  process_passengers.py \
--cluster=<your cluster> \
--region<your region> \
--project=<your project> \
--jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar 