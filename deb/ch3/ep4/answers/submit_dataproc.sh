gcloud dataproc jobs submit pyspark  process_passengers.py \
--cluster=<your cluster> \
--region<your region> \
--project=<your project> \
--jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar 

# Create cluster using image version 1.4 and instaling pip packages
gcloud dataproc clusters create dln-test \
--image-version=1.4 \
--region=us-west1 \
--project=deb-sandbox \
--single-node -\
-metadata='PIP_PACKAGES=confuse>=1.3.0' \
--initialization-actions=gs://goog-dataproc-initialization-actions-us-west1/python/pip-install.sh




# Zip up the logger and config
zip -r deb_utils.zip deb/

# submit passengers
gcloud dataproc jobs submit pyspark deb/ch3/ep4/answers/passengers.py \
--cluster=dln-test \
--region=us-west1 \
--project=deb-sandbox \
--jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar \
--py-files=deb_utils.zip \
--files=config.yaml \

# Submit addresses
gcloud dataproc jobs submit pyspark deb/ch3/ep4/answers/addrs.py \
--cluster=dln-test \
--region=us-west1 \
--project=deb-sandbox \
--jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar \
--py-files=deb_utils.zip \
--files=config.yaml \

# Submit cards
gcloud dataproc jobs submit pyspark deb/ch3/ep4/answers/cards.py \
--cluster=dln-test \
--region=us-west1 \
--project=deb-sandbox \
--jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar \
--py-files=deb_utils.zip \
--files=config.yaml \
