
## Google SDK commands to copy flights dataset to your GS Bucket

# list our flights bucket
gsutil ls -lhr  gs://deb-airline-data/bots/csv

# change your project
gcloud config set project <PROJECT>
# create a GS bucket
gsutil mb -c standard -l us-west2 <YOUR_BUCKET>
# copy our flights dataset to your bucket
gsutil -m cp -r gs://deb-airline-data/bots/csv <YOUR_BUCKET>/bots/csv

# download one of the flights files and inspect it
cd /tmp
gsutil cp gs://<YOUR_BUCKET>/bots/csv/2019/flights_2019_01.csv .
cat flights_2019_01.csv | wc -l
head -n 10 flights_2019_01.csv
