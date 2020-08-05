## Google Cloud Credentials

This directory contains GCP Service Account credentials. The credential files are used by Google Cloud SK and python clinet
libraries (used in flask apps) to gain access to GCP resources such as Firestore, GS, AppEngine, etc...

### Setup GCP Service Account

To setup GCP Service account to be used by python clients:

1. Obtain the service account __KEY FILE__ `deb-airliner-a9b584c1a09c.json` and save it onto `deb-airliner/webapps/credentials/`
1. If you don't have the key file, you can email [Parham](mailto:parham@turalabs.com) or download it from Google Storage (below)
1. Run the following commands:

```bash
# download gcp key file
cd deb-airliner-internal/webapps/credentials
gsutil cp gs://deb-airline-data/creds/deb-airliner-a9b584c1a09c.json .
chmod 400 deb-airliner-a9b584c1a09c.json

# make sure key file is accessible to client apps
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/deb-airliner-a9b584c1a09c.json"

# make sure gcp client apps are up-to-date
# load VirtualEnv before this
# update your pip and setuptools
piip install --upgrade pip
pip install --upgrade setuptools
pip install --upgrade google-cloud-firestore
``` 