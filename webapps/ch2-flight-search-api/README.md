# Flight Schedule API

Series of REST APIs to retrieve future flight schedules used in chapter 2.

### Install VirtualEnv 

Before running the Flask app, you must create or use the project VirtualEnv.

You can re-use the project level VirtualEnv with `deb-airliner-internal/requirements.txt` OR create a new VirtualEnv here and pip install packages from
this `deb-airliner-internal/webapps/flights-api/requirements.txt`:

```bash
# create a new virtualenv (or use the exsiting one from your project)
cd deb-airliner-interl/webapps/flights-api
virtualenv -p python3 --no-site-packages venv
source vnev/bin/activate
# update your pip and setuptools
piip install --upgrade pip
pip install --upgrade setuptools
# install all packages
pip install requirements.txt
```


## Google Cloud Credentials

Before running this application you must setup download and setup the Google Cloud service account. 

We will create a _/creds_ directory to save all the credential files. The credential files are used by Google Cloud SK and python client
libraries (used in flask apps) to gain access to GCP resources such as Firestore, GS, AppEngine, etc...

To setup GCP Service account to be used by python clients:

1. Obtain the service account __KEY FILE__ `deb-airliner-a9b584c1a09c.json` and save it onto `deb-airliner/webapps/credentials/`
1. If you don't have the key file, you can email [Parham](mailto:parham@turalabs.com) or download it from Google Storage (below)
1. Run the following commands:

```bash
# download gcp key file
cd deb-airliner-internal/webapps/flights-api
mkdir creds
cd creds
gsutil cp gs://deb-airline-data/creds/deb-airliner-a9b584c1a09c.json .
chmod 400 deb-airliner-a9b584c1a09c.json

# make sure key file is accessible to client apps
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/deb-airliner-a9b584c1a09c.json"
```

You must make sure __GOOGLE_APPLICATION_CREDENTIALS__ environment variable correctly points to your credential file. This is the most 
important part.


## Run

To run the application you must
1. Install or activate your python virtualenv and install all packages in requirements.txt ([above](#install-virtualenv))
1. Setup your google account credential file ([above](#google-cloud-credentials))
1. Download __flights.db__ sqlite3 database
1. Set environment variables for flask
1. Run the app

```bash
cd deb-airliner-interl/webapps/flights-api
gsutil cp gs://deb-airline-data/utils/flights.db .
export FLASK_APP=run.py
flask run
```

The REST API now should be available on _`http://localhost:5000`_. To test you can run:

```bash
curl -X GET http://localhost:5000/version -H 'Accept: application/json'

curl -X GET 'http://localhost:5000/flights?date=2020-05-01&airline=DL' -H 'Accept: application/json'
```

# API Documentation

You can access the API on http://localhost:5000

See [postman documentation](https://documenter.getpostman.com/view/5437810/Szt5fWwP) for full list of REST API methods available

[Download](https://www.getpostman.com/collections/90e85b84d469234587b1) Postman collection to use the API.

__NOTE:__ All flights routes use Auth Bearer Authorization. You must provide your API key using the `Authorization: Bearer <KEY>` header.


### Remaining Tasks

- Add flight delays and ranking
