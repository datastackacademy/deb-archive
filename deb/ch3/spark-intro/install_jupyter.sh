#!/bin/bash

# install and run a virtualenv
python3.7 -m venv venv
source venv/bin/activate

# install jupyter notebook
pip install jupyterlab
pip install notebook

# install and setup jupyter themes
pip install jupyterthemes

# change default theme and fonts
jt -t onedork -T -tf sourcesans -nf sourcesans -tfs 12

# install pandas
pip install pandas pyarrow pandas-gbq

# install pyspark
pip install pyspark

# to start jupyter server:
# jupyter notebook
