"""
Loads YAMl configuration from a file in deb root directory called: deb/config.yaml
Add configuration parameters for your programs to the file. You can read your configuration as a dict.

This module uses pypi confuse module to load yaml configuration. For more information please refer to confuse documentation:
https://confuse.readthedocs.io/en/latest

Usage:

For a config.yaml as:
logging:
   level: INFO

# get logging level
config['logging']['level']


Author: Par (turalabs.com)
Contact:

license: GPL v3 - PLEASE REFER TO DEB/LICENSE FILE
"""

import confuse
import os


# setting up config directory for confuse search path
# by default confuse will load config.yaml from the home directory now
os.environ['DEBDIR'] = os.getcwd()
config = confuse.Configuration("deb", __name__)
