#!/bin/bash

# bootstrap commands
sudo yum install -y python27 python27-devel python27-pip gcc-c++ git libffi-devel
sudo pip2.7 install boto3 mrjob simplejson pywb
