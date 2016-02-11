#!/usr/bin/env bash


# SEE resources/elasticsearch/setup_es_beta_ubuntu.sh

# RUN IN BACKGROUND
export ES_MIN_MEM=5g
export ES_MAX_MEM=5g
cd /usr/local/elasticsearch
sudo bin/elasticsearch -p current_pid.txt &
disown -h

