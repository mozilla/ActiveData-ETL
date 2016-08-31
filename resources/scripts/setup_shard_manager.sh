#!/usr/bin/env bash




sudo yum -y install git

# INSTALL PYTHON
sudo yum -y install python27
sudo easy_install pip

# INSTALL ActiveData-ShardManager
cd  /home/ec2-user
git clone https://github.com/klahnakoski/ActiveData.git
cd /home/ec2-user/ActiveData/
git checkout better-balance
sudo /usr/local/bin/pip install -r requirements.txt


cd /home/ec2-user/ActiveData/
export PYTHONPATH=.
python27 resources/scripts/es_fix_unassigned_shards.py --settings=resources/config/fix_unassigned_shards.json



# COPY KEYS TO MACHINE
#put ~/private_active_data_etl.json ~/private_active_data_etl.json
cp ~/private_active_data_etl.json ~/private.json
chmod 600 ~/private_active_data_etl.json
chmod 600 ~/private.json

#put ~/.ssh/aws-pulse-logger.pem ~/.ssh/aws-pulse-logger.pem
chmod 600 ~/.ssh/aws-pulse-logger.pem



