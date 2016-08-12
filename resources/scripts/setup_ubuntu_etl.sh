# USE THIS TO INSTALL PYTHON 2.7 ONTO UBUNTU EC2 INSTANCE
# 2.7 IS ALREADY DEFAULT


mkdir  /home/ubuntu/temp
cd  /home/ubuntu/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py

# WE REQUIRE psycopg2 FOR REDSHIFT
# BUT THE AMAZON IMAGE DOES NOT LOAD IT PROPERLY; UPDATE
sudo apt-get update
sudo apt-get -y install python-psycopg2


cd  /home/ubuntu
sudo apt-get -y install git-core


git clone https://github.com/klahnakoski/Activedata-ETL.git

cd /home/ubuntu/Activedata-ETL/
git checkout etl
sudo pip install -r requirements.txt

