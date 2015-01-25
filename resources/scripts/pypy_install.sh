# USE THIS TO INSTALL INTO UBUNTU EC2 INSTANCE

mkdir /home/ubuntu/temp
cd /home/ubuntu/temp
wget https://bitbucket.org/pypy/pypy/downloads/pypy-2.4.0-linux64.tar.bz2
tar xfj pypy-2.4.0-linux64.tar.bz2

# MUST GO TO DIR TO MAKE LINK
cd /usr/bin
sudo ln -s /home/ubuntu/temp/pypy-2.4.0-linux64/bin/pypy
cd /home/ubuntu/temp

wget https://bootstrap.pypa.io/get-pip.py
sudo pypy get-pip.py

cd  /home/ubuntu
sudo apt-get install git-core

cd  /home/ubuntu
git clone https://github.com/klahnakoski/TestLog-ETL.git
cd /home/ubuntu/TestLog-ETL/
git checkout etl
sudo /home/ubuntu/temp/pypy-2.4.0-linux64/bin/pip install -r requirements.txt

