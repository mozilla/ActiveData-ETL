# USE THIS TO INSTALL INTO STANDARD EC2 INSTANCE
sudo yum -y install python27
rm -fr /home/ec2-user/temp
mkdir  /home/ec2-user/temp
cd  /home/ec2-user/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python27 get-pip.py
sudo ln -s /usr/local/bin/pip /usr/bin/pip

cd  /home/ec2-user
sudo yum -y install git
git clone https://github.com/klahnakoski/TestLog-ETL.git
cd /home/ec2-user/TestLog-ETL/
git checkout push-to-es
sudo pip install -r requirements.txt
