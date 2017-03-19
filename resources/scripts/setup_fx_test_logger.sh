# USE THIS TO INSTALL INTO STANDARD EC2 INSTANCE
sudo yum -y install python27
mkdir  /home/ec2-user/temp
cd  /home/ec2-user/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python27 get-pip.py

cd  /home/ec2-user
sudo yum -y install git
git clone https://github.com/klahnakoski/ActiveData-ETL.git
cd /home/ec2-user/ActiveData-ETL/
git checkout fx-test-logger

sudo python27 /usr/local/lib/python2.7/site-packages/pip/__init__.py install requests
sudo python27 /usr/local/lib/python2.7/site-packages/pip/__init__.py install boto
sudo python27 /usr/local/lib/python2.7/site-packages/pip/__init__.py install BeautifulSoup

echo '{"aws_credentials":{"region":"us-west-2"}}' > /home/ec2-user/private.json

chmod a+x /home/ec2-user/ActiveData-ETL/resources/scripts/fx-test-logger.sh

sudo -i
echo "10 * * * * /home/ec2-user/ActiveData-ETL/resources/scripts/fx-test-logger.sh" > /var/spool/cron/ec2-user
exit
