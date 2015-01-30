# USE THIS TO INSTALL INTO STANDARD EC2 INSTANCE
sudo yum -y install python27
mkdir  /home/ec2-user/temp
cd  /home/ec2-user/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python27 get-pip.py

cd  /home/ec2-user
sudo yum -y install git
git clone https://github.com/klahnakoski/TestLog-ETL.git
cd /home/ec2-user/TestLog-ETL/
git checkout pulse-logger
sudo pip install -r requirements.txt

cat > pulse_logger_settings.json
# PASTE SETTINGS FILE HERE
# CTRL-D WHEN DONE
