#INSTALATION FOR UBUNTU

# REQUIRED FOR Python SSH
sudo apt-get install libffi-dev
sudo apt-get install libssl-dev
sudo pip install --upgrade requests[security]

sudo pip install pyopenssl
sudo pip ndg-httpsclient
sudo pip pyasn1

sudo pip install supervisor

cd ~
mkdir -p ~/TestLog-ETL/results/logs
sudo cp ~/TestLog-ETL/resources/supervisor/beta/supervisord.conf /etc

#START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
sudo supervisord -c /etc/supervisord.conf

sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl


tail -f /tmp/supervisord.log
