sudo apt-get install -y supervisor

sudo service supervisor start

cd /home/ubuntu
mkdir -p /home/ubuntu/TestLog-ETL/results/logs

sudo cp /home/ubuntu/TestLog-ETL/resources/supervisor/staging/etl.conf /etc/supervisor/conf.d/

sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl
