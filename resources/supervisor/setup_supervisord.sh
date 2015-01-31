sudo apt-get install -y supervisor
sudo service supervisor start

cd /home/ubuntu
sudo cp etl.conf /etc/supervisor/conf.d/

sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl
