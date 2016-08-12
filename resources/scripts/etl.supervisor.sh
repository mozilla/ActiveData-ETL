cd /home/ubuntu/Activedata-ETL/
git checkout etl
git stash clear
git stash
git pull origin etl
git stash apply

sudo cp /home/ubuntu/Activedata-ETL/resources/supervisor/etl.conf /etc/supervisor/conf.d/

sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl restart all
sudo supervisorctl
tail -f etl:00
