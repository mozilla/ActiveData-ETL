# USE THIS TO INSTALL PYPY ONTO UBUNTU EC2 INSTANCE

#MAKE SWAP FOR MORE MEMORY (IN RARE CASE IT IS NEEDED
sudo dd if=/dev/zero of=/media/swapfile.img bs=4096 count=1M
sudo mkswap /media/swapfile.img
sudo sed -i '$ a\/media/fasthdd/swapfile.img swap swap sw 0 0' /etc/fstab
sudo swapon /media/swapfile.img


#DOWNLOAD PYPY
mkdir /home/ubuntu/temp
cd /home/ubuntu/temp
# wget https://bitbucket.org/pypy/pypy/downloads/pypy-2.4.0-linux64.tar.bz2
wget https://bitbucket.org/pypy/pypy/downloads/pypy-2.5.0-linux64.tar.bz2
tar xfj pypy-2.4.0-linux64.tar.bz2

# MUST GO TO DIR TO MAKE LINK
cd /usr/bin
sudo ln -s /home/ubuntu/temp/pypy-2.4.0-linux64/bin/pypy
cd /home/ubuntu/temp

wget https://bootstrap.pypa.io/get-pip.py
sudo pypy get-pip.py

cd  /home/ubuntu
sudo apt-get -y install git-core

cd  /home/ubuntu
git clone https://github.com/klahnakoski/ActiveData-ETL.git
cd /home/ubuntu/ActiveData-ETL/
git checkout etl
sudo /home/ubuntu/temp/pypy-2.4.0-linux64/bin/pip install -r requirements.txt

