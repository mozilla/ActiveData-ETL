TestLog-ETL
===========

The ETL process responsible for filling ActiveData.

Branches
--------

Many branches are meant as stable versions for each of the processes involved 
in the ETL.  Ideally, they would be unified, but library upgrades can cause 
unique instability: deployment of a branch does not happen until (manual) 
testing has been done.

Here are the important branches:

* **dev** - unstable - primary branch for accepting changes
* **etl** - stable - for ETL machines
* **primary** - stable - for the "primary" and "coordinator" ES nodes 
* **pulse-logger** - stable - for the PulseLogger
* **push-to-es** - stable - code installed on ES spot instance machines for 
final indexing. 
* **beta** - stable - of all branches for testing on the beta machines 
* **master** - unstable - intermittently updated to track **dev**, eventually 
intended as the single-stable-version 


Requirements
------------

* Python 2.7.x 
* [Elasticsearch 1.7.x](https://www.elastic.co/downloads/past-releases/elasticsearch-1-7-5)  (the current 2.x versions are not supported yet)
* Access to Amazon S3 bucket for ETL results
* Access to Amazon SQS for the ETL pipeline


Installing Fabric 
-----------------

It is 2016, and Python is still hard on Windows.  It would be [a nice question for Stack Overflow](http://stackoverflow.com/questions/9000380/install-python-fabric-on-windows), but apparently complex installation procedures are

1. [Install Python, and PIP](https://github.com/klahnakoski/pyLibrary#windows-7-install-instructions-for-python)
2. `pip install fabric` - There will be errors 

 8
down vote
accepted
	

I have just managed to install fabric on win7 box, using information from various places in the net. That was really annoying, so just to save others frustration I put together the following list.

    Install pip http://www.pip-installer.org/en/latest/index.html (that's easy, follow the guide on the web site, goes without problems)
    run from command line pip install fabric - you'll get errors, some stuff about missing bat files, etc. while installing pycrypto.
    Download precompiled pycrypto package from http://www.voidspace.org.uk/downloads/ for your version of Python, for instance for 2.7 it is http://www.voidspace.org.uk/downloads/pycrypto-2.1.0.win32-py2.7.zip
    run again from command line pip install fabric - this time everything seems to be ok, until you try to run fabric script. It will complain about "No module named win32api", etc.
    Install that missing win32api from http://sourceforge.net/projects/pywin32/files/pywin32/ - first choose a build version (newest, probably) and then again .exe file for your Python version.

And you are done, fabric should work ok.
