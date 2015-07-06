
TestLog-ETL
===========

The ETL process responsible for filling ActiveData.

Instructions for installation are deliberately omitted until development slows down. 

Branches
--------

Each of the branches are meant as stable versions for each of the proceses involved in the ETL. Here are the important branches:


* **dev** - unstable - primary branch for accepting changes
* **etl** - stable - for ETL machines
* **pulse-logger** - stable - for the PulseLogger
* **push-to-es** - stable - code installed on ES machines for final indexing. 
* **beta** - stable - of all branches for testing on the beta machines 
* **master** - unstable - intermittently updated to track **dev**, eventually intended as the single-stable-version 
