Settings Files
==============

These are the config files for various environments and functionality.  They
all assume you have a `private.json` file in your home directory with the
following structure:

    {
        "email":{
            "host": "mail.mozilla.com",
            "port": 465,
            "username": "",
            "password": "",
            "use_ssl": 1
        },
        "aws_credentials":{
            "aws_access_key_id":"",
            "aws_secret_access_key" :"",
            "region":"us-west-2"
        },
        "pulse_user":{
            "user": "",
            "password": ""
        }
    }
