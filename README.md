How to run:
----------

Setup configuration file:

    cp ipd.ini.sample ipd.ini
    $EDITOR ipd.ini

See comments for required configuration fields to modify.

Virtualenv
----------

    virtualenv venv
    source venv/bin/activate
    pip install -r requirements.txt
    python MqttIpdPublish.py

