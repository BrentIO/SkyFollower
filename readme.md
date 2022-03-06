# SkyFollower
Track, store, and alert on aircraft movement from an ADS-B locally.


## What Does it Do?
SkyFollower reads data from your ADS-B output using either Beast or raw message output, enriches that data by calling an external web service for registration and aircraft information, sends notifications about tracked aircraft movements using a rules-based engine.  All of the output is stored in MongoDB for any other use you might have in a simple, easy-to-query format.

Data is stored in memory until the signal has been lost.

What doesn't it do?  Mapping, real-time statistics, or pretty much anything else not listed above.




## Prerequisites
SkyFollower can run on pretty much any operating system that runs Python, but this document will be focused exclusively on explaining how to do so under Linux.

### MongoDB
MongoDB is a document-based datastore and is perfect for operations like tracking aircraft.  While the aircraft is actively being tracked, the data is stored in memory.  It is recommended, but not required, for MongoDB to be installed on the same system as SkyFollower.

> MongoDB 4.4 Community Edition is recommended.  Follow their [installation instructions](https://docs.mongodb.com/v4.4/installation/).  Installing MongoDB later than 4.4 may not be possible on your computer, depending on the age and processor capabilities, and has not been tested.


### Python 3
This should be installed by default with your Linux distribution.  If not:
```
sudo apt-get install python3 python3-pip
```

You can also check to ensure you have Python installed:
```
python3 --version
```

### Python Packages
Prior to running SkyFollower you must install several dependent Python packages:
```
sudo pip3 install tinydb pyModeS pymongo requests paho-mqtt
```

### Aircraft Registration
To store information about the operator and registration, download and configure the [Aircraft Registration Import](https://github.com/BrentIO/Aircraft-Registration-Import).

## Required Configuration
Download SkyFollower
```
sudo git clone https://github.com/BrentIO/SkyFollower.git /etc/P5Software/SkyFollower
```


## Required Configuration
You *must* configure these settings in the settings.json file.

```
sudo nano /etc/P5Software/SkyFollower/settings.json
```

| Parameter     | Default   | Description |
----------------|-----------|-------------|
| `latitude` | 38.8969137 | Latitude of the receiver |
| `longitude` | -77.0357096 | Longitude of the receiver |
| `adsb -> uri` | my.adsb.server.lan | IP address or domain name of your ADS-B receiver.|



## Optional Configuration
The settings.json file contains all of the user-configurable settings for SkyFollower.  In addition to those in the Required Configuration section, optional parameters are described below.

| Parameter     | Default   | Description |
----------------|-----------|-------------|
| `log_level`| info | Amount of logging that should be written to the text log file. Options are `debug`, `error`, `warning`, `critical`. If using `debug`, be mindful that this will cause significant writes, and is intended for debugging purposes only.|
| `flight_ttl_seconds` | 300 | The number of seconds after loss of signal that the flight will be concluded and moved to MongoDB.  Configuring too low will cause fragmentation of data; too high will cause erroneous data, such as a quick turn on an aircraft. Any positive integer is permitted.|
| `local_database_mode` | memory | Determines if the cached database is stored in memory or disk.  Options are `disk` or `memory`.  If using disk, be mindful that this will cause significant writes, and is intended for debugging purposes only.|
| `adsb -> port` | 30002 | The port of your ADS-B receiver where the data can be found.  Use `30002` when setting the `adsb -> type` to `raw`.  Use `30005` when setting the `adsb -> type` to `beast`.|
| `adsb -> type` | raw | The data format your ADS-B receiver is sending to SkyFollower.  Use `raw` when setting the `adsb -> port` to `30002`.  Use `beast` when setting the `adsb -> port` to `30005`.|
| `mongoDb -> database`| SkyFollower | Name of the MongoDB database to store the flight data.|
| `mongoDb -> collection`| flights | Name of the document collection in the database to store the flight data.|
| `mqtt` |  If this object is omitted, MQTT will be disabled.|
| `mqtt -> uri` | my.mqtt.server.lan | IP address or domain name of your MQTT broker. :information_source: Coming soon.|
| `mqtt -> port` | 1883 | Port for your MQTT broker. :information_source: Coming soon.|
| `mqtt -> username` | my_username | Username to use when authenticating to your MQTT broker. :information_source: Coming soon.|
| `mqtt -> password` | my_clear_text_password | Password to use when authenticating to your MQTT broker. :information_source: Coming soon.|
| `mqtt -> statusTopic` | SkyFollower/status | MQTT topic used to announce SkyFollower service status.|
| `mqtt -> notificationTopic` | SkyFollower/notify | MQTT topic used to notify about aircraft which meet the monitoring criteria. :information_source: Coming soon.|
| `mongoDb -> uri` | localhost | URL for the MongoDB server.  You should only need to change this if you are not running SkyFollower and MongoDB on the same device.|
| `mongoDb -> port`| 27017 | Port for your MongoDB server.  You should only need to change this if you did a custom installation of MongoDB and specifically changed the port binding.|
| `mongoDb -> database`| flights | Name of the MongoDB database to store the flight data.|


## Installation

Copy the service file to the systemctl directory
```
sudo mv /etc/P5Software/SkyFollower/SkyFollower.service /lib/systemd/system/
```

Reload the systemctrl daemon to pick up the new SkyFollower service
```
sudo systemctl daemon-reload
```

Enable the service
```
sudo systemctl enable SkyFollower.service
```

Start the service
```
sudo systemctl start SkyFollower.service
```


## FAQ
- Can I run this on Raspberry Pi?
  - Sure, but I haven't tried it.  MongoDB will do a _lot_ of file I/O operations, so if you're running this from an SD card, expect it to die pretty quickly.  If you run MongoDB on a separate computer with either SSD or traditional hard drives, your experience will be much better.
  - Data is stored in memory while SkyFollower is actively tracking the aircraft.  If you have a lot of traffic in your area, this may also saturate the limited memory in a Raspberry Pi.
- Does this work with what ADS-B receivers is this compatible with?
  - Most ADS-B receivers will output either a `Beast` or `Raw` format, including FlightAware's SkyAware and PlaneFinder's PlaneFinderClient.  Running this software does not interfere with those services and can live happily beside them.
- Do I need to know about database management to make this work?
  - Nope.
- How do you differentiate between a loss of signal and a different flight?
  - The setting `flight_ttl_seconds` defines the number of seconds since loss-of-signal where the flight is determined done and the data is then persisted to MongoDB.  The default period of time to consider the signal lost and move the flight data to MongoDB is *300 seconds*.
- What happens when my computer shuts down or I stop the SkyFollower service?
  - The ADS-B listener is stopped and data stored in memory is immediately written to MongoDB.  Restarting SkyFollower immediately will likely cause a duplication of the flight information, since SkyFollower doesn't retroactively merge documents in MongoDB.
- What doesn't work?
  - `To Do`: MQTT notifications engine



## Credits
- [PyModeS](https://github.com/junzis/pyModeS), which powers the decoding of the native ADS-B messages