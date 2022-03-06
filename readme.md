# SkyFollower
Track, store, and alert on aircraft movement from an ADS-B locally.

## What Does it Do?
SkyFollower reads data from your ADS-B output using either Beast or raw message output, enriches that data by calling an [external web service](https://github.com/BrentIO/Aircraft-Registration-Import) for registration and aircraft information, and sends notifications about tracked aircraft movements using a rules-based engine.  All of the output is stored in MongoDB for any other use you might have in a simple, easy-to-query format.

## How does it Work?
As your ADS-B receiver publishes data, SkyFollower reads that data into a local in-memory database.  As messages are sent by the aircraft, a profile of the flight is built using the local, in-memory data.  Data is stored in-memory until the signal has been lost and then persisted to MongoDB.  If the service shuts down gracefully, the data is also persisted.



## Prerequisites
SkyFollower can run on pretty much any operating system that runs Python3, but this document will be focused exclusively on explaining how to do so under Ubuntu 20.04 (Focal Fossa).

### MongoDB
MongoDB is a document-based datastore and is perfect for operations like tracking historical aircraft movement.  It is recommended, but not required, for MongoDB to be installed on the same system as SkyFollower.

> MongoDB 4.4 Community Edition is recommended.  Follow their [installation instructions](https://docs.mongodb.com/v4.4/installation/).  Installing MongoDB later than 4.4 may not be possible on your computer, depending on the age and processor capabilities, and has not been tested.

### Required packages
Using apt, install the prerequisite packages, if you don't already have them installed:
```
sudo apt-get install -y python3 python3-pip tinydb pyModeS pymongo requests paho-mqtt
```

### Optional: Aircraft Registration and Operator Information
This step is optional, but SkyFollower can retrieve aircraft registration and operator information to enrich your data. This can be helpful if you want to trigger alerts about specific operators or registration numbers.  To do so, download and configure the [Aircraft Registration Import](https://github.com/BrentIO/Aircraft-Registration-Import).

## Download and Install SkyFollower
Clone SkyFollower from GitHub:
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
| `latitude` | 38.8969137 | Latitude of the receiver | <!--- 'Yeah, that's the White House' --->
| `longitude` | -77.0357096 | Longitude of the receiver | <!--- 'Yeah, that's the White House' --->
| `adsb -> uri` | my.adsb.server.lan | IP address or domain name of your ADS-B receiver.|



## Optional Configuration
The settings.json file contains all of the user-configurable settings for SkyFollower.  In addition to those in the Required Configuration section, optional parameters are described below.

| Parameter     | Default   | Description |
----------------|-----------|-------------|
| `log_level`| info | Amount of logging that should be written to the text log file. Options are `debug`, `error`, `warning`, `critical`. If using `debug`, be mindful that this will cause significant writes, and is intended for debugging purposes only.  If omitted, `info` is used.|
| `flight_ttl_seconds` | 300 | The number of seconds after loss of signal that the flight will be concluded and moved to MongoDB.  Configuring too low will cause fragmentation of data; too high will cause erroneous data, such as a quick turn on an aircraft. Any positive integer is permitted.|
| `local_database_mode` | memory | Determines if the cached database is stored in memory or disk.  Options are `disk` or `memory`.  If using disk, be mindful that this will cause significant writes, and is intended for debugging purposes only.|
| `adsb -> port` | 30002 | The port of your ADS-B receiver where the data can be found.  Use `30002` when setting the `adsb -> type` to `raw`.  Use `30005` when setting the `adsb -> type` to `beast`.|
| `adsb -> type` | raw | The data format your ADS-B receiver is sending to SkyFollower.  Use `raw` when setting the `adsb -> port` to `30002`.  Use `beast` when setting the `adsb -> port` to `30005`.|
| `mongoDb -> uri` | localhost | URL for the MongoDB server.  You should only need to change this if you are not running SkyFollower and MongoDB on the same device.|
| `mongoDb -> port`| 27017 | Port for your MongoDB server.  You should only need to change this if you did a custom installation of MongoDB and specifically changed the port binding.|
| `mongoDb -> database`| SkyFollower | Name of the MongoDB database to store the flight data.|
| `mongoDb -> collection`| flights | Name of the document collection in the database to store the flight data.|
| `registration` | If this object is ommitted, retrieving registration data will be disabled.  Useful for converting the ICAO hex code to a registration object.  For example, querying `A8AE7F` would return to an object for a Boeing 757.  |
| `registration -> enabled` | false | Controls if retrieving registration data is enabled or disabled.|
| `registration -> uri` | http://localhost/api/registration?icao_hex=$ICAO_HEX$ | The URL of the webserver where the registration microservice runs.  Note, $ICAO_HEX$ will subsitute the ICAO hex code of the aircraft. |
| `registration -> x-api-key` | some_secret_key | The value of the x-api-key header that should be sent for authenticating the request to the service.|
| `operators` | If this object is ommitted, retrieving operator data will be disabled.  Useful for converting aircraft callsigns to operators.  For example, querying for `DAL` would return an object for Delta Air Lines.|
| `operators -> enabled` | false | Controls if retrieving operator data is enabled or disabled.|
| `operators -> uri` | http://localhost/api/operator?airline_designator=$CALLSIGN$ | The URL of the webserver where the operators microservice runs.  Note, $CALLSIGN$ will subsitute the ICAO callsign of the aircraft. |
| `operators -> x-api-key` | some_secret_key | The value of the x-api-key header that should be sent for authenticating the request to the service.|
| `mqtt` |  If this object is omitted, MQTT will be disabled.|
| `mqtt -> enabled` | false | Controls if MQTT is enabled or disabled.|
| `mqtt -> uri` | my.mqtt.server.lan | IP address or domain name of your MQTT broker. :information_source: Coming soon.|
| `mqtt -> port` | 1883 | Port for your MQTT broker. :information_source: Coming soon.|
| `mqtt -> username` | my_username | Username to use when authenticating to your MQTT broker. :information_source: Coming soon.|
| `mqtt -> password` | my_clear_text_password | Password to use when authenticating to your MQTT broker. :information_source: Coming soon.|
| `mqtt -> statusTopic` | SkyFollower/status | MQTT topic used to announce SkyFollower service status.|
| `mqtt -> notificationTopic` | SkyFollower/notify | MQTT topic used to notify about aircraft which meet the monitoring criteria. :information_source: Coming soon.|



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
  - There will be changes needed to the installation above, but it should work.
  - MongoDB will do a _lot_ of file I/O operations, so if you're running this from an SD card, expect the SD card to die pretty quickly.  If you run MongoDB on a separate computer with an SSD/NVMe/traditional hard drive, your experience will be much better and it should be fine.
  - Data is stored in memory while SkyFollower is actively tracking the aircraft.  If you have a lot of traffic in your area, this may also saturate the limited memory in a Raspberry Pi and cause issues for SkyFollower or other applications.
- What ADS-B receivers is this compatible with?
  - Most ADS-B receivers will output either a `Beast` or `raw` format, including FlightAware's SkyAware and PlaneFinder's PlaneFinderClient.  Running this software does not interfere with those services and can live happily beside them.
- Do I need to know about database management to make this work?
  - No, but you might want to learn how to query MongoDB to add additional features or get information that interests you.
- How do you differentiate between a loss of signal and a different flight?
  - The setting `flight_ttl_seconds` defines the number of seconds since loss-of-signal where the flight is determined done and the data is then persisted to MongoDB.
- What happens when my computer shuts down or I stop the SkyFollower service?
  - The ADS-B listener is stopped and data stored in memory is immediately written to MongoDB.  Restarting SkyFollower immediately will likely cause a duplication of the flight information, since SkyFollower doesn't retroactively merge documents in MongoDB.
- Is this going to break my map or other tools that I currently use?
  - SkyFollower listens to the same stream of data that your other tools do, and therefore should not interfere with those functions.  Custom maps, real-time statistics, and other neat add-on's that you might be using will continue to work unaffected.  Note, however, that if you install the [Aircraft Registration Import](https://github.com/BrentIO/Aircraft-Registration-Import), it is possible this could conflict with the port your map uses, so read those docs carefully.
- What promised features haven't been implemented yet?
  - `To Do`: MQTT notifications engine



## Credits
- [PyModeS](https://github.com/junzis/pyModeS), which powers the decoding of the native ADS-B messages