# SkyFollower
Track, store, and alert on aircraft movement from an ADS-B locally.

## What Does it Do?
SkyFollower reads data from your ADS-B output using either Beast or raw message output, enriches that data by calling an [external web service](https://github.com/BrentIO/Aircraft-Registration-and-Operator-Information) for aircraft registration and operator information, and sends notifications about tracked aircraft movements using a rules-based engine.  All of the output is stored in MongoDB for any other use you might have in a simple, easy-to-query format.

If you're using [Home Assistant](https://www.home-assistant.io/) for home automation, it can also raise events that can be used to drive other actions in your home automation, like announcing an aircraft passing overhead.

## How does it Work?
As your ADS-B receiver publishes data, SkyFollower reads that data into a local in-memory database.  As messages are received from the aircraft, a profile of the flight is built using the in-memory data until a loss of signal occurs with the tracked aircraft for a period of time at which point the flight is considered completed.  Completed flights are persisted to MongoDB.

If the service exits, the in-memory data is persisted to MongoDB immediately.


## Prerequisites
SkyFollower can run on pretty much any operating system that runs Python 3, but this document will be focused exclusively on explaining how to do so under Ubuntu 20.04 (Focal Fossa).

### MongoDB
MongoDB is a document-based datastore and is perfect for operations like tracking historical aircraft movement.  It is recommended, but not required, for MongoDB to be installed on the same system as SkyFollower.

> MongoDB 4.4 Community Edition is recommended.  Follow their <a href="https://www.mongodb.com/docs/v4.4/tutorial/install-mongodb-on-ubuntu/" target="_blank">installation instructions</a>.  Installing MongoDB later than 4.4 may not be possible on your computer, depending on the age and processor capabilities, and has not been tested.

### Allow External Connections to MongoDB
_Only perform this operation if you intend to query MongoDB from a separate computer._ Set the network binding to allow remote connections.  

```
sudo nano /etc/mongod.conf
```

```
net:
  port: 27017
  bindIp: 0.0.0.0
  ```

Restart mongo

```
sudo systemctl restart mongod
```

### Required packages
Using apt, install the prerequisite packages, if you don't already have them installed:
```
sudo apt-get install -y python3 python3-pip libgeos-dev
```

Python also requires a number of packages that must be installed to use SkyFollower:
```
sudo pip3 install tinydb pyModeS pymongo requests paho-mqtt shapely watchdog schedule
```

### Aircraft Registration and Operator Information
This step is optional, but SkyFollower can retrieve aircraft registration and operator information to enrich your data. This can be helpful if you want to trigger alerts about specific operators or registration numbers.  To do so, download and configure the [Aircraft Registration and Operator Information](https://github.com/BrentIO/Aircraft-Registration-and-Operator-Information).

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
| `flight_ttl_seconds` | 300 | The number of seconds after loss of signal until the flight will be concluded and the data persisted to MongoDB.  Configuring this option too low will cause fragmentation of data for long overhead flights with short losses of signal; too high will cause erroneous data where two flights are merged into one long flight, such as a quick turn on an aircraft at a nearby airport. Any positive integer is allowed.|
| `local_database_mode` | memory | Determines if the cached database is stored in memory or disk.  Options are `disk` or `memory`.  If using disk, be mindful that this will cause significant writes, and is intended for debugging purposes only.|
| `adsb -> port` | 30002 | The port of your ADS-B receiver where the data can be found.  Use `30002` when setting the `adsb -> type` to `raw`.  Use `30005` when setting the `adsb -> type` to `beast`.|
| `adsb -> type` | raw | The data format your ADS-B receiver is sending to SkyFollower.  Use `raw` when setting the `adsb -> port` to `30002`.  Use `beast` when setting the `adsb -> port` to `30005`.|
| `mongoDb -> uri` | localhost | URL for the MongoDB server.  You should only need to change this if you are not running SkyFollower and MongoDB on the same device.|
| `mongoDb -> port`| 27017 | Port for your MongoDB server.  You should only need to change this if you did a custom installation of MongoDB and specifically changed the port binding.|
| `mongoDb -> database`| SkyFollower | Name of the MongoDB database to store the flight data.|
| `mongoDb -> collection`| flights | Name of the document collection in the database to store the flight data.|
| `registration` | If this object is ommitted, retrieving registration data will be disabled.  Useful for converting the ICAO hex code to a registration object.  For example, querying `A8AE7F` would return to an object for a Boeing 757.  |
| `registration -> enabled` | false | Controls if retrieving registration data is enabled or disabled.|
| `registration -> uri` | http://localhost:8480/registration?icao_hex=$ICAO_HEX$ | The URL of the webserver where the registration microservice runs.  Note, $ICAO_HEX$ will subsitute the ICAO hex code of the aircraft. |
| `registration -> x-api-key` | some_secret_key | The value of the x-api-key header that should be sent for authenticating the request to the service.|
| `operators` | If this object is ommitted, retrieving operator data will be disabled.  Useful for converting aircraft callsigns to operators.  For example, querying for `DAL` would return an object for Delta Air Lines.|
| `operators -> enabled` | false | Controls if retrieving operator data is enabled or disabled.|
| `operators -> uri` | http://localhost:8480/operator?airline_designator=$CALLSIGN$ | The URL of the webserver where the operators microservice runs.  Note, $CALLSIGN$ will subsitute the ICAO callsign of the aircraft. |
| `operators -> x-api-key` | some_secret_key | The value of the x-api-key header that should be sent for authenticating the request to the service.|
| `mqtt` |  If this object is omitted, MQTT will be disabled.|
| `mqtt -> enabled` | false | Controls if MQTT is enabled or disabled.|
| `mqtt -> uri` | my.mqtt.server.lan | IP address or domain name of your MQTT broker. |
| `mqtt -> port` | 1883 | Port for your MQTT broker. |
| `mqtt -> username` | my_username | Username to use when authenticating to your MQTT broker. |
| `mqtt -> password` | my_clear_text_password | Password to use when authenticating to your MQTT broker. |
| `mqtt -> topic` | SkyFollower | Root for all topics.|
| `files` | Locations to additional files used by SkyFollower |
| `files -> areas` | ./areas.geojson | Path to the defined areas GeoJSON file |
| `files -> rules` | ./rules.json | Path to the notification rules |
| `home_assistant` | If this object is omitted, Home Assistant will be disabled.|
| `home_assistant -> enabled` | false | Controls if Home Assistant integration is enabled or disabled.|
| `home_assistant -> discovery_prefix` | homeassistant | The prefix that Home Assistant is listening to for auto discovery messages.  By default, Home Assistant is listening for auto discovery on the `homeassistant` MQTT topic and all child topics. |




## Service Installation

Copy the service file to the systemctl directory
```
sudo mv /etc/P5Software/SkyFollower/SkyFollower.service /lib/systemd/system/
```

Reload the systemctrl daemon to pick up the new SkyFollower service
```
sudo systemctl daemon-reload
```

Enable the service to run
```
sudo systemctl enable SkyFollower.service
```

Start the service
```
sudo systemctl start SkyFollower.service
```

## Areas
An area is a simple GeoJSON polygon of an interesting area.  The feature must include a property called `name` to be permitted.  A great tool to define your GeoJSON can be found at [https://geojson.io](https://geojson.io).  You can include geometries other than polygons, but SkyFollower will ignore anything that is not a polygon.

Below is an example which will be used in the next section, note that a `name` property of `LI` has been added to the `properties` section.  You may also find this example in the areas.example.geojson file.
```
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "name": "LI"
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            -73.8006591796875,
                            40.82835864973048
                        ],
                        [
                            -73.97369384765625,
                            40.734770989672406
                        ],
                        [
                            -74.03961181640625,
                            40.54720023441049
                        ],
                        [
                            -73.7677001953125,
                            40.538851525354666
                        ],
                        [
                            -73.245849609375,
                            40.58684239087908
                        ],
                        [
                            -72.70751953125,
                            40.73685214795608
                        ],
                        [
                            -71.8560791015625,
                            40.97160353279909
                        ],
                        [
                            -71.80938720703125,
                            41.044145364313174
                        ],
                        [
                            -71.89727783203125,
                            41.14970617453726
                        ],
                        [
                            -72.11700439453125,
                            41.21998578493921
                        ],
                        [
                            -72.36145019531249,
                            41.17038447781618
                        ],
                        [
                            -72.70477294921874,
                            41.03585891144301
                        ],
                        [
                            -72.83935546875,
                            41.04621681452063
                        ],
                        [
                            -73.1964111328125,
                            40.994410999439516
                        ],
                        [
                            -73.553466796875,
                            40.94671366508002
                        ],
                        [
                            -73.8006591796875,
                            40.82835864973048
                        ]
                    ]
                ]
            }
        }
    ]
}
```

## Rules
SkyFollower will optionally raise an event via MQTT based on a configured rule being met.

Rules are comprised of the following attributes.  Note that the field names are case sensitive.


| Field | Data Type | Required | Example | Meaning |
|-------|-----------|----------|---------|---------|
| `name` | string | No | Any Boeing 757-200 | An arbitrary name that is human-friendly to help identify the given rule |
| `description` | string | No | Boeing 757's are my favorite airplanes | An arbitrary description that is human-friendly to help identify the given rule |
| `identifer` | string | Yes | All_B752 | A unique identifier that will be sent in the MQTT message that may be used by other systems to apply further notification processing.  The identifier is appended to the MQTT topic |
| `enabled` | boolean | Yes | true | Indication if the rule should be processed or ignored |
| `conditions` | array | Yes | See below | The array of conditions that must be satisifed for this rule to trigger.  Conditions are treated as an `AND` is inserted between them | 

### Conditions
Conditions are evaluated individually, but all conditions must evaluate to `true` in order for the rule to be triggered, resulting in an MQTT notification being sent.

The structure of a condition is consistent across each condition type.  Values are always strings, and are not case sensitive.

- `type` is the type of condition evaluation which should occur (see below)
- `value` is the value which will be evaluated
- `operator` indicates how the value should be evaluted.  Permitted values are `equals`, `minimum`, and `maximum`.  Numeric values are inclusive when using `minimum` or `maximum`

### Condition Types

| Type | Value | Permitted Operators | Meaning |
|------|-------|---------------------|---------|
| `aircraft_icao_hex` | Any | `equals` | Aircraft ICAO hex |
| `aircraft_powerplant_count` | Any positive integer | `equals`, `minimum`, `maximum` | Number of powerplants attached to the aircraft |
| `aircraft_registration` | Any | `equals` | Aircraft registration mark |
| `aircraft_type_designator` | Any | `equals` | ICAO type designator for the aircraft |
| `altitude` | Any positive integer | `minimum`, `maximum` | Aircraft altitude MSL |
| `area` | Any | `equals` | Name of the feature collection in the geoJSON file |
| `callsign` | Any | `equals` | Flight ICAO callsign |
| `date` | ISO-8601 date format YYYY-mm-dd | `equals`, `minimum`, `maximum` | Date in GMT |
| `heading` | Tuple  | `equals` | Aircraft ground track in degrees, where the first value is the minimum heading and the second value is the maximum heading.  Legal vales are `0` to `359`, inclusive
| `military` | Boolean | `equals` | If the aircraft is marked as known military |
| `operator_airline_designator` | Any | `equals` | ICAO airline designator |
| `squawk` | Any positive integer | `equals` | Transponder squawk code, including leading zeros as necessary
| `velocity` | Any positive integer | `minimum`, `maximum` | Forward velocity of the aircraft in knots |
| `vertical_speed` | Any integer | `minimum`, `maximum` | Vertical speed of the aircraft in feet per minute |
| `wake_turbulence_category` | One of: `Light`, `Medium`, `Medium 1`, `Medium 2`, `High Vortex Aircraft`, `Heavy`, `Super`, `Rotorcraft`, `High Performance` | `equals` | Aircraft wake turbulence category, as reported from the registry or through the ADS-B message |

> If any condition in the trigger evalutes to false, the entire rule evaluates to false. 

### Example Rules and Conditions
You may also find all of these examples in the rules.example.json file.

1. Any aircraft with an altitude at or below 10,000ft

```
{
  "name": "All aircraft below 10,000",
  "identifier": "acft_10k_and_below",
  "description": "Any aircraft with an altitude at or below 10,000ft",
  "enabled" : true,
  "conditions": [
      {
          "type": "altitude",
          "value": "10000",
          "operator": "maximum"
      }
    ]
}
```

2.  United Airlines Boeing 757's between 12,000 and 15,000ft heading north (between 340 and 020 degrees)

```
{
  "name": "UAL B757-200",
  "identifier": "Northbound_United_B75s_12k-15k",
  "description": "United Airlines Boeing 757-200's between 12,000 and 15,000ft heading north after takeoff",
  "enabled" : true,
  "conditions": [
      {
          "type": "altitude",
          "value": "15000",
          "operator": "maximum"
      },
      {
          "type": "altitude",
          "value": "12000",
          "operator": "minimum"
      },
      {
          "type": "operator_airline_designator",
          "value": "UAL",
          "operator": "equals"
      },
      {
          "type": "aircraft_type_designator",
          "value": "B752",
          "operator": "equals"
      },
      {
          "type": "heading",
          "value": "340,020",
          "operator": "equals"
      },
      {
          "type": "vertical_speed",
          "value": "500",
          "operator": "minimum"
      }
    ]     
}
```

3.  Delta Flight 2 on Christmas Eve Descending at least 100FPM over Long Island
> Refer to the "Areas" section of this guide for the definition of Long Island.

```
{
  "name": "Grandma's Flight Home",
  "identifier": "grandma",
  "description": "Grandma's Flight Home Arriving on Christmas Eve",
  "enabled" : true,
  "conditions": [
      {
          "type": "callsign",
          "value": "DAL2",
          "operator": "equals"
      },
      {
          "type": "date",
          "value": "2022-12-24",
          "operator": "minimum"
      },
      {
          "type": "date",
          "value": "2022-12-24",
          "operator": "maximum"
      },
      {
          "type": "vertical_speed",
          "value": "-100",
          "operator": "maximum"
      },
      {
          "type": "area",
          "value": "LI",
          "operator": "equals"
      }
    ]
}
```


## FAQ
- Can I run this on Raspberry Pi?  Yes, but...
  - There will be changes needed to the installation above, but it should work.
  - MongoDB will do a _lot_ of file I/O operations, so if you're running this from an SD card, expect the SD card to die pretty quickly.  If you run MongoDB on a separate computer with an SSD/NVMe/traditional hard drive, your experience will be much better and it should be fine.
  - Data is stored in memory while SkyFollower is actively tracking the aircraft.  If you receive a lot of traffic with your receiver, this may also saturate the limited memory in a Raspberry Pi and cause issues for SkyFollower or other applications.
- What ADS-B receivers is this compatible with?
  - Most ADS-B receivers will output either a `Beast` or `raw` format, including FlightAware's SkyAware and PlaneFinder's PlaneFinderClient.  Running this software does not interfere with those services and can live happily beside them.
- Do I need to know about database management to make this work?
  - No, but you might want to learn how to query MongoDB to add additional features or get information that interests you.
- How do you differentiate between a loss of signal and a different flight?
  - The setting `flight_ttl_seconds` defines the number of seconds since loss-of-signal where the flight is determined done and the data is then persisted to MongoDB.
- What happens when my computer shuts down or I stop the SkyFollower service?
  - The ADS-B listener is stopped and data stored in memory is immediately written to MongoDB.  Restarting SkyFollower immediately will likely cause a duplication of the flights currently being received, since SkyFollower doesn't retroactively merge documents in MongoDB.
- Is this going to break my map or other tools that I currently use with dump1090 or the like?
  - SkyFollower listens to the same stream of data that your other tools do, and therefore should not interfere with those functions.  Custom maps, real-time statistics, and other neat add-on's that you might be using will continue to work unaffected.  Note, however, that if you install the [Aircraft Registration and Operator Information](https://github.com/BrentIO/Aircraft-Registration-and-Operator-Information), it is possible this could conflict with the port your map uses, so read those docs carefully.
- The service won't start and there's nothing in any of the logs.  What do I do?
  - Try starting the service manually by using `sudo python3 /etc/P5Software/SkyFollower/main.py`.  If there is an error, it will usually be printed on the screen for you to see.