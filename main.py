#!/usr/bin/env python3

from importlib.resources import path
import os
import logging
import logging.handlers as handlers
import json
from queue import Empty
import sys
from tinydb import TinyDB, Query                    #pip3 install tinydb
from tinydb.storages import MemoryStorage
import pyModeS as pms                               #pip3 install pyModeS
from pyModeS.extra.tcpclient import TcpClient
from pymongo import MongoClient, errors                    #pip3 install pymongo
import uuid
import time
import datetime
from datetime import datetime, timedelta
import threading
from threading import Thread, current_thread
import requests
import re
import paho.mqtt.client                             #pip3 install paho-mqtt
import signal
from rulesEngine import rulesEngine as skyFollowerRE
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import schedule


def handle_interrupt(signal, frame):
    raise sigKill("SIGKILL Requested")


class sigKill(Exception):
    pass

    
class ADSBClient(TcpClient):

    connected = False
   
    def __init__(self):
        super(ADSBClient, self).__init__(settings['adsb']['uri'], settings['adsb']['port'], settings['adsb']['type'])

        threadStatusCheck = threading.Thread(target=self.checkIfAlive)
        threadStatusCheck.start()


    def setConnected(self):
        if self.connected == False:
            self.connected = True
            logger.info("ADS-B client connected to " + settings['adsb']['uri'] + ".")


    def checkIfAlive(self):

        try:

            #Timeout
            time.sleep(30)
        
            if self.connected == False:
                raise Exception("No data received from " + settings['adsb']['uri'] + " before timeout.")                

        except Exception as ex:
            logger.critical(ex)
            logger.critical("Error; Exiting with code 2")
            os._exit(2)


    def handle_messages(self, messages):
        self.setConnected()
        threadMessageProcessor = threading.Thread(target=messageProcessor, args=(messages,))
        threadMessageProcessor.start()


def messageProcessor(messages):
    
    for msg, ts in messages:
        
        try:

            #Object to store data
            data = {}

            data['timestamp'] = ts

            #Get the download format
            data['downlink_format'] = pms.df(msg)

            if data['downlink_format'] not in (0, 11, 16, 4, 20, 5, 21, 17):
                logger.debug("Unexpected downlink format " + str(data['downlink_format']) + " msg: " + msg)
            
            #Throw away certain DF's (0 & 16 are ACAS, 11 is all-call)
            if data['downlink_format'] == 0 or data['downlink_format'] == 11 or data['downlink_format'] == 16:
                continue
            
            data['icao_hex'] = pms.adsb.icao(msg)

            #Ensure we have an icao_hex
            if data['icao_hex'] == None:
                continue

            if data['downlink_format'] == 4 or data['downlink_format'] == 20:
                data['altitude'] = pms.common.altcode(msg)

            if data['downlink_format'] == 5 or data['downlink_format'] == 21:
                data['squawk'] = pms.common.idcode(msg)

            if data['downlink_format'] == 17:
                
                typeCode = pms.adsb.typecode(msg)
                data['messageType'] = typeCode

                #Throw away TC 28 and 29...not yet supported
                if typeCode == 28 or typeCode == 29:
                    continue

                if 1 <= typeCode <= 4:
                    data['callsign'] = pms.adsb.callsign(msg).replace("_","")
                    data['category'] = pms.adsb.category(msg)                    

                if 5 <= typeCode <= 18 or 20 <= typeCode <=22:
                    data['latitude'] = pms.adsb.position_with_ref(msg, settings['latitude'], settings['longitude'])[0]
                    data['longitude'] = pms.adsb.position_with_ref(msg, settings['latitude'], settings['longitude'])[1]
                    data['altitude'] = pms.adsb.altitude(msg)

                if 5 <= typeCode <= 8:
                    data['velocity'] = pms.adsb.velocity(msg)[0]
                    data['heading'] = pms.adsb.velocity(msg)[1]
                    data['vertical_speed'] = pms.adsb.velocity(msg)[2]

                if typeCode == 19:
                    data['velocity'] = pms.adsb.velocity(msg)[0]
                    data['heading'] = pms.adsb.velocity(msg)[1]
                    data['vertical_speed'] = pms.adsb.velocity(msg)[2]

                if typeCode == 31:
                    data['adsb_version'] = pms.adsb.version(msg)

            #Process the message for storage in the local database
            storeMessageLocal(data)

        except Exception as ex:
            logger.warning("Exception while processing message " + str(msg) + " : " + str(ex))
        

def storeMessageLocal(data):

    stats.increment_message_count()

    Record = Query()
    result = localDb.search(Record.icao_hex == data['icao_hex'])

    flight = {}

    if len(result) == 0:
        flight['icao_hex'] = data['icao_hex']
        flight['first_message'] = data['timestamp']
        flight['_id'] = str(uuid.uuid4())
        flight['last_message'] = 0
        flight['total_messages'] = 0
        flight['matched_rules'] = []

        logger.debug("New aircraft added to localDb _id:" + flight['_id'] + " ICAO HEX: " + flight['icao_hex'])
        stats.increment_flights_count()

        #Get the aircraft data
        aircraftData = getRegistration(flight['icao_hex'])

        #If the registration was returned, store the data
        if aircraftData is None:
            flight['aircraft'] = {}
            flight['aircraft']['icao_hex'] = flight['icao_hex']
        else:
            flight['aircraft'] = aircraftData
        
    else:
        flight = result[0]

    #Set the last message to now
    flight['last_message'] = data['timestamp']

    #Increment the number of messages received
    flight['total_messages'] = flight['total_messages'] + 1

    #Check message for position reports
    if "latitude" in data and "longitude" in data and "altitude" in data:
        positionReport = {}
        positionReport['timestamp'] = datetime.utcfromtimestamp(data['timestamp'])

        if "latitude" in data:
            positionReport['latitude'] = data['latitude']

        if "longitude" in data:
            positionReport['longitude'] = data['longitude']

        if "altitude" in data:
            positionReport['altitude'] = data['altitude']

        if 'positions' not in flight:
            flight['positions'] = []

        flight['positions'].append(positionReport)

    #Check message for velocity reports
    if "velocity" in data or "heading" in data or "vertical_speed" in data:
        velocityReport = {}
        velocityReport['timestamp'] = datetime.utcfromtimestamp(data['timestamp'])

        if "velocity" in data:
            velocityReport['velocity'] = data['velocity']

        if "heading" in data:
            velocityReport['heading'] = data['heading']

        if "vertical_speed" in data:
            velocityReport['vertical_speed'] = data['vertical_speed']

        if 'velocities' not in flight:
            flight['velocities'] = []

        flight['velocities'].append(velocityReport)

    if "squawk" in data:
        flight['squawk'] = data['squawk']

    if "category" in data:
        
        parseAircraftCategoryResponse = parseAircraftCategory(data['category'])

        if parseAircraftCategoryResponse is not None:
            if 'aircraft' not in flight:
                flight['aircraft'] = {}
                
            flight['aircraft']['wake_turbulence_category'] = parseAircraftCategoryResponse

    if "callsign" in data:

        #If the callsign is currently empty and the incoming data is not empty
        if 'callsign' not in flight and data['callsign'] != "" and 'registration' in flight['aircraft']:

            #Store the callsign, note only the first callsign received will be used
            flight['callsign'] = data['callsign']

            #See if there is operator data in the callsign
            parseCallsignResponse = parseCallsign(flight['callsign'], flight['aircraft']['registration'])

            if parseCallsignResponse is not None:

                #Delete repetitive data
                del parseCallsignResponse['callsign']

                #There is operator data, store it
                flight['operator'] = parseCallsignResponse

    if "adsb_version" in data:

        if 'aircraft' not in flight:
            flight['aircraft'] = {}

        flight['aircraft']['adsb_version'] = data['adsb_version']

    #Check if the flight is of interest
    interestResult = checkFlightOfInterest(flight)

    if interestResult != []:
        for matchedRule in interestResult:
            flight['matched_rules'].append(matchedRule['identifier'])

    #Commit to the local database
    localDb.upsert(flight, Record.icao == data['icao_hex'])


def parseCallsign(callsign, registration):

    #If the callsign is empty, return the registration
    if callsign == "":
        return None

    #If the callsign and registration are the same, just return the registration
    if callsign == registration:
        return None

    #Callsigns are 3 letter designators followed by numbers
    if re.match("^[A-Z][A-Z][A-Z][0-9]+$",callsign) is None:
        return None

    #Valid callsign received, parse to get the operator and flight number
    getOperatorResponse = getOperator(callsign[0:3])

    if getOperatorResponse == None:
        return None

    returnValue = {}

    returnValue['callsign'] = callsign
    returnValue['airline_designator'] = getOperatorResponse['airline_designator']
    returnValue['flight_number'] = callsign[3:]
    returnValue['operator'] = getOperatorResponse['name']
    returnValue['phonic'] = getOperatorResponse['callsign'] + " " + str(returnValue['flight_number'])
    returnValue['country'] = getOperatorResponse['country']

    return returnValue


def parseAircraftCategory(category):

    if category == 1:
        return "Light"

    if category == 2:
        return "Medium 1"

    if category == 3:
        return "Medium 2"

    if category == 4:
        return "High Vortex Aircraft"

    if category == 5:
        return "Heavy"

    if category == 6:
        return "High Performance"

    if category == 7:
        return "Rotorcraft"

    return None


def getRegistration(icao_hex):

    if settings['registration']['enabled'] != True:
        return None

    try:

        r = requests.get(settings['registration']['uri'].replace("$ICAO_HEX$", icao_hex), headers={'x-api-key': settings['registration']['x-api-key']})

        if r.status_code == 200:
            return json.loads(r.text)

        logger.info("Unable to get registration details for " + str(icao_hex) +"; getRegistration returned " + str(r.status_code))
        return None

    except Exception as ex:
        logger.warning("Error getting registration.")
        logger.error(ex)
        return None


def getOperator(callsign):

    if settings['operators']['enabled'] != True:
        return None

    try:

        r = requests.get(settings['operators']['uri'].replace("$CALLSIGN$", callsign), headers={'x-api-key': settings['operators']['x-api-key']})

        if r.status_code == 200:
            return json.loads(r.text)
        
        logger.info("Unable to get operator details for " + str(callsign) +"; getOperator returned " + str(r.status_code))
        return None

    except Exception as ex:
        logger.warning("Error getting operator.")
        logger.error(ex)
        return None


def storeMessageRemote(threadState = True):

    countOfMigrated = 0

    try:

        Record = Query()

        #Determine if we will continue to live after this round
        if threadState:
            stale_flights = localDb.search(Record.last_message < (datetime.now().timestamp() - timedelta(seconds = settings['flight_ttl_seconds']).total_seconds()))
        else:
            #Thread is shutting down, persist all the records regardless of their status
            logger.info("Shutdown detected, persisting all local records to MongoDB.")
            stale_flights = localDb.all()

        if len(stale_flights) > 0:

            mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
            adsbDB = mongoDBClient[settings['mongoDb']['database']]
            adsbDBCollection = adsbDB[settings['mongoDb']['collection']] 

            #An array will be returned, cycle through each flight
            for flight in stale_flights:

                icao_hex = flight['icao_hex']

                #Make the datestamps human-readable
                flight['first_message'] = datetime.utcfromtimestamp(flight['first_message'])
                flight['last_message'] = datetime.utcfromtimestamp(flight['last_message'])

                #Delete data we do not want to persist
                del flight['matched_rules']

                if 'aircraft' in flight:
                    
                    #ICAO Hex will be repetitive, but only delete it if there is an aircraft object
                    del flight['icao_hex']

                    if 'military' in flight['aircraft']:
                        if flight['aircraft']['military'] == False:
                            del flight['aircraft']['military']

                logger.debug("Inserting into MongoDB " + flight['_id'] + " " + flight['aircraft']['icao_hex'])

                adsbDBCollection.insert_one(flight)

                countOfMigrated = countOfMigrated + 1

                localDb.remove(Record.icao_hex == icao_hex)
            
            mongoDBClient.close()

            logger.debug("Finished migration to MongoDB.  " + str(countOfMigrated) + " records were migrated.")

        else:
            logger.debug("No records ready to be migrated to MongoDB.")

    except errors.DuplicateKeyError as dke:

        if 'keyValue' in dke.details:
            if '_id' in dke.details['keyValue']:

                try:
                    
                    directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "/errors/")

                    if not os.path.exists(directory):
                        os.makedirs(directory)

                    fileName = os.path.join(directory, "dke_" + str(datetime.utcnow().isoformat()).replace(":", "-") + ".json")

                    flight['error'] = "pymongo.errors.DuplicateKeyError"

                    with open(fileName, 'w') as outfile:
                        json.dump(flight, outfile, default=jsonDefaultConverter)

                    localDb.remove(Record._id == dke.details['keyValue']['_id'])

                    logger.critical("Duplicate key error occurred.  Data was written to the disk as file " + fileName +".")

                except Exception as ex:
                    logger.critical("Duplicate key error failed when attempting to write to disk.  Data was lost.  The duplicate _id=" + str(dke.details['keyValue']['_id']))
                    logger.critical(ex)

    except Exception as ex:
        logger.error("Error migrating data to MongoDB.")
        logger.error(ex)


def jsonDefaultConverter(o):
  if isinstance(o, datetime):
      return o.__str__()


def checkFlightOfInterest(flight):

    global rulesEngine

    result = rulesEngine.evaluate(flight)

    for matchedRule in result:

        notification = {}

        if 'aircraft' in flight:
            notification['aircraft'] = flight['aircraft']

        if 'squawk' in flight:
            notification['squawk'] = flight['squawk']

        notification['rule'] = {}

        notification['rule']['name'] = matchedRule['name']
        notification['rule']['description'] = matchedRule['description']
        notification['rule']['identifier'] = matchedRule['identifier']

        mqtt_publishNotication(notification['rule']['identifier'], json.dumps(notification))

    return result


def mqtt_publishNotication(identifier, message):

    if settings['mqtt']['enabled'] != True:
        return

    if mqttClient.is_connected():
        mqttClient.publish(settings["mqtt"]["topic_rule"] + identifier, message)


def mqtt_publishOnline():

    if settings['mqtt']['enabled'] != True:
        return

    if mqttClient.is_connected() == True:
        mqttClient.publish(settings['mqtt']['topic_status'], "ONLINE", retain=True)


def mqtt_publishAutoDiscovery():

    if settings['mqtt']['enabled'] != True:
        return

    if settings['home_assistant']['enabled'] != True:
        return

    ad = autoDiscovery()
    ad.status()
    ad.stats()
    ad.rules()
 
    

def mqtt_onConnect(client, userdata, flags, rc):
    #########################################################
    # Handles MQTT Connections
    #########################################################

    if settings['mqtt']['enabled'] != True:
        return

    try:
        if rc != 0:
            logger.warning("Failed to connect to MQTT.  Response code: " + str(rc) + ".")

        else:
            logger.info("MQTT connected to " + settings["mqtt"]["uri"] + ".")

            mqtt_publishOnline()
            mqtt_publishAutoDiscovery()
            stats.publish()

    except Exception as ex:
        logger.error(ex)
        

def exitApp(exitCode=None):

    #Force the log level to info
    logger.setLevel(logging.INFO)

    if exitCode is None:
        exitCode = 0

    #Commit the database if it is not memory
    if 'local_database_mode' in settings:
        if settings['local_database_mode'] == "disk":
            logger.info("Committing database to disk.")
            localDb.commit()

    if exitCode == 0:
        logger.info(applicationName + " application finished successfully.")

    if exitCode != 0:
        logger.info("Error; Exiting with code " + str(exitCode))

    sys.exit(exitCode)


def setLogLevel(logLevel):

    logLevel = logLevel.lower()

    if logLevel == "debug":
        logger.setLevel(logging.DEBUG)
        logger.debug("Logging set to DEBUG.")
        return

    if logLevel == "error":
        logger.setLevel(logging.ERROR)
        logger.error("Logging set to ERROR.")
        return

    if logLevel == "warning":
        logger.setLevel(logging.WARNING)
        logger.warning("Logging set to WARNING.")
        return

    if logLevel == "critical":
        logger.setLevel(logging.CRITICAL)
        logger.critical("Logging set to CRITICAL.")
        return


def setup():

    global applicationName
    global settings
    global logger
    global localDb
    global mqttClient
    global rulesEngine
    global stats

    #Define some constants
    applicationName = "SkyFollower"
    settings = {}

    stats = statistics()

    try:

        filePath = os.path.dirname(os.path.realpath(__file__))

        #Setup the logger, 10MB maximum log size
        logger = logging.getLogger(applicationName)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
        logHandler = handlers.RotatingFileHandler(os.path.join(filePath, 'events.log'), maxBytes=10485760, backupCount=1)
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
        logger.setLevel(logging.INFO)

        logger.info(applicationName + " application started.")

        #Make sure the settings file exists
        if os.path.exists(os.path.join(filePath, 'settings.json')) == False:
            raise Exception("Settings file does not exist.  Expected file " + os.path.join(filePath, 'settings.json'))

        #Settings file exists, read it in and verify its contents
        with open(os.path.join(filePath, 'settings.json')) as settingsFile:
            settings = json.load(settingsFile)

        if "log_level" in settings:
            setLogLevel(settings['log_level'])

        if "files" not in settings:
            raise Exception ("files object is missing from settings.json")

        rulesEngine = skyFollowerRE(logger)

        if "areas" in settings['files']:
            settings['files']['areas'] = settings['files']['areas'].replace("./", filePath + "/")
            rulesEngine.loadAreas(settings['files']['areas'])
        else:
            logger.warning("Missing files -> areas in settings.json")

        if "rules" in settings['files']:
            settings['files']['rules'] = settings['files']['rules'].replace("./", filePath + "/")
            rulesEngine.loadRules(settings['files']['rules'])
            
        else:
            logger.warning("Missing files -> rules in settings.json")

        if "adsb" not in settings:
            raise Exception ("adsb object is missing from settings.json")

        if "uri" not in settings['adsb']:
            raise Exception ("Missing adsb -> uri in settings.json")

        if settings['adsb']['uri'] == "":
            raise Exception ("Empty adsb -> uri in settings.json")

        if "port" not in settings['adsb']:
            raise Exception ("Missing adsb -> port in settings.json")

        if str(settings['adsb']['port']).isnumeric() != True:
            raise Exception ("Invalid adsb -> port in settings.json")

        if "type" not in settings['adsb']:
            raise Exception ("Missing adsb -> type in settings.json")

        if settings['adsb']['type'].lower() not in ['beast','raw']:
            raise Exception ("Unknown adsb -> type in settings.json.  Valid values are beast | raw")

        if "flight_ttl_seconds" not in settings:
            logger.debug("Setting 'flight_ttl_seconds' not declared in the settings file; Defaulting to 300 seconds.")
            settings['flight_ttl_seconds'] = 300

        if str(settings['flight_ttl_seconds']).isnumeric() != True:
            raise Exception ("Invalid flight_ttl_seconds in settings.json")

        if "latitude" not in settings:
            raise Exception ("Missing latitude in settings.json")

        if isinstance(settings['latitude'], float) == False:
            raise Exception ("Invalid latitude in settings.json")

        if "longitude" not in settings:
            raise Exception ("Missing longitude in settings.json.  Expected float.")

        if isinstance(settings['longitude'], float) == False:
            raise Exception ("Invalid longitude in settings.json.  Expected float.")

        if settings['latitude'] == 38.8969137 and settings['longitude'] == -77.0357096:
            raise Exception ("Configure your latitude and longitude in settings.json.")

        if 'mqtt' not in settings:
            logger.info("mqtt is not declared in the settings file; MQTT will be disabled.")

            settings['mqtt'] = {}

            settings['mqtt']['enabled'] = False

        else:
            
            if 'enabled' not in settings['mqtt']:
                settings['mqtt']['enabled'] = False

            if settings['mqtt']['enabled'] == False:
                logger.info("MQTT is disabled in the settings file; MQTT will be disabled.")

            if "uri" not in settings['mqtt']:
                raise Exception ("Missing mqtt -> uri in settings.json")

            if settings['mqtt']['uri'] == "":
                raise Exception ("Empty mqtt -> uri in settings.json")

            if "port" not in settings['mqtt']:
                raise Exception ("Missing mqtt -> port in settings.json")

            if str(settings['mqtt']['port']).isnumeric() != True:
                raise Exception ("Invalid mqtt -> port in settings.json")

            if "username" not in settings['mqtt']:
                raise Exception ("Missing mqtt -> username in settings.json")

            if settings['mqtt']['username'] == "":
                raise Exception ("Empty mqtt -> username in settings.json")

            if "password" not in settings['mqtt']:
                raise Exception ("Missing mqtt -> password in settings.json")

            if settings['mqtt']['password'] == "":
                raise Exception ("Empty mqtt -> password in settings.json")   

            if "topic" not in settings['mqtt']:
                raise Exception ("Missing mqtt -> topic in settings.json")

            if settings['mqtt']['topic'] == "":
                raise Exception ("Empty mqtt -> topic in settings.json")

            settings['mqtt']['topic_status'] = str(settings['mqtt']['topic']+ "/status").replace("//", "/")
            settings['mqtt']['topic_rule'] = str(settings['mqtt']['topic'] + "/rule/").replace("//", "/")
            settings['mqtt']['topic_statistics'] = str(settings['mqtt']['topic'] + "/stats/").replace("//", "/")
            settings['mqtt']['topic_error'] = str(settings['mqtt']['topic'] + "/error").replace("//", "/")

            #Create MQTT Client
            mqttClient = paho.mqtt.client.Client()

        if 'local_database_mode' not in settings:
            settings['local_database_mode'] = "memory"

        if "mongoDb" not in settings:
            raise Exception ("mongoDb object is missing from settings.json")

        if "uri" not in settings['mongoDb']:
            raise Exception ("Missing mongoDb -> uri in settings.json")

        if "port" not in settings['mongoDb']:
            raise Exception ("Missing mongoDb -> port in settings.json")

        if str(settings['mongoDb']['port']).isnumeric() != True:
            raise Exception ("Invalid mongoDb -> port in settings.json")

        if "database" not in settings['mongoDb']:
            raise Exception ("Missing mongoDb -> database in settings.json")

        if "collection" not in settings['mongoDb']:
            raise Exception ("Missing mongoDb -> collection in settings.json")

        if 'registration' not in settings:
            logger.info("registration is not declared in the settings file; Retrieving registrations will be disabled.")

            settings['registration'] = {}

            settings['registration']['enabled'] = False

        else:

            if 'enabled' not in settings['registration']:
                settings['registration']['enabled'] = False

            if settings['registration']['enabled'] == False:
                logger.info("Registration is disabled in the settings file; Retrieving registrations will be disabled.")

            if "registration" not in settings:
                raise Exception ("registration object is missing from settings.json")

            if "uri" not in settings['registration']:
                raise Exception ("Missing registration -> uri in settings.json")

            if "$ICAO_HEX$" not in settings['registration']['uri']:
                raise Exception ("Missing $ICAO_HEX$ text in registration -> uri in settings.json")

            if "x-api-key" not in settings['registration']:
                raise Exception ("Missing registration -> x-api-key in settings.json")

        if 'operators' not in settings:
            logger.info("operators is not declared in the settings file; Retrieving operators will be disabled.")

            settings['operators'] = {}

            settings['operators']['enabled'] = False

        else:

            if 'enabled' not in settings['operators']:
                settings['operators']['enabled'] = False

            if settings['operators']['enabled'] == False:
                logger.info("Operators is disabled in the settings file; Retrieving operators will be disabled.")

            if "uri" not in settings['operators']:
                raise Exception ("Missing registration -> uri in settings.json")

            if "$CALLSIGN$" not in settings['operators']['uri']:
                raise Exception ("Missing $CALLSIGN$ text in operators -> uri in settings.json")

            if "x-api-key" not in settings['operators']:
                raise Exception ("Missing operators -> x-api-key in settings.json")

        if 'home_assistant' not in settings:
            logger.info("home_assistant is not declared in the settings file; Home Assistant will be disabled.")

            settings['home_assistant'] = {}

            settings['home_assistant']['enabled'] = False

        else:

            if 'enabled' not in settings['home_assistant']:
                settings['home_assistant']['enabled'] = False

            if settings['home_assistant']['enabled'] == False:
                logger.info("Home Assistant is disabled in the settings file; Home Assistant operators will be disabled.")

            if settings['home_assistant']['enabled'] == True and 'discovery_prefix' not in settings['home_assistant']:
                settings['home_assistant']['discovery_prefix'] = "homeassistant"
                logger.debug("Setting 'home_assistant -> discovery_prefix' not declared in the settings file; Defaulting to 'homeassistant'.")

            settings['mqtt']['topic_home_assistant_autodiscovery'] = str(settings['home_assistant']['discovery_prefix'] + "/").replace("//", "/")

        #Default the local database to be memory
        if str(settings['local_database_mode']).lower() == "memory":
            logger.debug("Using memory for localDb.")
            localDb = TinyDB(storage=MemoryStorage)

        else:
            settings['local_database_mode'] = "disk"
            logger.debug("Using disk for localDb.")
            settings['database_file'] = os.path.join(filePath, applicationName + ".tinydb")
            localDb = TinyDB(settings['database_file'])

    except Exception as ex:
        logger.error(ex)
        exitApp(1)


def run_scheduled_tasks():

    t = current_thread()
    t.alive = True

    while t.alive:
        schedule.run_pending()
        time.sleep(1)


def main():

    try:

        if settings['mqtt']['enabled'] == True:
    
            #Setup the handlers for connection and messages
            mqttClient.on_connect = mqtt_onConnect

            #Create the MQTT credentials from the settings file
            mqttClient.username_pw_set(settings["mqtt"]["username"], password=settings["mqtt"]["password"])

            #Set the last will and testament
            mqttClient.will_set(settings["mqtt"]["topic_status"], payload="OFFLINE", qos=0, retain=True)

            #Connect to MQTT
            mqttClient.connect_async(settings["mqtt"]["uri"], port=settings["mqtt"]["port"], keepalive=60)

        # run new client, change the host, port, and rawtype if needed
        adsb_client = ADSBClient()

        schedule.every().hour.at("00:30").do(stats.reset_hour)
        schedule.every().day.at("00:00").do(stats.reset_today)
        schedule.every(30).seconds.do(stats.publish)

        #Remote storage thread
        schedule.every(10).seconds.do(storeMessageRemote)
        
        scheduler = threading.Thread(name="scheduled_tasks", target=run_scheduled_tasks)

        #Start the threads
        if settings['mqtt']['enabled'] == True:
            mqttClient.loop_start()

        scheduler.start()

        observer = Observer()
        event_handler = fileChanged()
        observer.schedule(event_handler, path=os.path.dirname(settings['files']['areas']))
        observer.schedule(event_handler, path=os.path.dirname(settings['files']['rules']))
        observer.start()

        adsb_client.run() #Blocking, must be last        

        exitApp(0)

    except (sigKill, KeyboardInterrupt):

        if settings['mqtt']['enabled'] == True:
            mqttClient.publish(settings["mqtt"]['topic_status'], "TERMINATING")

        if scheduler:            
            if scheduler.is_alive():
                scheduler.alive = False
                scheduler.join()
     
        storeMessageRemote(False)

        if settings['mqtt']['enabled'] == True:
            mqttClient.loop_stop()

        exitApp(0)

    except Exception as ex:
        logger.error(ex)
        exitApp(1)
        
    finally:
        if observer:
            if observer.is_alive():
                observer.stop()
                observer.join()


class fileChanged(PatternMatchingEventHandler):

    def __init__(self):
        # Set the patterns for PatternMatchingEventHandler
        PatternMatchingEventHandler.__init__(self, patterns=[os.path.basename(settings['files']['areas']),os.path.basename(settings['files']['rules'])], ignore_directories=True, case_sensitive=False)

    def on_modified(self, event):

        global rulesEngine

        if os.path.basename(event.src_path) == os.path.basename(settings['files']['areas']):
            rulesEngine.loadAreas(settings['files']['areas'])

        if os.path.basename(event.src_path) == os.path.basename(settings['files']['rules']):
            rulesEngine.loadRules(settings['files']['rules'])
            ad = autoDiscovery()
            ad.rules()


class statistics():

    def __init__(self):
        self.count_flights_hour = 0
        self.count_flights_today = 0
        self.count_flights_lifetime = 0
        self.count_messages_hour = 0
        self.count_messages_today = 0
        self.count_messages_lifetime = 0
        self.time_start = int(time.time())

    def list(self):

        return [
            {"name": "count_flights_hour", "description": "Flight Count Last Hour", "value" : self.count_flights_hour, "type" : "count"},
            {"name": "count_flights_today", "description": "Flight Count Today","value" : self.count_flights_today, "type" : "count"},
            {"name": "count_flights_lifetime", "description": "Flight Count Total","value" : self.count_flights_lifetime, "type" : "count"},
            {"name": "count_messages_hour", "description": "Message Count Last Hour","value" : self.count_messages_hour, "type" : "count"},
            {"name": "count_messages_today", "description": "Message Count Today","value" : self.count_messages_today, "type" : "count"},
            {"name": "count_messages_lifetime", "description": "Message Count Total","value" : self.count_messages_lifetime, "type" : "count"},
            {"name": "time_start", "description": "Start Time","value" : self.time_start, "type" : "timestamp"},
            {"name": "uptime", "description": "Uptime","value" : int(time.time() - self.time_start), "type" : "uptime"}
        ]

    def reset_today(self):
        self.count_flights_today = 0
        self.count_messages_today = 0
        self.reset_hour()


    def reset_hour(self):
        self.count_flights_hour = 0
        self.count_messages_hour = 0


    def increment_flights_count(self):
        self.count_flights_hour = self.count_flights_hour + 1
        self.count_flights_today = self.count_flights_today + 1
        self.count_flights_lifetime = self.count_flights_lifetime + 1


    def increment_message_count(self):
        self.count_messages_hour = self.count_messages_hour + 1
        self.count_messages_today = self.count_messages_today + 1
        self.count_messages_lifetime = self.count_messages_lifetime + 1


    def publish(self):

        ## Publishes the current value of each statistic tracked, if MQTT is connected

        if not mqttClient.is_connected():
            return

        for stat in self.list():
            mqttClient.publish(settings["mqtt"]["topic_statistics"] + stat['name'], stat['value'])
 

class autoDiscovery():

    def __init__(self):

        self.device = {
                "ids" : applicationName,
                "name": applicationName,
                "manufacturer" : "P5Software, LLC"
            }


    def __publish__(self, topic, payload, retain = True):

        if mqttClient.is_connected() == True:
            mqttClient.publish(topic=topic, payload=json.dumps(payload), retain=retain)


    def rules(self):
        
        for rule in rulesEngine.observed_rules:

            payload = {
                "availability_topic" : settings['mqtt']['topic_status'],
                "payload_available" : "ONLINE",
                "payload_not_available" : "OFFLINE",
                "state_topic" : settings["mqtt"]["topic_rule"] + rule['identifier'],
                "name" : "Rule " + rule['name'],
                "unique_id" : applicationName + "_rule_" + rule['identifier'],
                "object_id" : applicationName + "_rule_" + rule['identifier'],
                "device" : self.device,
                "expire_after" : 30,
                "icon" : "mdi:airplane-alert",
                "value_template" : "{{ value_json.aircraft.registration }}",
                "json_attributes_topic" : settings["mqtt"]["topic_rule"] + rule['identifier']
            }

            topic = settings['mqtt']['topic_home_assistant_autodiscovery'] + "sensor/" + applicationName + "_rule_"+ rule['identifier'] + "/config"

            self.__publish__(topic, payload)

        for rule in rulesEngine.removed_rules:

            topic = settings['mqtt']['topic_home_assistant_autodiscovery'] + "sensor/" + applicationName + "_rule_"+ rule['identifier'] + "/config"
            self.__publish__(topic, "")



    def stats(self):

        for stat in stats.list():

            payload = {
                "availability_topic" : settings['mqtt']['topic_status'],
                "payload_available" : "ONLINE",
                "payload_not_available" : "OFFLINE",
                "state_topic" : settings["mqtt"]["topic_statistics"] + stat['name'],
                "name" : stat['description'],
                "unique_id" : applicationName + "_" + stat['name'],
                "object_id" : applicationName + "_" + stat['name'],
                "device" : self.device
            }

            if stat['type'] == "timestamp":
                payload['icon'] = "mdi:clock"
                payload['value_template'] = "{{ ( value | int ) | timestamp_utc }}"
                payload['enabled_by_default'] = False

            if stat['type'] == "uptime":
                payload['icon'] = "mdi:clock"
                payload['value_template'] = "{% set time = (value | int) | int %} " \
                    "{% set minutes = ((time % 3600) / 60) | int %} " \
                    "{% set hours = ((time % 86400) / 3600) | int %} " \
                    "{% set days = (time / 86400) | int %} " \
                    "{%- if time < 60 -%} " \
                    "Less than a minute " \
                    "{%- else -%} " \
                    "{%- if days > 0 -%} " \
                        "{{ days }}d " \
                    "{%- endif -%} " \
                    "{%- if hours > 0 -%} " \
                        "{%- if days > 0 -%} " \
                        "{{ ' ' }} " \
                        "{%- endif -%} " \
                        "{{ hours }}h " \
                    "{%- endif -%} " \
                    "{%- if minutes > 0 -%} " \
                        "{%- if days > 0 or hours > 0 -%} " \
                        "{{ ' ' }} " \
                        "{%- endif -%} " \
                        "{{ minutes }}m " \
                    "{%- endif -%} " \
                    "{%- endif -%}"

            if stat['type'] == "count":
                payload['icon'] = "mdi:broadcast"
                payload['state_class'] = "total_increasing"

            topic = settings['mqtt']['topic_home_assistant_autodiscovery'] + "sensor/" + applicationName + "_" + stat['name'] +"/config"

            self.__publish__(topic, payload)


    def status(self):

        payload = {
            "availability_topic" : settings['mqtt']['topic_status'],
            "payload_available" : "ONLINE",
            "payload_not_available" : "OFFLINE",
            "state_topic" : settings['mqtt']['topic_status'],
            "name" : applicationName + " Application Status",
            "unique_id" : applicationName + "_status",
            "icon" : "mdi:lan-connect",
            "device" : self.device
        }

        topic = settings['mqtt']['topic_home_assistant_autodiscovery'] + "sensor/" + applicationName + "_status/config"

        self.__publish__(topic, payload)

    
if __name__ == "__main__":

    signal.signal(signal.SIGTERM, handle_interrupt)
    setup()
    main()