#!/usr/bin/env python3

import os
import logging
import logging.handlers as handlers
import json
import sys
from tinydb import TinyDB, Query                    #pip3 install tinydb
from tinydb.storages import MemoryStorage
import pyModeS as pms                               #pip3 install pyModeS
from pyModeS.extra.tcpclient import TcpClient
from pymongo import MongoClient                     #pip3 install pymongo
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

        #Object to store data
        data = {}

        data['timestamp'] = ts

        #Get the download format
        data['downlink_format'] = pms.df(msg)

        if data['downlink_format'] not in (0, 11, 16, 4, 20, 5, 21, 17):
            logger.info("Unexpected downlink format " + str(data['downlink_format']) + " msg: " + msg)
        
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


def storeMessageLocal(data):

    Record = Query()
    result = localDb.search(Record.icao_hex == data['icao_hex'])

    flight = {}

    if len(result) == 0:
        flight['icao_hex'] = data['icao_hex']
        flight['first_message'] = data['timestamp']
        flight['_id'] = str(uuid.uuid4())
        flight['last_message'] = 0
        flight['total_messages'] = 0
        flight['notified'] = False

        #Get the aircraft data
        aircraftData = getRegistration(flight['icao_hex'])

        #If the registration was returned, store the data
        if aircraftData is not None:
            flight['aircraft'] = aircraftData

        if flight['notified'] == False:
            checkFlightOfInterest(flight)
        
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

        if r.status_code != 200:
            return None
        else:
            return json.loads(r.text)

    except Exception as ex:
        logger.info("Error getting registration.")
        logger.error(ex)
        return None


def getOperator(callsign):

    if settings['operators']['enabled'] != True:
        return None

    try:

        r = requests.get(settings['operators']['uri'].replace("$CALLSIGN$", callsign), headers={'x-api-key': settings['operators']['x-api-key']})

        if r.status_code != 200:
            return None
        else:
            return json.loads(r.text)

    except Exception as ex:
        logger.info("Error getting operator.")
        logger.error(ex)
        return None


def storeMessageRemote():

    t = current_thread()
    t.alive = True

    while True:

        countOfMigrated = 0

        try:

            threadState = t.alive
            Record = Query()

            #Determine if we will continue to live after this round
            if threadState:
                stale_flights = localDb.search(Record.last_message < (datetime.now().timestamp() - timedelta(seconds = settings['flight_ttl_seconds']).total_seconds()))
                mqtt_publishOnline()
            else:
                #Thread is shutting down, persist all the records regardless of their status
                logger.info("Shutdown detected, persisting all local records to MongoDB.")
                stale_flights = localDb.all()

            if len(stale_flights) > 0:

                mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
                adsbDB = mongoDBClient[settings['mongoDb']['database']]

                #An array will be returned, cycle through each flight
                for flight in stale_flights:

                    icao_hex = flight['icao_hex']

                    #Make the datestamps human-readable
                    flight['first_message'] = datetime.utcfromtimestamp(flight['first_message'])
                    flight['last_message'] = datetime.utcfromtimestamp(flight['last_message'])

                    #Delete data we do not want to persist
                    del flight['notified']

                    if 'aircraft' in flight:
                        
                        #ICAO Hex will be repetitive, but only delete it if there is an aircraft object
                        del flight['icao_hex']

                        if 'military' in flight['aircraft']:
                            if flight['aircraft']['military'] == False:
                                del flight['aircraft']['military']

                    adsbDB.flights.insert_one(flight)

                    countOfMigrated = countOfMigrated + 1

                    localDb.remove(Record.icao_hex == icao_hex)
                
                mongoDBClient.close()

                logger.debug("Finished migration to MongoDB.  " + str(countOfMigrated) + " records were migrated.")

            #Determine if we should break out of the loop
            if not threadState:
                break

        except Exception as ex:
            logger.critical("Error migrating data to MongoDB.")
            logger.error(ex)

        finally:
            #Sleep another 10 seconds if the thread is still alive
            if t.alive:
                time.sleep(10)
            else:
                break


def checkFlightOfInterest(flight):


    return


def mqtt_publishNotication(flight):

    if settings['mqtt']['enabled'] != True:
        return

    print(flight)


def mqtt_publishOnline():

    if settings['mqtt']['enabled'] != True:
        return
    
    #Set the status online
    mqttClient.publish(settings["mqtt"]["statusTopic"], "ONLINE")


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

    except Exception as ex:
        logger.error(ex)
        print("Unable to connect to MQTT.")
        print(ex)
        

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

    #Define some constants
    applicationName = "SkyFollower"
    settings = {}

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
            logger.warning("Setting 'flight_ttl_seconds' not declared in the settings file; Defaulting to 300 seconds.")
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
            raise Exception ("Configure the latitude and longitude in settings.json.")

        if 'mqtt' not in settings:
            logger.warning("mqtt is not declared in the settings file; MQTT will be disabled.")

            settings['mqtt'] = {}

            settings['mqtt']['enabled'] = False

        else:
            settings['mqtt']['enabled'] = True

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

            if "statusTopic" not in settings['mqtt']:
                raise Exception ("Missing mqtt -> statusTopic in settings.json")

            if settings['mqtt']['statusTopic'] == "":
                raise Exception ("Empty mqtt -> statusTopic in settings.json")

            if "notificationTopic" not in settings['mqtt']:
                raise Exception ("Missing mqtt -> notificationTopic in settings.json")

            if settings['mqtt']['notificationTopic'] == "":
                raise Exception ("Empty mqtt -> notificationTopic in settings.json") 

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

        if 'registration' not in settings:
            logger.warning("registration is not declared in the settings file; Retrieving registrations will be disabled.")

            settings['registration'] = {}

            settings['registration']['enabled'] = False

        else:
            settings['registration']['enabled'] = True

            if "registration" not in settings:
                raise Exception ("registration object is missing from settings.json")

            if "uri" not in settings['registration']:
                raise Exception ("Missing registration -> uri in settings.json")

            if "$ICAO_HEX$" not in settings['registration']['uri']:
                raise Exception ("Missing $ICAO_HEX$ text in registration -> uri in settings.json")

            if "x-api-key" not in settings['registration']:
                raise Exception ("Missing registration -> x-api-key in settings.json")

        if 'operators' not in settings:
            logger.warning("operators is not declared in the settings file; Retrieving operators will be disabled.")

            settings['operators'] = {}

            settings['operators']['enabled'] = False

        else:
            settings['operators']['enabled'] = True
        
            if "operators" not in settings:
                raise Exception ("operators object is missing from settings.json")

            if "uri" not in settings['operators']:
                raise Exception ("Missing registration -> uri in settings.json")

            if "$CALLSIGN$" not in settings['operators']['uri']:
                raise Exception ("Missing $CALLSIGN$ text in operators -> uri in settings.json")

            if "x-api-key" not in settings['operators']:
                raise Exception ("Missing operators -> x-api-key in settings.json")

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


def main():

    try:

        if settings['mqtt']['enabled'] == True:
    
            #Setup the handlers for connection and messages
            mqttClient.on_connect = mqtt_onConnect

            #Create the MQTT credentials from the settings file
            mqttClient.username_pw_set(settings["mqtt"]["username"], password=settings["mqtt"]["password"])

            #Set the last will and testament
            mqttClient.will_set(settings["mqtt"]["statusTopic"], payload="OFFLINE", qos=0, retain=True)

            #Connect to MQTT
            mqttClient.connect_async(settings["mqtt"]["uri"], port=settings["mqtt"]["port"], keepalive=60)

        # run new client, change the host, port, and rawtype if needed
        adsb_client = ADSBClient()

        dbCleaner = threading.Thread(name="storeMessageRemote", target=storeMessageRemote)

        #Start the threads
        if settings['mqtt']['enabled'] == True:
            mqttClient.loop_start()

        dbCleaner.start()

        adsb_client.run() #Blocking, must be last            

        exitApp(0)

    except sigKill:

        if settings['mqtt']['enabled'] == True:
            mqttClient.publish(settings["mqtt"]["statusTopic"], "TERMINATING")

        dbCleaner.alive = False
        logger.info("Attempting to shutdown, waiting for remote storage thread to be terminated.")
        dbCleaner.join()

        if settings['mqtt']['enabled'] == True:
            mqttClient.loop_stop()

        logger.info("Remote storage thread terminated.")
        exitApp(0)       


    except KeyboardInterrupt:

        if settings['mqtt']['enabled'] == True:
            mqttClient.publish(settings["mqtt"]["statusTopic"], "TERMINATING")

        dbCleaner.alive = False
        logger.info("Attempting to shutdown, waiting for remote storage thread to be terminated.")
        dbCleaner.join()

        if settings['mqtt']['enabled'] == True:
            mqttClient.loop_stop()

        logger.info("Remote storage thread terminated.")
        exitApp(0)       

    except Exception as ex:
        logger.error(ex)
        exitApp(1)


if __name__ == "__main__":

    signal.signal(signal.SIGTERM, handle_interrupt)
    setup()
    main()