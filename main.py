#!/usr/bin/env python3

from importlib.resources import path
import os
import logging
import logging.handlers as handlers
import json
import sys
import sqlite3
import traceback
import pyModeS as pms                               #pip3 install pyModeS
from pymongo import MongoClient                    #pip3 install pymongo
import uuid
import time
import datetime
from datetime import datetime, timedelta
import threading
from threading import Thread, current_thread
import requests
import paho.mqtt.client                             #pip3 install paho-mqtt
import signal
from rulesEngine import rulesEngine as skyFollowerRE
from watchdog.observers import Observer             #pip3 install watchdog
from watchdog.events import PatternMatchingEventHandler
import schedule
import queue
import multiprocessing
import socket
import random
import re


def handle_interrupt(signal, frame):
    raise sigKill("SIGKILL Requested")


class sigKill(Exception):
    pass


class noQueueReaderThreadsAvailable(Exception):
    pass

class adsbConnectFailure(Exception):
    pass


class StoppableThread(threading.Thread):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class ADSBClient(StoppableThread):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connected = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def run(self):

        try:

            self.socket.connect((settings['adsb']['uri'], settings['adsb']['port']))
            self.connected = True

            while self.connected == True:

                if self.stopped() != False:
                    break

                data = self.socket.recv(4096)
                messages = []
                msg_stop = False
                self.current_msg = ""

                for b in data:
                    if b == 59:
                        msg_stop = True
                        ts = time.time()
                        messages.append([self.current_msg, ts])
                    if b == 42:
                        msg_stop = False
                        self.current_msg = ""

                    if (not msg_stop) and (48 <= b <= 57 or 65 <= b <= 70 or 97 <= b <= 102):
                        self.current_msg = self.current_msg + chr(b)

                data = []

                for msg in messages:
                    stats.increment_message_count()
                    messageQueue.put(msg)
                    stats.set_message_queue_depth(messageQueue.qsize())

            self.socket.close()
            logger.debug("ADS-B socket closed.")

        except ConnectionRefusedError as ex:
            logger.error("Connection to ADS-B Receiver " + settings['adsb']['uri'] + ":" +  str(settings['adsb']['port']) + " was refused.")

        except Exception as ex:
            logger.error("Exception in adsbClient of type: " + type(ex).__name__ + ": " + str(ex))


def messageQueueReader():

    while threading.current_thread().stopped() == False:

        try:
            messageProcessor(messageQueue.get())
            messageQueue.task_done()

        except queue.Empty:
            pass


def messageProcessor(objMsg):

    try:

        handlingWaitTime = int((datetime.now().timestamp() - objMsg[1])*1000)
        stats.set_message_handling_high_water_mark(handlingWaitTime)

        #Reduce message consumption as the queue gets deeper and the hardware can't keep up
        if 1000 < handlingWaitTime >= 850:      #Throttle 5%
            if random.randint(1, 20) == 1:
                stats.increment_throttled_message_count()
                return

        if 1500 < handlingWaitTime >= 1000:     #Throttle 10%
            if random.randint(1, 10) == 1:
                stats.increment_throttled_message_count()
                return

        if 2000 < handlingWaitTime >= 1500:     #Throttle 20%
            if random.randint(1, 5) == 1:
                stats.increment_throttled_message_count()
                return

        if handlingWaitTime >= 2000:            #Throttle 33%
            if random.randint(1, 3) == 1:
                stats.increment_throttled_message_count()
                return

        #Object to store data
        data = {}

        #Ensure the message appears to be valid and is not corrupted
        if len(objMsg[0]) < 14:
            return  

        #Get the download format
        data['downlink_format'] = pms.df(objMsg[0])

        if data['downlink_format'] not in [5, 21, 17]:
            return
                    
        data['icao_hex'] = pms.adsb.icao(objMsg[0])

        #Ensure we have an icao_hex
        if data['icao_hex'] == None:
            return

        if data['downlink_format'] in [5, 21]:
            data['squawk'] = pms.common.idcode(objMsg[0])

        if data['downlink_format'] == 17:
            
            typeCode = pms.adsb.typecode(objMsg[0])
            data['messageType'] = typeCode

            #Throw away TC 28 and 29...not yet supported
            if typeCode in [28, 29]:
                return

            if 1 <= typeCode <= 4:
                data['ident'] = pms.adsb.callsign(objMsg[0]).replace("_","")
                data['category'] = pms.adsb.category(objMsg[0])                    

            if 5 <= typeCode <= 18 or 20 <= typeCode <=22:
                data['latitude'] = pms.adsb.position_with_ref(objMsg[0], settings['latitude'], settings['longitude'])[0]
                data['longitude'] = pms.adsb.position_with_ref(objMsg[0], settings['latitude'], settings['longitude'])[1]
                data['altitude'] = pms.adsb.altitude(objMsg[0])

            if 5 <= typeCode <= 8:
                data['velocity'] = pms.adsb.velocity(objMsg[0])[0]
                data['heading'] = pms.adsb.velocity(objMsg[0])[1]
                data['vertical_speed'] = pms.adsb.velocity(objMsg[0])[2]

            if typeCode == 19:
                data['velocity'] = pms.adsb.velocity(objMsg[0])[0]
                data['heading'] = pms.adsb.velocity(objMsg[0])[1]
                data['vertical_speed'] = pms.adsb.velocity(objMsg[0])[2]

            if typeCode == 31:
                data['adsb_version'] = pms.adsb.version(objMsg[0])

        flight = Flight()
        flight.setIcao_hex(data['icao_hex'])

        if not flight.exists:

            stats.increment_flights_count()

            flight.first_message = objMsg[1]
        
        flight.last_message = objMsg[1]
        flight.total_messages = flight.total_messages + 1

        if "latitude" in data and "longitude" in data and "altitude" in data:
            flight.addPosition(Position(objMsg[1], data['latitude'], data['longitude'], data['altitude']))

        if "velocity" in data and "heading" in data and "vertical_speed" in data:
            flight.addVelocity(Velocity(objMsg[1], data['velocity'], data['heading'], data['vertical_speed']))

        if "squawk" in data:
            flight.setSquawk(data['squawk'])

        if "category" in data:
            flight.setCategory(data['category'])

        if "ident" in data:
            flight.setIdent(data['ident'])

        if "adsb_version" in data:
            flight.setAdsbVersion(data['adsb_version'])

        flight.evaluateRules()

        flight.saveLocal()

    except Exception as ex:
        logger.error("Exception of type: " + type(ex).__name__ + " while processing message [" + str(objMsg[0]) + "] : " + str(ex))
        pass
          

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

    if rc != 0:
        logger.warning("Failed to connect to MQTT.  Response code: " + str(rc) + ".")

    else:
        logger.info("MQTT connected to " + settings["mqtt"]["uri"] + ".")

        mqtt_publishOnline()
        mqtt_publishAutoDiscovery()
        stats.publish()
        

def exitApp(exitCode=None):

    if exitCode is None:
        exitCode = 0

    #Commit the database if it is not memory
    if 'local_database_mode' in settings:
        if settings['local_database_mode'] == "disk":
            logger.info("Committing database to disk.")
            localDb.commit()

    if exitCode == 0:
        logger.info(applicationName + " application finished successfully.")
        sys.exit(exitCode)

    if exitCode != 0:
        logger.info("Error; Exiting with code " + str(exitCode))
        os._exit(exitCode)  
    

def setLogLevel(logLevel):

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
    global messageQueue

    applicationName = "SkyFollower"
    settings = {}

    stats = statistics()

    try:

        filePath = os.path.dirname(os.path.realpath(__file__))

        logger = logging.getLogger(applicationName)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
        logHandler = handlers.RotatingFileHandler(os.path.join(filePath, 'events.log'), maxBytes=10485760, backupCount=1)
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
        logger.setLevel(logging.INFO)

        if os.path.exists(os.path.join(filePath, 'settings.json')) == False:
            raise Exception("Settings file does not exist.  Expected file " + os.path.join(filePath, 'settings.json'))

        with open(os.path.join(filePath, 'settings.json')) as settingsFile:
            settings = json.load(settingsFile)

        if "log_level" in settings:
            settings['log_level'] = settings['log_level'].lower()
        else:
            settings['log_level'] = "info"

        setLogLevel(settings['log_level'])

        logger.info(applicationName + " application started.")
        logger.debug("Python Version: " + str(sys.version))

        #if sys.version_info.major == 3 and sys.version_info.minor < 10:
        #    logger.warning("Current Python Version " + str(sys.version_info.major) + "." + str(sys.version_info.minor) + " is below the recommended 3.10.  See readme for further information.")
        
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

        if settings['adsb']['type'].lower() not in ['raw']:
            raise Exception ("Unknown adsb -> type in settings.json.  Valid values are raw")

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

        settings['queue_reader_thread_count'] = 1 #Hard-coding for now, because multi-threading causes performances issues

        #if "queue_reader_thread_count" not in settings:
        #    settings['queue_reader_thread_count'] = multiprocessing.cpu_count()
        #else:
        #    if str(settings['queue_reader_thread_count']).isnumeric() != True:
        #        raise Exception ("Invalid queue_reader_thread_count in settings.json")

        #    if settings['queue_reader_thread_count'] < 1:
        #        raise Exception ("Setting 'queue_reader_thread_count' cannot be less than 1.")

        #    if settings['queue_reader_thread_count'] > multiprocessing.cpu_count():
        #        logger.warning("Setting 'queue_reader_thread_count' is set to " + str(settings['queue_reader_thread_count']) + ", which is greater than the CPU count of " + str(multiprocessing.cpu_count()))

        #logger.debug("Queue Reader Thread Count: " + str(settings['queue_reader_thread_count']) + " CPU Count: " + str(multiprocessing.cpu_count()))
            
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

            settings['mqtt']['topic_status'] = str(os.path.join(settings['mqtt']['topic'], "status"))
            settings['mqtt']['topic_rule'] = str(os.path.join(settings['mqtt']['topic'], "rule/"))
            settings['mqtt']['topic_statistics'] = str(os.path.join(settings['mqtt']['topic'], "statistic/"))

            mqttClient = paho.mqtt.client.Client()

        if 'local_database_mode' not in settings:
            settings['local_database_mode'] = "memory"

        if "mongoDb" not in settings:
            raise Exception ("mongoDb object is missing from settings.json")

        if "enabled" not in settings['mongoDb']:
            settings['mongoDb']['enabled'] = True
            logger.info("mongoDb -> enabled is missing in the settings file; MongoDB persistence will be enabled.")
            
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
                raise Exception ("Missing operators -> uri in settings.json")

            if "$IDENT$" not in settings['operators']['uri']:
                raise Exception ("Missing $IDENT$ text in operators -> uri in settings.json")

            if "x-api-key" not in settings['operators']:
                raise Exception ("Missing operators -> x-api-key in settings.json")

        if 'flights' not in settings:
            logger.info("flights is not declared in the settings file; Retrieving flight info will be disabled.")

            settings['flights'] = {}

            settings['flights']['enabled'] = False

        else:

            if 'enabled' not in settings['flights']:
                settings['flights']['enabled'] = False

            if settings['flights']['enabled'] == False:
                logger.info("Flights is disabled in the settings file; Retrieving flight info will be disabled.")

            if "uri" not in settings['flights']:
                raise Exception ("Missing flights -> uri in settings.json")

            if "$IDENT$" not in settings['flights']['uri']:
                raise Exception ("Missing $IDENT$ text in flights -> uri in settings.json")

            if "x-api-key" not in settings['flights']:
                raise Exception ("flights operators -> x-api-key in settings.json")

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
            localDb = sqlite3.connect(":memory:", check_same_thread=False)

        else:
            settings['local_database_mode'] = "disk"
            logger.debug("Using disk for localDb.")
            settings['database_file'] = os.path.join(filePath, applicationName + ".db")

            #Create the localDB tables
            if os.path.exists(settings['database_file']):
                    
                #Delete the old database
                os.remove(settings['database_file'])

            if os.path.exists(settings['database_file'] + "-journal"):
                    
                #Delete the old database
                os.remove(settings['database_file'] + "-journal")

            localDb = sqlite3.connect(settings['database_file'], check_same_thread=False)

        localDb.row_factory = sqlite3.Row        
        cursor = localDb.cursor()

        #Create the temporary tables
        cursor.execute("CREATE TABLE flights (icao_hex text NOT NULL, first_message real NOT NULL, last_message real, total_messages integer, aircraft text, ident text, operator text, squawk text, origin text, destination text, matched_rules text, PRIMARY KEY(icao_hex))")
        cursor.execute("CREATE TABLE positions (icao_hex text, timestamp real, latitude real, longitude real, altitude integer)")
        cursor.execute("CREATE TABLE velocities (icao_hex text, timestamp real, velocity integer, heading real, vertical_speed integer)")
        cursor.execute("CREATE INDEX positions_icao_hex ON positions (icao_hex);")
        cursor.execute("CREATE INDEX velocities_icao_hex ON velocities (icao_hex);")

        #Setup the message queue
        messageQueue = queue.Queue()

        stats.message_queue = messageQueue

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

        threading.excepthook = threadExceptHook
        
        threadsMessageQueueReader = []

        for i in range(settings['queue_reader_thread_count']):
            queueReaderThread = StoppableThread(target=messageQueueReader, name="MessageQueueReader_" + str(i), daemon=True)
            threadsMessageQueueReader.append(queueReaderThread)
            queueReaderThread.start()

        adsb_client = ADSBClient()
        threadADSBClient = Thread(name="ADSB Client", target=adsb_client.run, daemon=True)
        threadADSBClient.start()

        #Start the threads
        if settings['mqtt']['enabled'] == True:
            mqttClient.loop_start()

        flight = Flight()

        schedule.every().hour.at("00:30").do(stats.reset_hour)
        schedule.every().day.at("00:00").do(stats.reset_today)
        schedule.every(30).seconds.do(stats.publish)
        schedule.every(10).seconds.do(flight.persistStaleFlights)
        schedule.every(10).seconds.do(checkQueueReaderThreads, threadsMessageQueueReader)
       
        threadScheduler = threading.Thread(name="scheduled_tasks", target=run_scheduled_tasks, daemon=True)
        threadScheduler.start()

        observer = Observer()
        file_changed_event_handler = fileChanged()
        observer.schedule(file_changed_event_handler, path=os.path.dirname(settings['files']['areas']))
        observer.schedule(file_changed_event_handler, path=os.path.dirname(settings['files']['rules']))
        observer.start()

        #Run forever
        threadADSBClient.join()

        #Exit with an error code
        exitApp(2)

    except adsbConnectFailure as ex:
        exitApp(3)

    except (sigKill, KeyboardInterrupt) as ex:

        logger.debug("Number of active message queue reader threads at exit: " + str(checkQueueReaderThreads(threadsMessageQueueReader)))
        logger.info("Shutdown was requested.")

        for job in schedule.get_jobs():
            schedule.cancel_job(job)

        if settings['mqtt']['enabled'] == True:
            mqttClient.publish(settings["mqtt"]['topic_status'], "TERMINATING")

        if adsb_client.connected == True:
            adsb_client.stop()

        while checkQueueReaderThreads(threadsMessageQueueReader) > 0 and messageQueue.qsize() > 0:
            logger.info("Waiting for the queue readers to empty the message queue.  Current queue size is " + str(messageQueue.qsize()))
            time.sleep(1)

        flight = Flight()
        flight.persistStaleFlights(True)
     
        if settings['mqtt']['enabled'] == True and mqttClient.is_connected():
            mqttClient.loop_stop()

        for messageQueueReaderThread in threadsMessageQueueReader:
            if messageQueueReaderThread.is_alive() == True:
                messageQueueReaderThread.stop()

        exitApp(0)

    except Exception as ex:
        logger.error("Exception of type: " + type(ex).__name__ + " in main(): " + str(ex))
        pass


def threadExceptHook(args):

    if args.exc_type == noQueueReaderThreadsAvailable:
        logger.critical(str(args.exc_value))

        for job in schedule.get_jobs():
            schedule.cancel_job(job)

        if settings['mqtt']['enabled'] == True:
            mqttClient.publish(settings["mqtt"]['topic_status'], "TERMINATING")

        flight = Flight()
        flight.persistStaleFlights(True)
     
        if settings['mqtt']['enabled'] == True and mqttClient.is_connected():
            mqttClient.loop_stop()

        exitApp(4)

    #Handle all other errors by writing them to the log
    logger.critical(traceback.format_tb(args.exc_traceback))
    logger.critical(str(args))


def checkQueueReaderThreads(threadsMessageQueueReader):

    working_threads = 0

    for thread in threadsMessageQueueReader:

        if thread.is_alive():
            working_threads = working_threads + 1
            continue

    if working_threads < settings['queue_reader_thread_count']:
        logger.critical("A queue reader thread has terminated.  Current queue reader thread count is: " + str(working_threads))
        settings['queue_reader_thread_count'] = working_threads

    if working_threads == 0:
        raise noQueueReaderThreadsAvailable("No remaining queue reader threads to process incoming messages.")

    return working_threads


class Flight():
    """Flight Record"""

    def __init__(self) -> None:
        self.exists:bool = False
        self.icao_hex:str = ""
        self.first_message:int = 0
        self.last_message:int = 0
        self.total_messages:int = 0
        self.aircraft:dict = {}
        self.ident:str = ""
        self.operator:dict = {}
        self.squawk:str = ""
        self.origin:dict = {}
        self.destination:dict = {}
        self.positions:list[Position] = []
        self.velocities:list[Velocity] = []
        self.matched_rules:list[str] = []


    def toDict(self) -> dict:

        record = {}
        record['icao_hex'] = self.icao_hex
        record['first_message'] = datetime.utcfromtimestamp(self.first_message)
        record['last_message'] = datetime.utcfromtimestamp(self.last_message)
        record['total_messages'] = self.total_messages

        if self.aircraft != {}:
            record['aircraft'] = self.aircraft
        else:
            record['aircraft'] = {}
            record['aircraft']['icao_hex'] = self.icao_hex

        if self.ident != "":
            record['ident'] = self.ident

        if self.operator != {}:
            record['operator'] = self.operator

        if self.squawk != "":
            record['squawk'] = self.squawk

        record['origin'] = self.origin

        record['destination'] = self.destination         

        if len(self.matched_rules) > 0 and settings['log_level'] == "debug":
            record['matched_rules'] = self.matched_rules

        if len(self.positions) > 0:
            record['positions'] = []

            for position in self.positions:
                record['positions'].append(position.toDict())

        if len(self.velocities) > 0:
            record['velocities'] = []

            for velocity in self.velocities:
                record['velocities'].append(velocity.toDict())

        return record


    def get(self, limit_position:bool = True, limit_velocity:bool = True):
        """Retrieves the given ICAO HEX from the local database.
        If no records are found, False is returned.
        If records are returned, True is returned and the object is populated from the database.
        """

        sqliteCur = localDb.cursor()

        sqliteCur.execute("SELECT icao_hex, first_message, last_message, total_messages, aircraft, ident, operator, squawk, origin, destination, matched_rules  FROM flights WHERE icao_hex='" + self.icao_hex + "'")
        result = sqliteCur.fetchall()

        if len(result) == 0:
            self.exists = False
            logger.debug("ICAO HEX " + self.icao_hex + " will be added to localDb.")
            return

        self.exists = True
        self.icao_hex = result[0]['icao_hex']
        self.first_message = result[0]['first_message']
        self.last_message = result[0]['last_message']
        self.total_messages = result[0]['total_messages']
        self.aircraft = json.loads(result[0]['aircraft'])
        self.ident = result[0]['ident']
        self.operator = json.loads(result[0]['operator'])
        self.squawk = result[0]['squawk']
        self.origin = json.loads(result[0]['origin'])
        self.destination = json.loads(result[0]['destination'])
        self.matched_rules = json.loads(result[0]['matched_rules'])
        self.positions = []
        self.velocities = []

        self._getPositions(limit_position)
        self._getVelocities(limit_velocity)    
        
        return True


    def saveLocal(self):
       
        """Saves the flight data to the localDb."""
        sqliteCur = localDb.cursor()
        sqlStatement = "REPLACE INTO flights (icao_hex, first_message, last_message, total_messages, aircraft, ident, operator, squawk, origin, destination, matched_rules) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        parameters = (self.icao_hex, self.first_message, self.last_message, self.total_messages, json.dumps(self.aircraft), self.ident, json.dumps(self.operator), self.squawk, json.dumps(self.origin), json.dumps(self.destination), json.dumps(self.matched_rules))
        sqliteCur.execute(sqlStatement, parameters)


    def persist(self):
        """Persists the data to the remote data store."""

        if settings['mongoDb']['enabled'] == False:
            return

        mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
        adsbDB = mongoDBClient[settings['mongoDb']['database']]
        adsbDBCollection = adsbDB[settings['mongoDb']['collection']]

        record = self.toDict()
        record['_id'] = str(uuid.uuid4())

        if "icao_hex" in record:
            record.pop("icao_hex")

        if "military" in record['aircraft']:
            if record['aircraft']['military'] == False:
                record['aircraft'].pop("military")

        if "icao_code" in self.origin:
            record['origin'] = self.origin['icao_code']
        else:
            record.pop('origin')

        if "icao_code" in self.destination:
            record['destination'] = self.destination['icao_code']
        else:
            record.pop('destination')

        if "source" in self.operator:
            record['operator'].pop("source")
       
        adsbDBCollection.insert_one(record)

        logger.debug("Persisted record _id: " + record['_id'] + " ICAO HEX: " + record['aircraft']['icao_hex'])
        

    def delete(self):
        """Deletes the object from the localDb."""

        sqliteCur = localDb.cursor()
        sqliteCur.execute("DELETE FROM flights WHERE icao_hex ='" + self.icao_hex + "'")
        sqliteCur.execute("DELETE FROM positions WHERE icao_hex ='" + self.icao_hex + "'")
        sqliteCur.execute("DELETE FROM velocities WHERE icao_hex ='" + self.icao_hex + "'")


    def _getPositions(self, limit:bool=True):
        """Retrieves position reports for the current aircraft.
        If limit is True, only the last message is returned."""

        sql = "SELECT timestamp, latitude, longitude, altitude FROM positions WHERE icao_hex='" + self.icao_hex + "' ORDER BY timestamp"

        if limit == True:
            sql = sql + " DESC LIMIT 1"

        sqliteCur = localDb.cursor()
        sqliteCur.execute(sql)
        results = sqliteCur.fetchall()

        if results == None or len(results) < 1:
            return

        for result in results:
            self.positions.append(Position(result['timestamp'], result['latitude'], result['longitude'], result['altitude']))


    def addPosition(self, position:'Position'):
        
        sqliteCur = localDb.cursor()
        sqlStatement = "INSERT INTO positions (icao_hex, timestamp, latitude, longitude, altitude) VALUES (?,?,?,?,?)"
        parameters = (self.icao_hex, position.timestamp, position.latitude, position.longitude, position.altitude)
        sqliteCur.execute(sqlStatement, parameters)

        self.positions.append(position)


    def addVelocity(self, velocity:'Velocity'):
        
        sqliteCur = localDb.cursor()
        sqlStatement = "INSERT INTO velocities (icao_hex, timestamp, velocity, heading, vertical_speed) VALUES (?,?,?,?,?)"
        parameters = (self.icao_hex, velocity.timestamp, velocity.velocity, velocity.heading, velocity.vertical_speed)
        sqliteCur.execute(sqlStatement, parameters)

        self.velocities.append(velocity)


    def _getVelocities(self, limit:bool=True):
        """Retrieves velocity reports for the current aircraft.
        If limit is True, only the last message is returned."""

        sql = "SELECT * FROM velocities WHERE icao_hex='" + self.icao_hex + "' ORDER BY timestamp"

        if limit == True:
            sql = sql + " DESC LIMIT 1"

        sqliteCur = localDb.cursor()
        sqliteCur.execute(sql)
        results = sqliteCur.fetchall()

        for result in results:
            self.velocities.append(Velocity(result['timestamp'], result['velocity'], result['heading'], result['vertical_speed']))


    def _getAircraft(self):

        if settings['registration']['enabled'] != True:
            return

        if self.aircraft != {}:
            return

        self.aircraft['icao_hex'] = self.icao_hex

        r = requests.get(settings['registration']['uri'].replace("$ICAO_HEX$", str(self.icao_hex)), headers={'x-api-key': settings['registration']['x-api-key']})

        if r.status_code == 200:
            self.aircraft = json.loads(r.text)
            return 

        if r.status_code == 404:
            logger.debug("Unable to get registration details for " + str(self.icao_hex) +"; _getAircraft returned " + str(r.status_code))
            stats.increment_registration_unknown_count()
            return

        logger.info("Unable to get registration details for " + str(self.icao_hex) +"; _getAircraft returned " + str(r.status_code))
        stats.increment_registration_unknown_count()
        return

    def setIdent(self, value:str):

        value = value.strip()
       
        if self.ident != "":
            return

        if value == "" or value == "00000000":
            return

        self.ident = value
        self._getFlightInfo()
        self._getOperator()


    def setIcao_hex(self, value:str, limit_position:bool = True, limit_velocity:bool = True):

        value = value.strip()

        if self.icao_hex != "":
            return

        self.icao_hex = value

        self.get(limit_position = limit_position, limit_velocity = limit_velocity)

        if self.exists == False:
            self._getAircraft()


    def setCategory(self, value:int):

        if value == 1:
            self.aircraft['wake_turbulence_category'] = "Light"
            return

        if value == 2:
            self.aircraft['wake_turbulence_category'] = "Medium 1"
            return

        if value == 3:
            self.aircraft['wake_turbulence_category'] = "Medium 2"
            return

        if value == 4:
            self.aircraft['wake_turbulence_category'] = "High Vortex Aircraft"
            return

        if value == 5:
            self.aircraft['wake_turbulence_category'] = "Heavy"
            return

        if value == 6:
            self.aircraft['wake_turbulence_category'] = "High Performance"
            return

        if value == 7:
            self.aircraft['wake_turbulence_category'] = "Rotorcraft"
            return


    def setSquawk(self, value:str):

        value = value.strip()

        if self.squawk != "":
            return

        self.squawk = value
        

    def setAdsbVersion(self, value:int):

        if 'adsb_version' in self.aircraft:
            return

        self.aircraft['adsb_version'] = value

        if self.aircraft['adsb_version'] != "":
            return

        self.aircraft['adsb_version'] = value
    

    def _getOperator(self):

        if settings['operators']['enabled'] != True:
            return

        if "registration" in self.aircraft:
            if self.aircraft['registration'].replace("-", "") == self.ident.replace("-", ""):
                return

        #Filter US-based registration numbers
        if bool(re.match("^N[1-9]((\d{0,4})|(\d{0,3}[A-HJ-NP-Z])|(\d{0,2}[A-HJ-NP-Z]{2}))$", self.ident)):
            return

        if 'military' in self.aircraft:
            if self.aircraft['military'] == True:
                logger.debug("aircraft is military " + str(self.ident))
                return

        value = []

        #Get the identifier from the value (VIR41HK) becomes [VIR,,HK]
        value = ";".join(re.split("[^a-zA-Z]", self.ident))

        value = value.split(";")[0] #Retrieves the first instance (VIR)

        if value == []:
            logger.debug("value is empty " + str(self.ident))
            return

        if len(value) < 2:
            logger.debug("value length is less than 2 " + str(self.ident))
            return

        r = requests.get(settings['operators']['uri'].replace("$IDENT$", value), headers={'x-api-key': settings['operators']['x-api-key']})

        if r.status_code == 200:
            self.operator = r.json()
            return

        if r.status_code == 404:
            logger.debug("Operator details unavailable for " + str(value) +"; service returned " + str(r.status_code))
            stats.increment_operator_unknown_count()
            return
        
        logger.debug("Operator details unavailable for " + str(value) +"; service returned " + str(r.status_code))
        stats.increment_operator_unknown_count()
        return


    def _getFlightInfo(self):
        
        if settings['flights']['enabled'] != True:
            return

        r = requests.get(settings['flights']['uri'].replace("$IDENT$", self.ident), headers={'x-api-key': settings['flights']['x-api-key']})

        if r.status_code == 200:
            self.origin = r.json()['origin']
            self.destination = r.json()['destination']
            self.operator['flight_number'] = r.json()['flight_number']
            return 

        if r.status_code == 404:
            logger.debug("Flight info unavailable for " + self.ident +"; service returned " + str(r.status_code))
            return
        
        logger.info("Flight info unavailable for " + self.ident +"; service returned " + str(r.status_code))
        return


    def evaluateRules(self):

        global rulesEngine

        startTime = datetime.now()

        result = []

        if self.icao_hex not in rulesEngine.evaluating_flights:
            rulesEngine.evaluating_flights.append(self.icao_hex)
            result = rulesEngine.evaluate(self)
            rulesEngine.evaluating_flights.remove(self.icao_hex)

        stats.set_rule_evaluation_high_water_mark(int((datetime.now()-startTime).microseconds/1000))

        for matchedRule in result:

            notification = {}
            notification = self.toDict()

            if "icao_hex" in notification:
                notification.pop("icao_hex")

            if "positions" in notification:
                notification.pop("positions")

            if "velocities" in notification:
                notification.pop("velocities")

            if "operator" in notification:
                if notification['operator'] == {}:
                    notification.pop("operator")

            if "origin" in notification:
                if notification['origin'] == {}:
                    notification.pop("origin")

            if "destination" in notification:
                if notification['destination'] == {}:
                    notification.pop("destination")

            notification['rule'] = {}
            notification['rule']['name'] = matchedRule['name']
            notification['rule']['description'] = matchedRule['description']
            notification['rule']['identifier'] = matchedRule['identifier']
            self.matched_rules.append(matchedRule['identifier'])
            logger.debug("Rule Matched \"" +  matchedRule['name'] + "\" for ICAO HEX: " + self.icao_hex)

            mqtt_publishNotication(notification['rule']['identifier'], json.dumps(notification, default=str))


    def persistStaleFlights(self, all_flights_stale:bool=False):
        """Persists stale flights.  If requested, considers all flights to be stale."""

        sqliteCur = localDb.cursor()

        if all_flights_stale == False:
            logger.debug("Querying for stale flights.")
            sql = "SELECT icao_hex FROM flights WHERE last_message < " + str(datetime.now().timestamp() - timedelta(seconds = settings['flight_ttl_seconds']).total_seconds())
        else:
            logger.info("All flights will be persisted.")
            sql = "SELECT icao_hex FROM flights"

        sqliteCur.execute(sql)
        stale_flights = sqliteCur.fetchall()

        countStaleFlights = len(stale_flights)

        logger.debug("Found " + str(countStaleFlights) + " stale flights to persist.")

        for entry in stale_flights:
            stale_flight = Flight()
            stale_flight.setIcao_hex(entry['icao_hex'], limit_position=False, limit_velocity = False)

            stale_flight.persist()

            if all_flights_stale != True:
                stale_flight.delete()
            

class Position(dict):
    """Position Report"""

    def __init__(self, timestamp:float = None, latitude:float = None, longitude:float = None, altitude:int = None):
        self.timestamp:float = timestamp
        self.latitude:float = latitude
        self.longitude:float = longitude
        self.altitude:int = altitude


    def toDict(self) -> dict:

        return {
            "timestamp" : datetime.utcfromtimestamp(self.timestamp), 
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude": self.altitude
        }


class Velocity(dict):
    """Velocity Report"""

    def __init__(self, timestamp:float = None, velocity:float = None, heading:float = None, vertical_speed:int = None):
        self.timestamp:float = timestamp
        self.velocity:float = velocity
        self.heading:float = heading
        self.vertical_speed:int = vertical_speed
        

    def toDict(self) -> dict:

        return {
            "timestamp" : datetime.utcfromtimestamp(self.timestamp), 
            "velocity": self.velocity,
            "heading": self.heading,
            "vertical_speed": self.vertical_speed
        }
       

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
        self.count_operator_unknown_today = 0
        self.count_operator_unknown_lifetime = 0
        self.count_registration_unknown_today = 0
        self.count_registration_unknown_lifetime = 0
        self.message_handling_high_water_mark_ms = 0
        self.message_queue_depth = 0
        self.rule_evaluation_high_water_mark_ms = 0
        self.count_messages = 0
        self.lastPublished = datetime.now()
        self.count_messages_throttled = 0

    def set_message_handling_high_water_mark(self, value):
        if value > self.message_handling_high_water_mark_ms:
            self.message_handling_high_water_mark_ms = value

    def get_message_count_per_second(self):
        if (datetime.now() - self.lastPublished).seconds > 0:
            return int(self.count_messages / (datetime.now() - self.lastPublished).seconds)
        else:
            return 0

    def set_message_queue_depth(self, value):
        if value > self.message_queue_depth:
            self.message_queue_depth = value

    def set_rule_evaluation_high_water_mark(self, value):
        if value > self.rule_evaluation_high_water_mark_ms:
            self.rule_evaluation_high_water_mark_ms = value       


    def list(self):

        return [
            {"name": "count_flights_hour", "description": "Flight Count Last Hour", "value" : self.count_flights_hour, "type" : "count"},
            {"name": "count_flights_today", "description": "Flight Count Today","value" : self.count_flights_today, "type" : "count"},
            {"name": "count_flights_lifetime", "description": "Flight Count Total","value" : self.count_flights_lifetime, "type" : "count"},
            {"name": "count_messages_hour", "description": "Message Count Last Hour","value" : self.count_messages_hour, "type" : "count"},
            {"name": "count_messages_today", "description": "Message Count Today","value" : self.count_messages_today, "type" : "count"},
            {"name": "count_messages_lifetime", "description": "Message Count Total","value" : self.count_messages_lifetime, "type" : "count"},
            {"name": "count_messages_throttled", "description": "Messages Throttled to Improve Performance","value" : self.count_messages_throttled, "type" : "count"},
            {"name": "count_operator_unknown_today", "description": "Operator Unknown Count Today","value" : self.count_operator_unknown_today, "type" : "count"},
            {"name": "count_operator_unknown_lifetime", "description": "Operator Unknown Count Total","value" : self.count_operator_unknown_lifetime, "type" : "count"},
            {"name": "count_registration_unknown_today", "description": "Registration Unknown Count Today","value" : self.count_registration_unknown_today, "type" : "count"},
            {"name": "count_registration_unknown_lifetime", "description": "Registration Unknown Count Total","value" : self.count_registration_unknown_lifetime, "type" : "count"},
            {"name": "message_handling_high_water_mark_ms", "description": "Message Processing Delay High Water Mark", "value" : self.message_handling_high_water_mark_ms, "type" : "time_ms"},
            {"name": "message_queue_depth", "description": "Message Queue Depth High Water Mark", "value" : self.message_queue_depth, "type" : "queue"},
            {"name": "count_messages_second", "description": "Message Rate", "value" : self.get_message_count_per_second(), "type" : "time_per_sec"},
            {"name": "rule_evaluation_high_water_mark_ms", "description": "Rule Evaluation Duration High Water Mark", "value" : self.rule_evaluation_high_water_mark_ms, "type" : "time_ms"},
            {"name": "time_start", "description": "Start Time","value" : self.time_start, "type" : "timestamp"},
            {"name": "uptime", "description": "Uptime","value" : int(time.time() - self.time_start), "type" : "uptime"}
        ]


    def reset_today(self):
        self.count_flights_today = 0
        self.count_messages_today = 0
        self.count_operator_unknown_today = 0
        self.count_registration_unknown_today = 0
        self.reset_hour()


    def reset_hour(self):
        self.count_flights_hour = 0
        self.count_messages_hour = 0
            

    def reset_on_publish(self):
        self.message_handling_high_water_mark_ms = 0
        self.message_queue_depth = 0
        self.rule_evaluation_high_water_mark_ms = 0
        self.count_messages = 0
        self.count_messages_throttled = 0


    def increment_flights_count(self):
        self.count_flights_hour = self.count_flights_hour + 1
        self.count_flights_today = self.count_flights_today + 1
        self.count_flights_lifetime = self.count_flights_lifetime + 1


    def increment_message_count(self):
        self.count_messages = self.count_messages + 1
        self.count_messages_hour = self.count_messages_hour + 1
        self.count_messages_today = self.count_messages_today + 1
        self.count_messages_lifetime = self.count_messages_lifetime + 1


    def increment_throttled_message_count(self):
        self.count_messages_throttled = self.count_messages_throttled + 1


    def increment_operator_unknown_count(self):
        self.count_operator_unknown_today = self.count_operator_unknown_today + 1
        self.count_operator_unknown_lifetime = self.count_operator_unknown_lifetime + 1


    def increment_registration_unknown_count(self):
        self.count_registration_unknown_today = self.count_registration_unknown_today + 1
        self.count_registration_unknown_lifetime = self.count_registration_unknown_lifetime + 1


    def publish(self):

        for stat in self.list():
            logger.debug("Statistic: " + stat['name'] + ": " + str(stat['value']))

            if mqttClient.is_connected():
                mqttClient.publish(settings["mqtt"]["topic_statistics"] + stat['name'], stat['value'])

        #Reset the statistics
        self.reset_on_publish()
        self.lastPublished = datetime.now()
 

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
                payload['value_template'] = "{% set val = value | int(0) %} "\
                        "{% if val < 1000000 %} "\
                        "{{val}} "\
                        "{% elif val > 1000000 and val < 1000000000 %} "\
                        "{{(val/1000000) | round(2)}}M "\
                        "{% elif val > 1000000000 and val < 1000000000000 %} "\
                        "{{(val/1000000000) | round(2)}}B "\
                        "{% elif val > 1000000000000 %} "\
                        "{{(val/1000000000000) | round(2)}}T "\
                        "{% endif %}"

            if stat['type'] == "queue":
                payload['icon'] = "mdi:tray-full"
                payload['state_class'] = "measurement"

            if stat['type'] == "time_ms":
                payload['unit_of_measurement'] = "ms"
                payload['icon'] = "mdi:clock"
                payload['state_class'] = "measurement"

            if stat['type'] == "time_per_sec":
                payload['unit_of_measurement'] = "m/s"
                payload['icon'] = "mdi:broadcast"
                payload['state_class'] = "measurement"

            if stat['type'] == "measurement":
                payload['state_class'] = "measurement"

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