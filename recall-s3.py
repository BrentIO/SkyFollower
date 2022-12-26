import os
import sys
import json
from pymongo import MongoClient
import logging
import logging.handlers as handlers
import argparse
import boto3
import gzip
from dateutil import parser as dtparser
from datetime import datetime, timedelta


def setup(args):
    global logger
    global applicationName
    global settings

    settings = {}

    try:

        filePath = os.path.dirname(os.path.realpath(__file__)) + "/"

        applicationName = "SkyFollower S3 Recall"

        #Setup the logger, 10MB maximum log size
        logger = logging.getLogger(applicationName)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
        logHandler = handlers.RotatingFileHandler(filePath + 'events.log', maxBytes=10485760, backupCount=1)
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
        logger.setLevel(logging.INFO)

        logger.info(applicationName + " application started.")
        
        #Make sure the settings file exists
        if os.path.exists(filePath + 'settings.json') == False:
            raise Exception("Settings file does not exist.  Expected file " + filePath + 'settings.json')

        #Get the settings file
        with open(filePath + 'settings.json') as settingsFile:
            settings = json.load(settingsFile)

        if "log_level" in settings:
            setLogLevel(settings['log_level'])

        #Validate the required settings exist
        if "mongoDb" not in settings:
            raise Exception ("mongoDb object is missing from settings.json")

        if "enabled" not in settings['mongoDb']:
            settings['mongoDb']['enabled'] = False
            raise Exception ("mongoDB -> enabled is missing from settings.json.  It must be enabled to use this application.")
            
        if settings['mongoDb']['enabled'] == False:
            raise Exception ("mongoDB -> enabled is set to false in settings.json.  It must be enabled to use this application.")
            
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

        if "s3_migration" not in settings:
            raise Exception ("s3_migration object is missing from settings.json")

        if "enabled" not in settings['s3_migration']:
            settings['s3_migration']['enabled'] = False
            raise Exception ("s3_migration -> enabled is missing from settings.json.  It must be enabled to use this application.")

        if settings['s3_migration']['enabled'] == False:
            raise Exception ("s3_migration -> enabled is set to false in settings.json.  It must be enabled to use this application.")

        if "bucket_name" not in settings['s3_migration']: 
            raise Exception ("s3_migration -> bucket_name is missing from settings.json.")

        if settings['s3_migration']['bucket_name'].strip() == "":
            raise Exception ("Invalid s3_migration -> bucket_name in settings.json.")

        if "key" not in settings['s3_migration']: 
            raise Exception ("s3_migration -> key is missing from settings.json.")

        if settings['s3_migration']['key'].strip() == "":
            raise Exception ("Invalid s3_migration -> key in settings.json.")

        if "secret" not in settings['s3_migration']: 
            raise Exception ("s3_migration -> key is missing from settings.json.")

        if settings['s3_migration']['secret'].strip() == "":
            raise Exception ("Invalid s3_migration -> secret in settings.json.")

        if "purge_days" not in settings['s3_migration']:
            settings['s3_migration']['purge_days'] = 30

    except Exception as ex:
        logger.error(ex)
        print(ex)
        exitApp(1)


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


def main(args):
    
    try:

        id = args.id.strip()

        file_name = id

        if not file_name.endswith(".gz"):
            file_name = file_name + ".gz"

        #Retrieve the entire flight document from S3
        flight = json.loads(download_from_s3(file_name))

        if "velocities" in flight:
            for velocity in flight['velocities']:
                velocity['timestamp'] = dtparser.parse(velocity['timestamp'])

        if "positions" in flight:

            for position in flight['positions']:
                position['timestamp'] = dtparser.parse(position['timestamp'])

        logger.debug("Migrated Document to S3: " + id)

        update_flight(flight, id)
      
        #Finished successfully
        exitApp(0)

    except Exception as ex:
        logger.error(ex)
        print(ex)
        exitApp(1)


def update_flight(flight, id):

    mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
    skyFollowerDB = mongoDBClient[settings['mongoDb']['database']]
    flightsCollection = skyFollowerDB[settings['mongoDb']['collection']]

    #Set the migrated date to now and delete the position and velocities arrays
    flightsCollection.update_one({'_id': id}, {"$set":{'expires': (datetime.today()+timedelta(days=settings['s3_migration']['purge_days'])), 'positions': flight['positions'], 'velocities': flight['velocities']}})

    logger.debug("Restored document from S3: " + id)


def download_from_s3(file_name) -> dict:
    
    s3_client = boto3.client('s3', aws_access_key_id=settings['s3_migration']['key'], aws_secret_access_key=settings['s3_migration']['secret'])

    response = s3_client.get_object(Bucket=settings['s3_migration']['bucket_name'], Key=file_name)

    #Data will be compressed on S3, decompress it
    decompressed_body = gzip.decompress(response['Body'].read()).decode('utf-8')

    return decompressed_body


def exitApp(exitCode=None):

    if exitCode is None:
        exitCode = 0

    if exitCode == 0:
        print(applicationName + " application finished successfully.")
        logger.info(applicationName + " application finished successfully.")

    if exitCode != 0:
        logger.info("Error; Exiting with code " + str(exitCode))

    sys.exit(exitCode)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Recalls position and velocity details from S3 and temporarily populates it in SkyFollower\'s MongoDB collection')
    parser.add_argument(dest='id', metavar="id", help='ID of the flight to recall from S3 (ex a550a2e5-019c-4966-bf5e-ac09af52d80c).')

    args = parser.parse_args()

    #Setup the configuration required
    setup(args)

    main(args)