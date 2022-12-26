import os
import sys
import json
from pymongo import MongoClient
import logging
import logging.handlers as handlers
import argparse
import boto3
from botocore.exceptions import ClientError
import gzip
from datetime import date, datetime, timedelta


def setup(args):
    global logger
    global applicationName
    global settings

    settings = {}

    try:

        filePath = os.path.dirname(os.path.realpath(__file__)) + "/"

        applicationName = "SkyFollower S3 Migration"

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

        if "migrate_days" not in settings['s3_migration']:
            settings['s3_migration']['migrate_days'] = 90

        if str(settings['s3_migration']['migrate_days']).isnumeric() == False:
            raise Exception ("Non-numeric value for s3_migration -> migrate_days in settings.json.")

        if "purge_days" not in settings['s3_migration']:
            settings['s3_migration']['purge_days'] = 30

        if str(settings['s3_migration']['purge_days']).isnumeric() == False:
            raise Exception ("Non-numeric value for s3_migration -> purge_days in settings.json.")

        if "document_limit" not in settings['s3_migration']:
            settings['s3_migration']['document_limit'] = 100

        if str(settings['s3_migration']['document_limit']).isnumeric() == False:
            raise Exception ("Non-numeric value for s3_migration -> document_limit in settings.json.")

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


def main():
    
    try:

        #Log the current DB stats
        logger.info("Initial Database Statistics: " + json.dumps(getDBStats()))

        #Get documents that need to be migrated from MongoDB to S3
        query_and_migrate()

        #Get documents that were restored and need to have details purged
        query_and_purge()

        #Log the final DB Stats
        logger.info("Final Database Statistics: " + json.dumps(getDBStats()))

        #Finished successfully
        exitApp(0)

    except Exception as ex:
        logger.error(ex)
        print(ex)
        exitApp(1)


def getDBStats():
    mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
    skyFollowerDB = mongoDBClient[settings['mongoDb']['database']]

    return skyFollowerDB.command("dbstats")


def query_and_migrate():
    
    mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
    skyFollowerDB = mongoDBClient[settings['mongoDb']['database']]
    flightsCollection = skyFollowerDB[settings['mongoDb']['collection']]
    migratedCount = 0

    #Get documents older than migrate_days that have not been migrated already
    query = {"$and": [{'last_message': {'$lt': (datetime.today()-timedelta(days=settings['s3_migration']['migrate_days']))}}, {'migrated':{ '$exists': False }}]}

    docsResult = flightsCollection.find(query).batch_size(settings['s3_migration']['document_limit'])

    for flight in docsResult:
        if upload_to_s3(flight, flight['_id']) == True:
            mark_document_migrated(flight['_id'])
            migratedCount = migratedCount + 1

    logger.info("Migrated count: " + str(migratedCount))


def mark_document_migrated(id):
    mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
    skyFollowerDB = mongoDBClient[settings['mongoDb']['database']]
    flightsCollection = skyFollowerDB[settings['mongoDb']['collection']]

    #Set the migrated date to now and delete the position and velocities arrays
    flightsCollection.update_one({'_id': id}, {"$set":{'migrated': datetime.now()}, "$unset":{'positions': "", 'velocities':""}})

    logger.debug("Migrated Document to S3: " + id)


def query_and_purge():
    
    mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
    skyFollowerDB = mongoDBClient[settings['mongoDb']['database']]
    flightsCollection = skyFollowerDB[settings['mongoDb']['collection']]
    purgedCount = 0

    #Get documents older than migrate_days that have not been migrated already
    query = {"$and": [{'expires': {'$lt': (datetime.today()-timedelta(days=settings['s3_migration']['purge_days']))}}, {'migrated':{ '$exists': True }}]}

    docsResult = flightsCollection.find(query).batch_size(settings['s3_migration']['document_limit'])

    for flight in docsResult:
        mark_document_purged(flight['_id'])
        purgedCount = purgedCount + 1

    logger.info("Purged count: " + str(purgedCount))


def mark_document_purged(id):
    mongoDBClient = MongoClient(host=settings['mongoDb']['uri'], port=settings['mongoDb']['port'])
    skyFollowerDB = mongoDBClient[settings['mongoDb']['database']]
    flightsCollection = skyFollowerDB[settings['mongoDb']['collection']]

    #Set the migrated date to now and delete the position and velocities arrays
    flightsCollection.update_one({'_id': id}, {"$unset":{'positions': "", 'velocities':"", 'expires':""}})

    logger.debug("Purged Recalled Document: " + id)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def upload_to_s3(object, file_name) -> bool:
    try:
        s3_client = boto3.client('s3', aws_access_key_id=settings['s3_migration']['key'], aws_secret_access_key=settings['s3_migration']['secret'])

        #Convert the data to binary
        json_data = json.dumps(object, default=json_serial).encode('utf-8')

        #Append .gz to the file since we are gzipping it before upload
        if not file_name.endswith(".gz"):
            file_name = file_name + ".gz"

        #Upload the compressed object to S3
        s3_client.put_object(Bucket=settings['s3_migration']['bucket_name'], Key=file_name, Body=gzip.compress(json_data))

    except ClientError as e:
        logger.error("AWS Client Error from upload_to_s3: " + str(e))
        return False
    
    return True


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

    parser = argparse.ArgumentParser(description='Migrates position and velocity details from SkyFollower\'s MongoDB collection over a certain age to Amazon S3')

    args = parser.parse_args()

    #Setup the configuration required
    setup(args)

    main()
