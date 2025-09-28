import pika
import json
import pyModeS as pms
import time
import datetime
import logging
import os
import sys
import redis
from redis.commands.json.path import Path
import redis.exceptions


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", 5672)
RABBITMQ_QUEUE_ROOT = os.getenv("RABBITMQ_QUEUE", "skyfollower.1090")
LATITUDE = float(os.getenv("LATITUDE", 0))
LONGITUDE = float(os.getenv("LONGITUDE", 0))
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
REDIS_DB = os.getenv("REDIS_DB", 0)
ID_PROC_CONSUMER_1090 = int(os.getenv("ID_PROC_CONSUMER_1090", 0))
RABBITMQ_QUEUE = f"{RABBITMQ_QUEUE_ROOT}.{ID_PROC_CONSUMER_1090}"


redisClient = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def callback(ch, method, properties, body):

    try:
        message = json.loads(body.decode("utf-8"))
        messageProcessor(message)

    except Exception as e:
        logger.error(f"Failed to process message: {e}")


def main():

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    logger.info(f"Consuming messages on '{RABBITMQ_QUEUE}'. Press Ctrl+C to exit.")
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Exiting...")
    finally:
        redisClient.close()
        connection.close()


def messageProcessor(raw_message:dict):
       
    try:
        #Object to store data
        data = {}

        message = raw_message['data'].replace("*","").replace(";","")
        data['time'] = raw_message['time']

        #Get the download format
        data['downlink_format'] = pms.df(message)

        if data['downlink_format'] not in [5, 21, 17]:
            return
                    
        data['icao_hex'] = pms.adsb.icao(message)

        #Ensure we have an icao_hex
        if data['icao_hex'] == None:
            return

        if data['downlink_format'] in [5, 21]:
            data['squawk'] = pms.common.idcode(message)

        if data['downlink_format'] == 17:
            
            typeCode = pms.adsb.typecode(message)
            data['messageType'] = typeCode

            #Throw away TC 28 and 29...not yet supported
            if typeCode in [28, 29]:
                return

            if 1 <= typeCode <= 4:
                data['ident'] = pms.adsb.callsign(message).replace("_","")
                data['category'] = pms.adsb.category(message)                    

            if 5 <= typeCode <= 18 or 20 <= typeCode <=22:
                data['latitude'] = pms.adsb.position_with_ref(message, LATITUDE, LONGITUDE)[0]
                data['longitude'] = pms.adsb.position_with_ref(message, LATITUDE, LONGITUDE)[1]
                data['altitude'] = pms.adsb.altitude(message)

            if 5 <= typeCode <= 8:
                data['velocity'] = pms.adsb.velocity(message)[0]
                data['heading'] = pms.adsb.velocity(message)[1]
                data['vertical_speed'] = pms.adsb.velocity(message)[2]

            if typeCode == 19:
                data['velocity'] = pms.adsb.velocity(message)[0]
                data['heading'] = pms.adsb.velocity(message)[1]
                data['vertical_speed'] = pms.adsb.velocity(message)[2]

            if typeCode == 31:
                data['adsb_version'] = pms.adsb.version(message)


        if redisClient.json().set(f"flight:info:{str(data['icao_hex']).upper()}", Path.root_path(), {"first_message": data['time'], "last_message": data['time']}, nx=True) == True:
            redisClient.json().set(f"flight:velocities:{str(data['icao_hex']).upper()}", Path.root_path(), [], nx=True)
            redisClient.json().set(f"flight:positions:{str(data['icao_hex']).upper()}", Path.root_path(), [], nx=True)

        if all(key in data for key in ['velocity', 'heading', 'vertical_speed']):
            redisClient.json().arrappend(f"flight:velocities:{str(data['icao_hex']).upper()}", Path.root_path(), {"time": data['time'], "velocity": data['velocity'], "heading": data['heading'], "vertical_speed": data['vertical_speed']})

        if all(key in data for key in ['latitude', 'longitude', 'altitude']):
            redisClient.json().arrappend(f"flight:positions:{str(data['icao_hex']).upper()}", Path.root_path(), {"time": data['time'], "latitude": data['latitude'], "longitude": data['longitude'], "altitude": data['altitude']})


    except redis.exceptions.RedisError as redisError:
        print("Redis Error " + str(redisError) )
        

    except Exception as ex:
        logger.error("Exception of type: " + type(ex).__name__ + " while processing message [" + str(message) + "] : " + str(ex))
        pass


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
        self.squawk_count:int = 0
        self.broadcast:str = ""
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
            record['squawk'] = str(self.squawk)

        if self.broadcast != "":
            record['broadcast'] = self.broadcast

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

        sqliteCur.execute("SELECT icao_hex, first_message, last_message, total_messages, aircraft, ident, operator, squawk, squawk_count, broadcast, origin, destination, matched_rules  FROM flights WHERE icao_hex='" + self.icao_hex + "'")
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
        self.squawk = str(result[0]['squawk'])
        self.squawk_count = str(result[0]['squawk_count'])
        self.broadcast = result[0]['broadcast']
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
        sqlStatement = "REPLACE INTO flights (icao_hex, first_message, last_message, total_messages, aircraft, ident, operator, squawk, squawk_count, broadcast, origin, destination, matched_rules) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
        parameters = (self.icao_hex, self.first_message, self.last_message, self.total_messages, json.dumps(self.aircraft), self.ident, json.dumps(self.operator), self.squawk, self.squawk_count, self.broadcast, json.dumps(self.origin), json.dumps(self.destination), json.dumps(self.matched_rules))
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

        logger.debug("Persisted record _id: " + record['_id'] + " ICAO HEX: " + str(record['aircraft']['icao_hex']))
        

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

        value = str(value).strip()

        if self.squawk != "":

            if str(value) in ['7500','7600','7700','7777']:
                self.squawk_count = self.squawk_count + 1
                if self.squawk_count >= 3:
                    self.squawk = str(value)
            return

        self.squawk_count = 0
        self.squawk = str(value)
        

    def setAdsbVersion(self, value:int):

        if 'adsb_version' in self.aircraft:
            return

        self.aircraft['adsb_version'] = value

        if self.aircraft['adsb_version'] != "":
            return

        self.aircraft['adsb_version'] = value


    def setBroadcast(self, value:str):

        value = value.strip()
       
        if self.broadcast != "":
            return

        self.broadcast = value
    

    def _getOperator(self):

        if settings['operators']['enabled'] != True:
            return

        if "registration" in self.aircraft:
            if self.aircraft['registration'].replace("-", "") == self.ident.replace("-", ""):
                return

        #Filter US-based registration numbers, even those with historical (NC123AB, NX, NR, NL) markings
        if bool(re.match("^N[C,X,R,L]?[1-9]((\d{0,4})|(\d{0,3}[A-HJ-NP-Z])|(\d{0,2}[A-HJ-NP-Z]{2}))$", self.ident)):
            return

        #Filter modern US-based registration numbers with air ambulance (LN123AB) or air taxi (TN123AB) prefix appended
        if bool(re.match("^[L,T]?N[1-9]((\d{0,4})|(\d{0,3}[A-HJ-NP-Z])|(\d{0,2}[A-HJ-NP-Z]{2}))$", self.ident)):
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
       







def setup():

    global logger

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] - %(message)s'
    )
    streamHandler = logging.StreamHandler(sys.stdout)
    streamHandler.setFormatter(formatter)   

    logger.addHandler(streamHandler)

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    if log_level not in ["INFO", "WARNING", "ERROR", "CRITICAL", "DEBUG"]:
        log_level = "INFO"

    if log_level == "INFO":
        logger.setLevel(logging.INFO)

    if log_level == "WARNING":
        logger.setLevel(logging.WARNING)
        logger.log(logging.WARNING, f"Set log level to {log_level}")

    if log_level == "ERROR":
        logger.setLevel(logging.ERROR)
        logger.log(logging.ERROR, f"Set log level to {log_level}")

    if log_level == "CRITICAL":
        logger.setLevel(logging.CRITICAL)
        logger.log(logging.CRITICAL, f"Set log level to {log_level}")

    if log_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
        logger.log(logging.DEBUG, f"Set log level to {log_level}")

    logger.info("Application started.")


if __name__ == "__main__":
    setup()
    main()
