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
import re


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", 5672)
RABBITMQ_QUEUE_ROOT = os.getenv("RABBITMQ_QUEUE", "skyfollower.1090")
RABBITMQ_QUEUE_PERSIST = os.getenv("RABBITMQ_QUEUE_PERSIST", "skyfollower.persist")
LATITUDE = float(os.getenv("LATITUDE", 0))
LONGITUDE = float(os.getenv("LONGITUDE", 0))
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
REDIS_DB = os.getenv("REDIS_DB", 0)
ID_PROC_CONSUMER_1090 = int(os.getenv("ID_PROC_CONSUMER_1090", 0))
RABBITMQ_QUEUE = f"{RABBITMQ_QUEUE_ROOT}.{ID_PROC_CONSUMER_1090}"


redisClient = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


lastStatOutput = 0


def callback(ch, method, properties, body):

    global lastStatOutput

    try:
        stats.increment_message_count()
        message = json.loads(body.decode("utf-8"))
        messageProcessor(message)

        currentTime = int(time.time() * 1000)

        if currentTime - lastStatOutput > 10000:
            stats.publish()
            lastStatOutput = currentTime

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

        flight = Flight(data['icao_hex'],data['time'])

        if all(key in data for key in ['velocity', 'heading', 'vertical_speed']):
            flight.addVelocity(Velocity(timestamp=data['time'],velocity=data['velocity'],heading=data['heading'],vertical_speed=data['vertical_speed']))

        if all(key in data for key in ['latitude', 'longitude', 'altitude']):
            flight.addPosition(Position(timestamp=data['time'],latitude=data['latitude'],longitude=data['longitude'],altitude=data['altitude']))

        if "squawk" in data:
            flight.setSquawk(data['squawk'])

        if "category" in data:
            flight.setCategory(data['category'])

        if "ident" in data:
            flight.setIdent(data['ident'])

        if "adsb_version" in data:
            flight.setAdsbVersion(data['adsb_version'])

        if "broadcast" in data:
            flight.setBroadcast(data['broadcast'])

        flight.evaluateRules()
        flight.saveLocal()


    except redis.exceptions.RedisError as redisError:
        print(f"Redis Error {redisError}")
        

    except Exception as ex:
        logger.error(f"Exception of type: {type(ex).__name__} while processing message [{message}] : {ex}")
        pass


class Flight():
    """Flight Record"""

    def __init__(self, icao_hex, message_time) -> None:
        self.icao_hex:str = str(icao_hex).upper().strip()
        self.first_message:int = message_time
        self.last_message:int = message_time
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

        if self.icao_hex == "":
            raise Exception("ICAO Hex is empty")
        
        if redisClient.json().set(f"flight:info:{self.icao_hex}", Path.root_path(), {"first_message": self.first_message, "last_message": self.last_message, "total_messages": self.total_messages}, nx=True) == True:
            #Key does not exist in Redis
            redisClient.json().set(f"flight:velocities:{self.icao_hex}", Path.root_path(), [], nx=True)
            redisClient.json().set(f"flight:positions:{self.icao_hex}", Path.root_path(), [], nx=True)

            self._getAircraft()

        else:
            #Key exists in Redis
            self.get()
            self.last_message = message_time
            self.total_messages = self.total_messages + 1


    def toDict(self) -> dict:

        record = {}
        record['first_message'] = self.first_message
        record['last_message'] = self.last_message
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

        if self.origin != {}:
            record['origin'] = self.origin

        if self.destination != {}:
            record['destination'] = self.destination         

        if len(self.matched_rules) > 0:
            record['matched_rules'] = self.matched_rules

        return record


    def get(self, limit_position:bool = True, limit_velocity:bool = True):
        
        """Retrieves the given ICAO HEX from the local database.
        If no records are found, False is returned.
        If records are returned, True is returned and the object is populated from the database.
        """

        result = redisClient.json().get(f"flight:info:{self.icao_hex}")

        self.first_message = result['first_message']
        self.last_message = result['last_message']
        self.total_messages = result['total_messages']


        if "aircraft" in result:
            self.aircraft = result['aircraft']

        if "ident" in result:
            self.ident = result['ident']

        if "operator" in result:
            self.operator = result['operator']

        if "squawk" in result:
            self.squawk = str(result['squawk'])

        if "squawk_count" in result:
            self.squawk_count = str(result['squawk_count'])

        if "broadcast" in result:
            self.broadcast = result['broadcast']

        if "origin" in result:
            self.origin = result['origin']

        if "destination" in result:
            self.destination = result['destination']

        if "matched_rules" in result:
            self.matched_rules = result['matched_rules']

        #self._getPositions(limit_position)
        #self._getVelocities(limit_velocity)    


    def saveLocal(self):
        redisClient.json().set(f"flight:info:{self.icao_hex}", Path.root_path(), self.toDict())


    def persist(self):
        """Persists the data to the remote data store."""
        return

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
        return

        sqliteCur = localDb.cursor()
        sqliteCur.execute("DELETE FROM flights WHERE icao_hex ='" + self.icao_hex + "'")
        sqliteCur.execute("DELETE FROM positions WHERE icao_hex ='" + self.icao_hex + "'")
        sqliteCur.execute("DELETE FROM velocities WHERE icao_hex ='" + self.icao_hex + "'")


    def _getPositions(self, limit:bool=True):
        """Retrieves position reports for the current aircraft.
        If limit is True, only the last message is returned."""
        return

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
        redisClient.json().arrappend(f"flight:positions:{self.icao_hex}", Path.root_path(), {"time": position.timestamp, "latitude": position.latitude, "longitude": position.longitude, "altitude": position.altitude})


    def addVelocity(self, velocity:'Velocity'): 
        redisClient.json().arrappend(f"flight:velocities:{self.icao_hex}", Path.root_path(), {"time": velocity.timestamp, "velocity": velocity.velocity, "heading": velocity.heading, "vertical_speed": velocity.vertical_speed})


    def _getVelocities(self, limit:bool=True):
        """Retrieves velocity reports for the current aircraft.
        If limit is True, only the last message is returned."""
        return

        sql = "SELECT * FROM velocities WHERE icao_hex='" + self.icao_hex + "' ORDER BY timestamp"

        if limit == True:
            sql = sql + " DESC LIMIT 1"

        sqliteCur = localDb.cursor()
        sqliteCur.execute(sql)
        results = sqliteCur.fetchall()

        for result in results:
            self.velocities.append(Velocity(result['timestamp'], result['velocity'], result['heading'], result['vertical_speed']))


    def _getAircraft(self):
        
        if self.icao_hex == "":
            return
        
        result = redisClient.json().get(f"aircraft:icao_hex:{self.icao_hex}")

        if result != None:
            self.aircraft = result
        else:
            stats.increment_registration_unknown_count()


    def setIdent(self, value:str):

        value = value.strip()
       
        if self.ident != "":
            return

        if value == "" or value == "00000000":
            return

        self.ident = value
        self._getFlightInfo()
        self._getOperator()


    def setCategory(self, value:int):

        match value:
            case 1:
                self.aircraft['wake_turbulence_category'] = "Light"

            case 2:
                self.aircraft['wake_turbulence_category'] = "Medium 1"

            case 3:
                self.aircraft['wake_turbulence_category'] = "Medium 2"

            case 4:
                self.aircraft['wake_turbulence_category'] = "High Vortex Aircraft"

            case 5:
                self.aircraft['wake_turbulence_category'] = "Heavy"

            case 6:
                self.aircraft['wake_turbulence_category'] = "High Performance"

            case 7:
                self.aircraft['wake_turbulence_category'] = "Rotorcraft"


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
        
        result = redisClient.json().get(f"operators:{value}")

        if result != None:
            self.operator = result
        else:
            stats.increment_operator_unknown_count()


    def _getFlightInfo(self):
        return
        
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
        return

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
        return

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
        self.lastPublished = time.time()
        self.count_messages_throttled = 0

    def set_message_handling_high_water_mark(self, value):
        if value > self.message_handling_high_water_mark_ms:
            self.message_handling_high_water_mark_ms = value

    def get_message_count_per_second(self):
        if (time.time() - self.lastPublished) > 0:
            return int(self.count_messages / (time.time() - self.lastPublished))
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
        print(("---------------------------------------------------------------------------------------------------------------------------------------"))

        print(json.dumps(self.list(), indent=4))

        #for stat in self.list():
        #    logger.debug("Statistic: " + stat['name'] + ": " + str(stat['value']))

            #if mqttClient.is_connected():
            #    mqttClient.publish(settings["mqtt"]["topic_statistics"] + stat['name'], stat['value'])

        #Reset the statistics
        self.reset_on_publish()
        self.lastPublished = time.time()


def setup():

    global logger
    global stats
    global lastStatOutput

    lastStatOutput = 0

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

    stats = statistics()


if __name__ == "__main__":
    setup()
    main()
