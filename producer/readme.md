# SkyFollower Producer 1090

SkyFollower Producer 1090 deploys a TCP → RabbitMQ ADS-B data pipeline in Docker for 1090MHz.

SkyFollower Producer 1090 (`producer_1090.py`) connects to a TCP ADS-B source such as [readsb or adsb-ultrafeeder™](https://github.com/sdr-enthusiasts/docker-adsb-ultrafeeder) listening to a 1090MHz and forwards the raw data to RabbitMQ.

In the event the connection to RabbitMQ is unavailable, the Producer will 

---

## Prerequisites

Start with a fresh Raspberry Pi OS install.

You should install RabbitMQ on a separate system.

### Install Docker & Docker Compose

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git
```

Get Docker from their website:
```bash
curl -sSL https://get.docker.com | sh
```

Make curent user part of the docker group:
```bash
sudo usermod -aG docker $USER
```

Verify:
```bash
docker --version
docker-compose --version
```
---

Make the SkyFollower Folder
```bash
sudo mkdir /opt/SkyFollower
```

Change ownership so that docker can access it
```bash
sudo chgrp -R docker /opt/SkyFollower
```
```bash
sudo chmod -R g+w /opt/SkyFollower
```


## Deploy the Project

1. **Clone from GitHub:**
   ```bash
   git clone https://github.com/BrentIO/SkyFollower.git
   ```

2. **Modify the docker compose yaml as necssary:**
   ```bash
   nano /opt/SkyFollower/docker-compose.yaml
   ```

3. **Start the containers**
   ```bash
   docker compose up -d
   ```

4. **Monitor logs**
   ```bash
   docker logs -f skyfollower-producer1090
   ```

---

## Component Diagram
```plantuml
@startuml Producer 1090 Component Diagram

skinparam BoxPadding 20
skinparam ParticipantPadding 20
skinparam SequenceGroupBodyBackgroundColor #FFFFFF90
skinparam SequenceReferenceBackgroundColor #FFFFFF90
skinparam ArrowColor Black
skinparam BackgroundColor White
skinparam DatabaseBackgroundColor White
skinparam SequenceBoxBorderColor Black
skinparam style strictuml

package "ADS-B Receiver" as pkg_readsb{
   component "[[https://github.com/sdr-enthusiasts/docker-adsb-ultrafeeder readsb or adsb-ultrafeeder™]]" as readsb <<Service>>
}


package "SkyFollower Producer 1090" {
   component "TCP Reader" as tcpReader1090<<Thread>>
   component "Local Queue Reader" as localQueueRader1090<<Thread>>
   queue "Ephermal Storage" as localQueue1090 <<Queue>>

   tcpReader1090 --> localQueue1090: Store message
   localQueueRader1090 --> localQueue1090: Retrieve message
   tcpReader1090 -> localQueueRader1090: Thread event\non message storage
}

package "SkyFollower Producer 978" {
   component "TCP Reader" as tcpReader978<<Thread>>
   component "Local Queue Reader" as localQueueRader978<<Thread>>
   queue "Ephermal Storage" as localQueue <<Queue>>

   tcpReader978 --> localQueue: Store message
   localQueueRader978 --> localQueue: Retrieve message
   tcpReader978 -> localQueueRader978: Thread event\non message storage
}

package "RabbitMQ" as rabbitmq {
   queue "skyfollower.978" as queue978 <<Queue>>
   queue "skyfollower.1090" as queue1090 <<Queue>>
}

package "SkyFollower Consumer 1090" {
   component "Queue Reader" as reader1090<<Thread>>
   note right of reader1090
      Multiple readers
      are possible.
   end note
}

package "SkyFollower Consumer 978" {
   component "Queue Reader" as reader978<<Thread>>
   note right of reader978
      Multiple readers
      are possible.
   end note
}

package "SkyFollower Core"{
   component "Flight Life Processor" as lifeprocessor <<Processor>>
   component "S3 Migration Processor" as s3migration <<Processor>>
}

package "Redis" {
   database "Redis" as redis <<Cache>>
}

package "MongoDB" {
   database "SkyFollower" as mongodb <<Database>>
}

package "Amazon S3" {
   component "S3" as s3 <<Service>>
}

tcpReader1090 ---> readsb: Reads TCP Socket\nPort 30002
localQueueRader1090 -up---> queue1090: Publish message
reader1090 -down--> queue1090: Read message

tcpReader978 ---> readsb: Reads TCP Socket\nPort 30978
localQueueRader978 -up---> queue978: Publish message
reader978 -down--> queue978: Read message

reader1090 -up-> redis: Get aircraft and operator information\nCache active Flight
reader978 -up-> redis: Get aircraft and operator information\nCache active Flight

lifeprocessor -up-> redis: Get stale cached flights to persist
lifeprocessor -> mongodb: Persist completed flight
s3migration --down-> mongodb: Get flights to migrate

s3migration --down-> s3: Write JSON file

localQueueRader978 -right[hidden]- queue978
localQueueRader1090 -left[hidden]- queue1090

@enduml

```

## Sequence Diagram

```plantuml
@startuml Producer 1090 Sequence Diagram
autonumber "<b>(0)"
hide unlinked
skinparam BoxPadding 20
skinparam ParticipantPadding 20
skinparam SequenceGroupBodyBackgroundColor #FFFFFF90
skinparam SequenceReferenceBackgroundColor #FFFFFF90
skinparam ArrowColor Black
skinparam BackgroundColor White
skinparam DatabaseBackgroundColor White
skinparam SequenceBoxBorderColor Black
skinparam style strictuml

skinparam participant {
   BackgroundColor White
   BorderColor Black
   FontColor Black
}

skinparam queue {
   BackgroundColor White
   BorderColor Black
   FontColor Black
}

skinparam sequence {
   MessageAlignment left
   LifeLineBorderColor Black
   LifeLineBackgroundColor White
}

title SkyFollower Producer 1090

participant Antenna as antenna

box "Receiver Docker Instance"
    participant "[[https://github.com/sdr-enthusiasts/docker-adsb-ultrafeeder readsb or\nadsb-ultrafeeder™]]" as readsb <<Service>>
    participant "SkyFollower\nTCP Reader" as tcpReader1090 <<Processor>>
    queue "Ephermal Storage" as localQueue <<Queue>>
    participant "SkyFollower\nLocal Queue Reader" as localQueueRader1090 <<Processor>>
end box

box "RabbitMQ Instance"
   queue "skyfollower.1090" as rabbitmq <<Queue>>
end box

box "SkyFollower Docker Instance"
   participant "SkyFollower\nQueue Processor" as sfqueueproc
   database "Redis" as cache <<Cache>>
   database "MongoDB" as mongodb <<Database>>
   participant "SkyFollower\nFlight Life Processor" as sflifeproc
   participant "SkyFollower\nS3 Migration Processor" as s3migrationproc

end box

tcpReader1090 -> readsb++: **Open Socket**\n(Default port 30002)
readsb --> tcpReader1090--: Connection

localQueueRader1090 -> rabbitmq++: **Open Connection**
rabbitmq --> localQueueRader1090--: Connection

antenna -\ readsb++: **Message**
readsb -\ tcpReader1090++: **Message**
deactivate readsb

tcpReader1090 -> tcpReader1090++: **Add timestamp**
deactivate tcpReader1090

tcpReader1090 -> localQueue++: **Add to local queue**\n""{"time":{timestamp},""\n"""message":"{message}"}""
localQueue --> tcpReader1090--: Added
tcpReader1090 -\ localQueueRader1090++: **Raise Thread Event**
deactivate tcpReader1090

loop for message in queue
   localQueueRader1090 -> localQueue++: **Get message**
   localQueue --> localQueueRader1090--: Message
   localQueueRader1090 -> rabbitmq++: **Publish message**
   rabbitmq --> localQueueRader1090--: Response

   Alt Message published successfully
      localQueueRader1090 -> localQueue++: **Delete item from queue**
      localQueue --> localQueueRader1090--: Response

   else Timeout or failure
      localQueueRader1090 -> localQueueRader1090++: **Log critical event**
      deactivate localQueueRader1090
      localQueueRader1090 -> localQueueRader1090++: **Reattempt RabbitMQ connection**\nWait 10 seconds
      deactivate localQueueRader1090
   end
deactivate localQueueRader1090
end

@enduml

```
# Build the image from scratch
```bash
docker buildx build --platform=linux/amd64,linux/arm64/v8 --push -t brentio/skyfollower-producer-1090:latest .
```