#!/usr/bin/env python
import os
import sys
import pika
import json
import random
import time
from enum import Enum

NB_BOATS = 100

class Port(Enum):
    BREST = 1
    VALENCIA = 2
    PALERMO = 3
    BRIGHTON = 4
    AMSTERDAM = 5

class BoatSignal:

    def __init__(self, rnd):
        self.boatId = rnd.randint(0,NB_BOATS)
        self.destination = rnd.choice(list(Port))
        self.speed = rnd.randint(0,20)

    def getBoatId(self):
        return self.boatId

    def getDestination(self):
        return self.destination.name

    def getSpeed(self):
        return self.speed

    def toDict(self):
        d = {}
        d["boat_id"] = self.getBoatId()
        d["boat_destination"] = self.getDestination()
        d["boat_speed"] = self.getSpeed()

        return d

if __name__ == "__main__":

    boat_stream = 'areindore_boat_stream'
    # getting a connection to the broker
    credentials = pika.PlainCredentials('zprojet', 'rabbit22')
    parameters = pika.ConnectionParameters('rabbitmqserver.istic.univ-rennes1.fr', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # declaring the queue
    channel.queue_declare(queue=boat_stream)

    while(True):
        try:
            time.sleep(5)
            rnd = random.Random()
            bs = BoatSignal(rnd)
            d = bs.toDict()

            # send the message, through the exchange ''
            # which simply delivers to the queue having the key as name
            channel.basic_publish(exchange='',
                            routing_key=boat_stream,
                            body=str(d))

            print(" [x DATA GENERATOR] Sent: " + str(d))
        except Exception:
            connection.close()
            os._exit


    # gently close (flush)
    # connection.close()
