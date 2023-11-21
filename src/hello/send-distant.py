#!/usr/bin/env python
import pika

# getting a connection to the distant broker running on rabbitmqserver
credentials = pika.PlainCredentials('zprojet', 'rabbit22')
parameters = pika.ConnectionParameters('rabbitmqserver.istic.univ-rennes1.fr', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# declaring the queue
channel.queue_declare(queue='hello_geeksceo')

# send the message, through the exchange ''
# which simply delivers to the queue having the key as name
channel.basic_publish(exchange='',
                      routing_key='hello_geeksceo',
                      body='Hello World! - OWAGOKE')
print(" [x] Sent 'Hello World!'")

# gently close (flush)
connection.close()
