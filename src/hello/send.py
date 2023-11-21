#!/usr/bin/env python
import pika

# getting a connection to the broker
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# declaring the queue (idempotent)
channel.queue_declare(queue='hello')

# send the message, through the exchange ''
# which simply delivers to the queue having the key as name
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello world!')
print(" [x] Sent 'Hello World!'")

# gently close (flush)
connection.close()
