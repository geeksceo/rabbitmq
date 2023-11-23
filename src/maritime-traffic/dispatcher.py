

#!/usr/bin/env python
import sys
import os
import pika
import json

# defining what to do when a message is received
def callback(ch, method, properties, body):

    print(" [x DISPATCHER] Received %r" % body)

    # decoding bytes into string and formatting into JSON
    body_str = body.decode('utf8').replace("'", '"')
    body_json = json.loads(body_str)

    at_sea_stream = 'areindore_at_sea_stream'
    at_port_stream = 'areindore_at_port_stream'

    # Declaration de la queue de speed-estimator

    if(body_json['boat_speed'] > 5):
        ch.queue_declare(queue=at_sea_stream)
        ch.basic_publish(exchange='',
                                routing_key=at_sea_stream,
                                body=str(body_json))
    else:
        ch.queue_declare(queue=at_port_stream)
        ch.basic_publish(exchange='',
                                routing_key=at_port_stream,
                                body=str(body_json))



    # Receptionner la donnée
    # Compare la vitesse pour savoir à qui transmettre
    # si > 5 ajouter dans la queue de avg-speed-estimator
    # sinon ajouter dans la queue de classifier

    # channel.basic_publish(exchange='',
    #                         routing_key=boat_stream,
    #                         body=str(d))


def main():
    boat_stream = 'areindore_boat_stream'
    credentials = pika.PlainCredentials('zprojet', 'rabbit22')
    parameters = pika.ConnectionParameters('rabbitmqserver.istic.univ-rennes1.fr', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # declaring input and output queues
    channel.queue_declare(queue=boat_stream)

    # auto_ack: as soon as collected, a message is considered as acked
    channel.basic_consume(queue=boat_stream,
                      auto_ack=True,
                      on_message_callback=callback)
    # wait for messages
    print(' [* DISPATCHER] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
