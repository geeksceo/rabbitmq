
import json
import pika
import sys
import os

boats = dict()

# defining what to do when a message is received
def callback(ch, method, properties, body):
    print(" [x CLASSIFIER] Received %r" % body)

    # decoding bytes into string and formatting into JSON
    body_str = body.decode('utf8').replace("'", '"')
    body_json = json.loads(body_str)

    if body_json['boat_destination'] in boats:
        boats[body_json['boat_destination']] += 1
    else:
        boats[body_json['boat_destination']] = 1

    print(" [x CLASSIFIER PORT: %s ] has %s" % (body_json['boat_destination'], boats[body_json['boat_destination']]) )




def main():
    at_port_stream = 'areindore_at_port_stream'
    credentials = pika.PlainCredentials('zprojet', 'rabbit22')
    parameters = pika.ConnectionParameters('rabbitmqserver.istic.univ-rennes1.fr', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # declaring input and output queues
    channel.queue_declare(queue=at_port_stream)

    # auto_ack: as soon as collected, a message is considered as acked
    channel.basic_consume(queue=at_port_stream,
                      auto_ack=True,
                      on_message_callback=callback)
    # wait for messages
    print(' [*CLASSIFIER] Waiting for messages. To exit press CTRL+C')
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
