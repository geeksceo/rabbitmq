
import json
import pika
import sys, os

boats_list = list()

# defining what to do when a message is received
def callback(ch, method, properties, body):
    print(" [x SPEED ESTIMATOR] Received %r" % body)

    # decoding bytes into string and formatting into JSON
    body_str = body.decode('utf8').replace("'", '"')
    body_json = json.loads(body_str)
    boats_list.append(body_json['boat_speed'])
    print(" [x SPEED ESTIMATOR AVERAGE ] IS %s" % str(sum(boats_list)/len(boats_list)) )







def main():
    at_sea_stream = 'areindore_at_sea_stream'
    credentials = pika.PlainCredentials('zprojet', 'rabbit22')
    parameters = pika.ConnectionParameters('rabbitmqserver.istic.univ-rennes1.fr', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # declaring input and output queues
    channel.queue_declare(queue=at_sea_stream)

    # auto_ack: as soon as collected, a message is considered as acked
    channel.basic_consume(queue=at_sea_stream,
                      auto_ack=True,
                      on_message_callback=callback)
    # wait for messages
    print(' [*SPEED ESTIMATOR] Waiting for messages. To exit press CTRL+C')
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
