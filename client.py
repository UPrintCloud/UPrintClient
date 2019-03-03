import pika
from requests import get
import config


def main(channel, method, properties, body):
    url = '%s/api/download?job_id=%s' % (config.master_url, body.decode('utf-8'))
    req = get(url)
    with open('buf.pdf', 'wb') as file:
        file.write(req.content)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def listen(uri, username, password, queue_name):
    user = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(uri, credentials=user))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        main,
        queue=queue_name
    )
    channel.start_consuming()


if __name__ == '__main__':
    listen(config.node_url, config.username, config.password, config.client_id)

