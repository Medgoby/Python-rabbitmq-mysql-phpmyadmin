import time

from server import channel

QUEUES = [
    {
        "name": "queue-data-lake",
        "routing_key": "logs"
    },
    {
        "name": "queue-data-clean",
        "routing_key": "logs"
    }
]

EXCHANGE_NAME = "topic-exchange-logs"

# create exchange
channel.exchange_declare(EXCHANGE_NAME, durable=True, exchange_type='topic')


for queue in QUEUES:
    channel.queue_declare(queue=queue['name'], durable=False)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue['name'], routing_key=queue['routing_key'])



# Create producer to read log file and publish to exchange
with open('assets/web-server-nginx.log') as log_file:
    for line in log_file:
        time.sleep(2)
        channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=queue['routing_key'], body=line.encode('utf-8'))

        print(f"[x] published event `{line}` in topic `{queue['routing_key']}`")
        #print("Sent log line:", line.strip())

