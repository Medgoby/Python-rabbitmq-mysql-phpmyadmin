import hashlib
import re

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from pika.spec import BasicProperties
from server import channel
from src import utils
from src.row_log import Log_lake


from models import RowLog
from main import CreateEngine


connexion = CreateEngine()


def process_msg_lake(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):
    print(f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")

    log=body.decode("utf-8")
    if '- -' not in log:
       log_lake=Log_lake()
       log_lake.hash_body(log)
       log_lake.get_TimeStamp(log)
       log_lake.line(log)

       # INSERT INTO THE DATA BASE
       row_logs= RowLog(id=log_lake.id,timestamp=log_lake.timestamp,log=log_lake.log)
       c_instance = connexion.query(RowLog).filter_by(id=row_logs.id).one_or_none()
       if not c_instance:
        connexion.add(row_logs)
        connexion.commit()
        print(f"consuming --> {log}")



# def process_msg_data_lake(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):

#     body = body.decode("utf-8")
#     #regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")
    
#     if utils.get_user(body) != "-" and utils.get_session(body) != "-":
#         row_logs = RowLog(id= utils.hash_body(body), timestamp=utils.get_timestamp(body), log=utils.get_log(body))
#         connexion.add(row_logs)
#         connexion.commit()
#         print("Data inserted successfully")

#         print(f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")
        
#     else:
#         print("Message ignored")



channel.basic_consume(queue="queue-data-lake",
                      on_message_callback=process_msg_lake, auto_ack=True)
channel.start_consuming()