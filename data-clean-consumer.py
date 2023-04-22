import time
import re
import hashlib

from ip2geotools.databases.noncommercial import DbIpCity
from pycountry import countries
from datetime import datetime, timedelta
import urllib.parse
from http.client import responses

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from pika.spec import BasicProperties
from server import channel


from src.models import Base, CleanLog, RowLog
from main import CreateEngine

from src.log_clean import Log_clean
from src.row_log import Log_lake
from src.transformations import UserTransformation, UrlTransformation, SizeTransformation,StatusCodeTransformation

connexion = CreateEngine()

def process_msg_clean(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):
    
    log=body.decode("utf-8")
    
    if '- -' not in log:
       log_clean=Log_clean()
       log_clean.hash_body(log)
       log_clean.matched(log)
       log_clean.timestamp_to_datetime(log)
       log_clean.rest_version(log)
       log_clean.get_location(log)

       log_clean = StatusCodeTransformation().transform(log_clean)
       log_clean = UserTransformation().transform(log_clean)
       log_clean=UrlTransformation().get_schema_host_from_url(log_clean)
       log_clean=SizeTransformation().kilo_mega_bytes(log_clean)

       # INSERT INTO THE DATA BASE 
       RowClean= CleanLog(id=log_clean.id,timestamp=log_clean.timestamp,year=log_clean.year,month=log_clean.month,day=log_clean.day,day_of_week=log_clean.day_of_week,time=log_clean.time,ip=log_clean.ip,
                           country=log_clean.country,city=log_clean.city,session=log_clean.session,user=log_clean.user,is_email=log_clean.is_email,url=log_clean.url,schema=log_clean.schema,host=log_clean.host,
                           rest_version=log_clean.rest_vers,status=log_clean.status,status_verbose=log_clean.status_verbose,size_bytes=log_clean.size,size_kilo_bytes=log_clean.size_k_b,size_mega_bytes=log_clean.size_m_b,
                           email_domain=log_clean.domain,rest_method=log_clean.method)
       c_instance = connexion.query(CleanLog).filter_by(id=RowClean.id).one_or_none()
       if not c_instance:
        connexion.add(RowClean)
        connexion.commit()
        print(f"consuming --> {log}")

channel.basic_consume(queue="queue-data-clean",
                      on_message_callback=process_msg_clean, auto_ack=True,consumer_tag="consumer-data-clean")
channel.start_consuming()



