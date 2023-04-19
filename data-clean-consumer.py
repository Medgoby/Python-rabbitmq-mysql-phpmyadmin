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


from models import Base, CleanLog, RowLog
from main import CreateEngine

from src.log_clean import Log_clean
from src.row_log import Log_lake


#from row_log import Log_lake
#from log_clean import Log_clean
#from transformations import UserTransformation,StatusCodeTransformation, UrlTransformation, SizeTransformation

from transformations import UserTransformation, UrlTransformation, SizeTransformation,StatusCodeTransformation
#from models import Base,CleanLog,RowLog
#from sqlalchemy import create_engine
#from database import CONFIG
#from sqlalchemy.orm import sessionmaker


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

       #log_clean.get_country(log)


       log_clean = StatusCodeTransformation().transform(log_clean)
       log_clean = UserTransformation().transform(log_clean)
       #log_clean=UserTransformation().get_domain_from_email(log_clean)
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

# def process_msg_data_clean(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):
#     body = body.decode("utf-8")
#     regex = re.compile(r"(?P<ip>\S{7,15}) (?P<session>\S{1}|\S{15}) (?P<user>\S{1,50}) \[(?P<timestamp>\S{20}) "
#                        r"(?P<utc>\S{5})\] \"(?P<method>GET|POST|DELETE|PATCH|PUT) (?P<url>\S{1,4096}) "
#                        r"(?P<version>\S{1,10})\" (?P<status>\d{3}) (?P<size>\d+) -")

#     # mon = re.search(regex, text)
#     # mon.group('timestamp')

#     def match_regex(regex, body):
#         match = re.search(regex, body)
#         if match:
#             return match
#         else:
#             return None

#     match = match_regex(regex, body)

#     if match != None:

#         def get_id(body):
#             id = hashlib.md5(body.encode()).hexdigest()
#             return id



#         def timestamp_to_datetime(match):
#             timestamp = match.group("timestamp")
#             format = "%d/%b/%Y:%H:%M:%S"
#             date_time = datetime.strptime(timestamp, format)
#             date_time = date_time + timedelta(hours=5)
#             return date_time

#         def get_year(match):
#             date_time = timestamp_to_datetime(match)
#             year = date_time.strftime("%Y")
#             return year

#         def get_month(match):
#             date_time = timestamp_to_datetime(match)
#             month = date_time.strftime("%m")
#             return month

#         def get_day(match):
#             date_time = timestamp_to_datetime(match)
#             day = date_time.strftime("%d")
#             return day

#         def get_day_of_week(match):
#             date_time = timestamp_to_datetime(match)
#             day_of_week = date_time.strftime("%A")
#             return day_of_week

#         def get_time(match):
#             date_time = timestamp_to_datetime(match)
#             time = date_time.strftime("%H:%M:%S")
#             return time

#         def get_ip(match):
#             ip_matches = match.group("ip")
#             return ip_matches

#         def get_country(ip_matches):
#             res = DbIpCity.get(ip_matches, api_key="free")
#             country = str.upper(countries.get(alpha_2=res.country).name)
#             return country

#         def get_city(ip_matches):
#             res = DbIpCity.get(ip_matches, api_key="free")
#             city = str.upper(res.city)
#             return city

#         def get_session(match):
#             session = match.group("session")
#             if session == "-":
#                 return "None"
#             else:
#                 return session

#         def get_user(match):
#             user = match.group("user")
#             if user == "-":
#                 return "None"
#             else:
#                 return user

#         def get_email_domain(user):
#             if len(re.findall(r'((?<=@)[^.]+(?=\.)+.+)', user)) > 0:
#                 email_domain = re.findall(r'((?<=@)[^.]+(?=\.)+.+)', user)
#                 return email_domain
#             else:
#                 return None
            
#         def get_is_email(user):
#             if get_email_domain(user) != None:
#                 return "True"
#             else:
#                 return "False"

#         def get_rest_method(match):
#             rest_method = match.group("method")
#             return rest_method

#         def get_url(match):
#             url = match.group("url")
#             return url

#         def get_schema(url):
#             schema = urllib.parse.urlsplit(url).scheme
#             return schema

#         def get_host(url):
#             host = urllib.parse.urlsplit(url).hostname
#             return host

#         def get_version(match):
#             version = match.group("version")
#             return version

#         def get_status(match):
#             status = match.group("status")
#             return status

#         def get_status_verbose(match):
#             status = match.group("status")
#             status_verbose = responses[int(status)] if int(
#                 status) in responses else None
#             return status_verbose

#         def get_size(match):
#             size = match.group("size")
#             return size

#         def get_size_kb(size):
#             size_kb = int(size) / 1024
#             return size_kb

#         def get_size_mb(size):
#             size_mb = int(size) / 1024 / 1024
#             return size_mb

#         if get_user(match) != None or get_session(match) != None:
            
#             clean_logs = CleanLog(id=get_id(body),timestamp=timestamp_to_datetime(match), year=get_year(match), month=get_month(match), day=get_day(match), day_of_week=get_day_of_week(match), time=get_time(match), ip=get_ip(match), country=get_country(get_ip(match)), city=get_city(get_ip(match)), session=get_session(match), user=get_user(match), is_email=get_is_email(get_user(match)), email_domain=get_email_domain(get_user(match)),
#                                 rest_method=get_rest_method(match), url=get_url(match), schema=get_schema(get_url(match)), host=get_host(get_url(match)), status=get_status(match), status_verbose=get_status_verbose(match), size_bytes=get_size(match), size_kilo_bytes=get_size_kb(get_size(match)), size_mega_bytes=get_size_mb(get_size(match)))
#             con.add(clean_logs)
#             con.commit()
#             print("Data inserted successfully")

#             print(
#                 f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")
#             time.sleep(0.05)

#         else:
#             print("Message ignored")
#     else:
#         print("Message ignored")


