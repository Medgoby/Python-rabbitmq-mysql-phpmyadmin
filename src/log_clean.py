import re
from urllib.parse import urlparse
import requests
import hashlib



from ip2geotools.databases.noncommercial import DbIpCity
from pycountry import countries
from datetime import datetime, timedelta
import urllib.parse
from http.client import responses

import pandas as pd



class Log_clean:

    regex = re.compile(r"(?P<ip>\S{7,15}) (?P<session>\S{1}|\S{15}) (?P<user>\S{1,50}) \[(?P<timestamp>\S{20}) "
                           r"(?P<utc>\S{5})\] \"(?P<method>GET|POST|DELETE|PATCH|PUT|HEAD) (?P<url>\S{1,9500}) "
                           r"(?P<version>\S{1,10})\" (?P<status>\d{3}) (?P<size>\d+) -")
    
    def __init__(self):
        self.id=None
        self.ip = None
        self.user = None
        self.is_email = False
        self.domain = None
        self.status = None
        self.status_verbose = None
        self.timestamp=None
        self.method=None
        self.schema=None
        self.host=None
        self.size=None
        self.size_k_b=None
        self.size_m_b=None
        self.url=None
        self.year=None
        self.month=None
        self.day=None
        self.day_of_week=None
        self.time=None
        self.country=None
        self.city=None
        self.session=None
        self.rest_vers=None

    def matched(self, line: str):
        match = re.search(self.regex, line)
        if match != None :
            self.ip = match.group("ip")
            self.user = match.group("user")
            self.status = match.group("status")
            self.session=match.group("session")
            self.method=match.group("method")
            self.size=match.group("size")
            self.url=match.group("url")
        else:
            print('Log invalid')
    
    def timestamp_to_datetime(self,line):
        match = re.search(self.regex, line)

        timestamp = match.group("timestamp")
        format = "%d/%b/%Y:%H:%M:%S"
        date_time = datetime.strptime(timestamp, format)
        date_time = date_time + timedelta(hours=5)
        date_time=pd.to_datetime(date_time)
        self.timestamp = date_time
        self.year = date_time.strftime("%Y")
        self.month = date_time.strftime("%m")
        self.day = date_time.strftime("%d")
        self.day_of_week = date_time.strftime("%A")
        self.time = date_time.strftime("%H:%M:%S")
        
    def hash_body(self, line):
        self.id = hashlib.md5(line.encode()).hexdigest()

    def get_location(self, ip_adr: str):
        ip_address = ip_adr
        response = requests.get(f'https://ipapi.co/{self.ip}/json/').json()
        location_data = {
        "ip": ip_address,
        "city": response.get("city"),
        "region": response.get("region"),
        "country": response.get("country_name")
        }
        self.country=location_data.get("country")
        self.city=location_data.get("city")
  
    def get_country(self,line:str):
        match = re.search(self.regex, line)
        ip_matches = match.group("ip")
        if match != None :
            res = DbIpCity.get(ip_matches, api_key="free")
            self.country = str.upper(countries.get(alpha_2=res.country).name)
            self.city = str.upper(res.city)
        else:
            self.country = str.upper("Unknown")
            self.city = str.upper("Unknown")

    
    def get_user(self,line):
        pattern = re.search(self.regex, line)
        user = pattern.group("user")
        if user == "-":
            return "Unknown"
        return user   

    def get_session(self,line):
        pattern = re.search(self.regex, line)
        session = pattern.group("session")
        if session == "-":
            return "Unknown"
        return session


    def TimeStamp(self, line: str):
        timestamp_regex = r'\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})\]'
        match = re.search(timestamp_regex, line)
        self.timestamp=match.group().replace("[","").replace("]","")
      
    
    def rest_version(self, line: str):
        pattern = r'HTTP/(\d\.\d)'
        match = re.search(pattern, line)
        self.rest_vers=match.group()
    
    def __str__(self)->str:
         return f"""timestamp:{self.timestamp}\nyear:{self.year}\nmonth:{self.month}\nday:{self.day}\nday_of_week:{self.day_of_week}\ntime:{self.time}\n
         ip: {self.ip}\ncountry:{self.country}\ncity:{self.city}\nsession:{self.session}\nuser: {self.user}\nis_email: {self.is_email}\ndomain: {self.domain}\n
         method:{self.method}\n url:{self.url}\nschema:{self.schema}\nhost:{self.host}\nrest_version:{self.rest_vers}\nstatus: {self.status}\n
#        status_verbose: {self.status_verbose}\nsize:{self.size}\nsize_k_b:{self.size_k_b}\nsize_m_b:{self.size_m_b}
#         """
