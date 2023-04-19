import re
from urllib.parse import urlparse
from abc import ABC, abstractmethod
from src.log_clean import Log_clean
from datetime import datetime, timedelta
from urllib.parse import urlparse
import socket
import requests
#from ip2geotools.databases.noncommercial import DbIpCity
import geoip2.database
import http


class BaseTransformation:
    
    @abstractmethod
    def transform(self, log: Log_clean) -> Log_clean:
        raise NotImplementedError
    


class UserTransformation(BaseTransformation):

    def transform(self, log: Log_clean) -> Log_clean:
        if domain := self.get_domain_from_email(log.user):
            log.is_email = True
            log.domain = domain
        return log
    
    def get_domain_from_email(self, user: str) -> str :
        regex = re.compile(r"\S+@(?P<domain>\S+.\S+)")
        if match := re.search(regex, user):
            return match.group("domain")
        return None


class UrlTransformation(BaseTransformation):

      def get_schema_host_from_url(self, log: Log_clean) -> Log_clean:
        parsed_url = urlparse(log.url)
        log.schema = parsed_url.scheme
        log.host = parsed_url.netloc
        return log

class StatusCodeTransformation(BaseTransformation):

    def transform(self, log: Log_clean) -> Log_clean:
        try:
            log.status_verbose = http.HTTPStatus(int(log.status)).phrase
        except:
            log.status_verbose="Invalide status"
        return log


class SizeTransformation(BaseTransformation):

    def kilo_mega_bytes(self, log: Log_clean) -> Log_clean:
        size_kilo_bytes=int(log.size)/1024
        log.size_k_b=size_kilo_bytes
        size_mega_bytes=int(log.size)/(1024*1024)
        log.size_m_b=size_mega_bytes
        return log