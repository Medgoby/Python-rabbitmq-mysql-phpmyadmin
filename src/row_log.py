import re
import hashlib
from datetime import datetime, timezone, timedelta



import hashlib
import re


# class Log_lake:

#     regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")

#     def __init__(self):
#         self.id = None
#         self.timestamp = None
#         self.log = None

#     def __str__(self) -> str:
#         return f"""id: {self.id}\ntimestamp: {self.timestamp}\nline: {self.log}"""
    

#     def get_log(self,line:str):
#         return line

#     def get_timestamp(self,line:str):
#         pattern = re.search("\[(?P<timestamp>\S{20})", line)
#         datetime=pattern.group('timestamp')
#         return datetime
            
#     def hash_body(self,line:str):
#         hash_body = hashlib.md5(line.encode()).hexdigest()
#         return hash_body   

#     def get_user(self,line:str):
#         #regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")
#         pattern = re.search(self.regex, line)
#         user = pattern.group("user")
#         if user == "-":
#             return None
#         return user   

#     def get_session(self,line):
#         #regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")
#         pattern = re.search(self.regex, line)
#         session = pattern.group("session")
#         if session == "-":
#             return None
#         return session

class Log_lake:
    regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")
    def __init__(self):
        self.id = None
        self.timestamp = None
        self.log = None

    def __str__(self) -> str:
        return f"""id: {self.id}\ntimestamp: {self.timestamp}\nline: {self.log}"""


    def hash_body(self, line):
        self.id = hashlib.md5(line.encode()).hexdigest()

    def get_TimeStamp(self, line: str):
        pattern = re.search("\[(?P<timestamp>\S{20})", line)
        self.timestamp=pattern.group('timestamp')

    
    def line (self,line:str):
        self.line=line.replace('"','')
        delimiter = "-"
        replacement = ""
        parts = self.line.rsplit(delimiter, 1)
        self.log = replacement.join(parts)
        print(self.line)
    
