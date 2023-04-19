import hashlib
import re

#chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties,

class ProcessMsgDataLake():

    regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")
    
    def __init__(self, body:str):
        self.body=body #.decode("utf-8")

    def get_log(self):
        return self.body

    def get_timestamp(self):
        pattern = re.search("\[(?P<timestamp>\S{20})", self.body)
        datetime=pattern.group('timestamp')
        return datetime
    
    def hash_body(self):
        hashing_body = hashlib.md5(self.body.encode()).hexdigest()
        return hashing_body

    def get_user(self):
        pattern = re.search(self.regex, self.body)
        user = pattern.group("user")
        if user == "-":
            return None
        return user   

    def get_session(self):
        pattern = re.search(self.regex, self.body)
        session = pattern.group("session")
        if session == "-":
            return None
        return session 