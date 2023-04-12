import hashlib
import re

regex = re.compile(r"(?P<session>\S{1}|\S{15}) (?P<user>\S{1,50})")
def get_log(body):
        return body

def get_timestamp(body):
    pattern = re.search("\[(?P<timestamp>\S{20})", body)
    datetime=pattern.group('timestamp')
    return datetime
        
def hash_body(body):
    hash_body = hashlib.md5(body.encode()).hexdigest()
    return hash_body   

def get_user(body):
    pattern = re.search(regex, body)
    user = pattern.group("user")
    if user == "-":
        return None
    return user   

def get_session(body):
    pattern = re.search(regex, body)
    session = pattern.group("session")
    if session == "-":
        return None
    return session