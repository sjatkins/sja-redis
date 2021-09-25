import json, logging
import logging, threading, pdb
from itertools import takewhile
import redis

def ensure_string(val):
  return val.decode() if isinstance(val, bytes) else val

def subscribed_message_parts(msg):
  if msg['type'] != 'message':
    return None, None
  msg = fix_bytes(msg)
  data = msg['data']
  print ('data', msg['data'])
  if data.startswith('{') or data.startswith('['):
    data = json.loads(msg['data'])  # expects a list of values
  return msg['channel'], data

class RedisJSONMap:
  def __init__(self, gredis, map_name):
    self._gredis = gredis
    self._name = map_name

  def keys(self):
    return [bytesToString(s) for s in self._gredis.hkeys(self._name)]

  def put(self, key, data):
    self._gredis.hset(self._name, key, json.dumps(data))

  def get(self, key):
    raw = self._gredis.hget(self._name, bytesToString(key))
    return convert_stored_json(raw) if raw else {}

  def get_items(self, *keys):
    return [obj for obj in map(self.get, keys) if obj]

  def remove(self, key):
    self._gredis.hdel(self._name, key)

  def get_all(self):
    return {k: self.get(k) for k in self.keys()}

class GenericRedis:
  def __init__(self, host=None):
    self._host = host
    self._redis = redis.Redis(host=host)
    
  def pubsub(self):
    '''
    Redis requires an intermediate object for handling subscriptions
    and listening for data published to subscribed to channel.
    Typically only one of these would be used for a client per
    channel or semantically related set of channels that are listened to
    together.
    :return: an instance of the pubsub intermediate object.
    '''
    return self._redis.pubsub()

  def subscribe(self, *channels):
    p = self._redis.pubsub()
    p.subscribe(*channels)
    return p

  def publish(self, pub_key, something=None, **data):
    if data and not something:
      something = json.dumps(data)
    logger.debug('key = %s, data = %s', pub_key, something)
    self._redis.publish(pub_key, something)

  def queue_put(self, queue_name, data):
    '''
    Push the json encoded form of the data to the end of
    a list-like structure in Redis that is treated as a FIFO
    :param queue_name: redis key of the queue object
    :param data: the data to json encode and push
    :return: what redis returns. Namely current length of the queue
    '''
    return self._redis.rpush(queue_name, json.dumps(data))

  def queue_get(self, *queue_names):
    '''
    Take and return  item from first of the named queues that 
    has one.
    '''
    raw = self._redis.blpop(queue_names)
    data = ensure_string(raw[1])
    return json.loads(data)

  def hget(self, dict_name, key):
    return self._redis.hget(dict_name, key)

  def hset(self, dict_name, key, value):
    return self._redis.hset(dict_name, key, value)

  def hdel(self, dict_name, *keys):
    return self._redis.hdel(dict_name, *keys)

  def smembers(self, set_name):
    return self._redis.smembers(set_name)

  def remove(self, *keys):
    return self._redis.delete(*keys)

  def __getattr__(self, key):
    val = getattr(self._redis, key)
    if val is None:
      raise AttributeError
    return val
