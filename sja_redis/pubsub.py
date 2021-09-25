
import json, threading
from gtrace2.tools import gtrace_redis

import logging

logger = logging.getLogger('gtrace-pubsub')

class PubSub(object):
  def subscribe(self, key):
    pass

  def publish(self, key, **data):
    pass

  def unsubscribe(self, key):
    pass


class RedisPubSub(object):
  def __init__(self, host=None):
    self.redis = gtrace_redis.GTraceRedis(host=host)
    self._pubsub = self.redis.pubsub()

  def _received_data(self,  raw):
    data = gtrace_redis.ensure_string(raw['data'])
    return json.loads(data)

  @property
  def subscription(self):
    return self._pubsub

  def subscribe(self, *keys, prefix=True):
    '''
    This sets up a subscription to some 'channels' (keys).  It is advantages generally
    to preface the channel names with GTrace_SUFFIX.  This allows for instance the production
    workers to only receive and create events for production workers and machinery and staging
    and dev workers to not receive these production messages.
    :param keys:
    :return:
    '''
    if prefix:
      suffix = self.redis.suffix
      keys = ['%s:%s' % (suffix,k) for k in keys]
    logger.debug('subscribing to %s', keys)
    return self.redis.subscribe(*keys)

  def get_message(self, float_seconds=None):
    '''
    Return the next message ignoring subsciption messages
    :param key: the key or channel
    :param float_seconds:
    :return:
    '''
    data = self._pubsub.get_message(timeout=float_seconds)
    if not data:
      return None
    if data['type'] != 'message':
      return self.get_message(float_seconds=float_seconds)
    return self._received_data(data)

  def listen(self):
    return (self._received_data(item) for item in self._pubsub.listen())

  def unsubscribe(self, key):
    self._pubsub.unsubscribe(key)

  def publish(self, pub_key, something=None, prefix=True, **data):
    if prefix:
      pub_key = self.redis.suffix + ':' + pub_key
    self.redis.publish(pub_key, something, **data)

class SubscriptionHandlingThread(threading.Thread):
  def __init__(self, subscription=None, channel=None, message_handler=None, continue_test=None):
    '''
    Handles messages from a Redis subscription.to one or more channel.
    Initialized with a redis subscription object, a function to be invoked per message
    and a function used to check whether the message received means we
    should stop listening. 
    @param subscription: a redis subscription to one or more channel.  Must have either subscription or channel set
    @param channel: a redis channel to subscribe and listen to.  Specify eith channel on subscription but not both
    @param message_handler: optional function taking channel and message data and doing some processing of it
    @param continue_test: optional boolean function taking channel and message that returns whether thread should continue listening
    '''
    self._pubsub = RedisPubSub()
    super().__init__(name='subscription_handler', daemon=True)
    self.setDaemon = True
    if subscription and channel:
      raise Exception('specify channel or subscription but not both')
    if channel:
      logger.info('got chanel %s', channel)
      subscription = self._pubsub.subscribe(channel)
    self._subscription = subscription
    self._handler = message_handler
    self._continue = continue_test or (lambda channel, data: True)
    self._done = False

  def run(self):
    logger.debug('subscription handler running')
    for msg in self._subscription.listen():
      #logger.debug('got message %s', msg)
      if msg['type'] != 'message':
        continue
      channel, data = gtrace_redis.subscribed_message_parts(msg)
      logger.info('subscription handler received %s', (channel, data))
      if self._handler:
        self._handler(channel, data)
      if not self._continue(channel, data):
        self._done = True
        break


pubsub = RedisPubSub()
publish = pubsub.publish
subscribe = pubsub.subscribe
unsubscribe = pubsub.unsubscribe
