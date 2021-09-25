from gtrace2.tools import pubsub, gtrace_redis, task as tm
gredis = gtrace_redis.GTraceRedis()
import logging
from gtrace2.utils.misc import fix_bytes, suffix_adder, convert_stored_json, bytesToString
import threading
import json
from collections import defaultdict
from django.conf import settings

logger = logging.getLogger('task_queue')

class TaskWaiter(threading.Thread):
  def __init__(self, tasks, final_function=None):
    super().__init__(name='task_waiter')
    self._fn = final_function
    self.setDaemon(True)
    self._waiting_on = {t.key for t in tasks}
    self.subscription = pubsub.subscribe(['finished_task', 'aborted_task'])

  def run(self):
    for msg in self.subscription.listen():
      if msg['type'] != 'message':
        continue
      msg = fix_bytes(msg)
      logger.debug('task waiter received %s' % msg)
      task_data = json.loads(msg['data'])  # expects a list of values
      task = tm.Task.from_json(task_data)
      key = task.key
      if key in self._waiting_on:
        self._waiting_on.discard(key)
        if not self._waiting_on: # all done
          break
    self.subscription.unsubscribe()
    if self._fn:
      self._fn()


class RedisQueue:
  def __init__(self, queue_name=None, ready_queue_name=None, host=None):
    self._redis = gtrace_redis.GTraceRedis(host=host)
    self.set_suffix()

  @property
  def gredis(self):
    return self._redis

  def set_suffix(self, suffix=None):
    self._suffix = suffix or settings.GTRACE_SUFFIX
    w_suffix = suffix_adder(self._suffix, ':')
    self._queue = w_suffix('task_queue')
    self._ready = w_suffix('ready_queue')  #former waters now free
    self._flush = w_suffix('flush_queue')
    self._active = w_suffix('active_tasks')
    #logger.debug('using queues %s', (self._queue, self._ready, self._flush))
    
  def get(self):
    return self._redis.queue_get(self._ready, self._queue, self._flush) # TODO add to called

  def put(self, item):
    self._redis.queue_put(self._queue, item)



class RedisTaskQueue(RedisQueue):

  def __init__(self, host=None):
    super().__init__(host=host)
    self._tasks = gtrace_redis.RedisJSONMap(self.gredis, self._active)

  def _waiting_task(self, task_key):
    return 'waiting:' + task_key

  def active_tasks(self):
    return self._tasks.get_all()

  def get_active_task(self, task_key):
    return self._tasks.get(task_key)
    
  def add_dependent_task(self, task):
    logger.debug('setting up wait of %s on %s', task.identity(), [tm.task_identity(t) for t in task.dependent_on])
    key = task.key
    wait_key = tm.wait_key(key)
    for t in task.dependent_on:
      tkey = t.get('key')
      self.gredis.sadd(tkey, task.key)
      self.gredis.sadd(wait_key, tkey)

  def get_task(self):
    raw = self.get()
    task = tm.Task.from_json(raw)
    return task

  def put_task(self, task, was_waiting=False):
    if not was_waiting:
      self._tasks.put(task.key, task)
      logger.debug('new active task %s = %s', task.key, tm.task_identity(task))
    if task.dependent_on and not was_waiting:
      self.add_dependent_task(task)
    else:
      logger.info('enqueued task %s on %s', task.identity(), self._queue)
      queue_name = self._ready if was_waiting else self._queue
      self.gredis.queue_put(queue_name, task)

  def get_tasks_in(self, *task_ids, instantiate=False):
    raw = self._tasks.get_items(*task_ids)
    return list(map(tm.Task.from_json, raw)) if instantiate else raw

  def task_waiting_on(self, task_id):
    wait_key = tm.wait_key(task_id)
    still_waiting = [bytesToString(s) for s in self.gredis.smembers(wait_key)]
    keys = self._tasks.keys()
    #return still to be processed tasks depended on
    return [s for s in still_waiting if s in keys]

  def task_started_on(self, task_id, worker_id):
    task = self._tasks.get(task_id)
    self._tasks.update(task_id, started_on=worker_id)

  def finished_task(self, task):
    'handles task completion'
    def get_identity(task_id):
      task_id = bytesToString(task_id)
      data = self._tasks.get(task_id)
      return tm.task_identity(data) if data else 'unknown_%s' % task_id

    def handle_waiting(task):
      still_waiting = self.task_waiting_on(task.key)
      if not still_waiting:
        self.gredis.remove(wait_key)
        logger.debug('previously waiting %r no longer waiting', task.identity())
        self.put_task(task, was_waiting=True)
      else:
        logger.debug('%r still waiting on %s', task, [get_identity(t) for t in still_waiting])

    logger.debug('finished task %s', task.identity())
    who_cares = self.gredis.smembers(task.key)
    logger.debug('finished %s removed from waiters %s', task.key, who_cares)
    had_problem = 'exception' in task
    for key in who_cares:
      key = bytesToString(key)
      wait_key = tm.wait_key(key)
      self.gredis.srem(wait_key, task.key)
      still_waiting = self.gredis.smembers(wait_key)
      d_task = self._tasks.get(key)
      if not d_task:
        logger.info('dependent task with id %s no longer present', key)
        continue
      d_task = tm.Task.from_json(d_task)
      if had_problem and d_task.should_propagate_exception(task):
        logger.debug('propagated exception %s from %s to %s', task['exception'], task.identity(), d_task.identity())
        d_task.exception = 'Aborted due to exception on dependent task %s' % (task.identity(),)
      handle_waiting(d_task) # could clean up early on exception propagation
    self.gredis.remove(task.key)
    self._tasks.remove(task.key)


  def enqueue_and_wait(self, overall_task, subtasks, prelude):
    '''
    This version of a task with subparts that must be completed before it is completed
    makes the overall_task dependent on each of the subtasks.  It is expected that the
    overall task worker function invokes what it needs to order to do any final work of
    the task.
    :param overall_task: top level container like tasks dependent on subtasks
    :param subtasks: subtasks it is dependent upon for completion
    :param prelude: name of prelude function on the object associated with the overall_task defaults to 'prelude'
    '''
    overall_task.depends_on(*subtasks)
    for task in subtasks:
      task.depends_on(prelude)
      self.put_task(task)
    overall_task.dependent_on = [prelude] + overall_task.dependent_on  #ensure prelude comes first
    self.put_task(overall_task) #overal first but gated by prelude so overall is present for early finisihing subtasks.
    self.put_task(prelude)

  def push_worker_end(self, worker_id):
    self.gredis.queue_put(self._flush, worker_id)


queue = RedisTaskQueue()
