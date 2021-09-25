from gtrace2.tools import task_queue as tq, gtrace_redis as gr, task as tm, pubsub
from gtrace2.model import testrun, comparison
import logging

logger = logging.getLogger('task_status')

class TaskStatus:
  def wait_for(self, kind, kind_id):
    '''
    Looks up and if needed waits for the completion of the task with the given main_id or satisfying criteria.
    The task_kind matchs those understood by task_waiter.  But the common cases are
    1) publish_testrun - wait for the testrun to be fully published
    2) compare_testruns - wait for the comparison tocomplete
    These common cases only need 'main_id' set to the Testrun._id or Comparison._id respectively or '_id=xxxx'.  
    If used for publish_interface or compare_interface then more criteria are needed as detailed under task_worker.
    @param kind: 'testrun' or 'comparison' for now
    @param kind_id: _id of the kind waited for 
    @return: 'completed' or 'aborted'
    '''
    kind = kind.lower()
    if kind == 'testrun':
      function = 'publish_testrun'
      id_name = 'testrun_id'
    elif kind == 'comparison':
      function = 'compare_testruns'
      id_name = 'comparison_id'
    else:
      raise Exception('TaskStatus.waitfor cannot wait for %s' % kind)

    task_criteria = {'function': function, id_name: kind_id}
    taskrep = '%s(%s=%s)' % (function, id_name, kind_id)
    res = {'status': None}

    def matches(criteria, data):
      if isinstance(criteria, dict):
        return all([data.get(k) == criteria[k] for k in criteria])
      else:
        return data == criteria

    active_task = self.find_task(function, **task_criteria)
    if active_task:
      subscription = pubsub.subscribe('finished_task', 'aborted_task')

      def not_found_yet(channel, data):
        if matches(taskrep, data):
          res['status'] = channel.split('_')[0]
          return False
        return True

      waiter = pubsub.SubscriptionHandlingThread(subscription=subscription, continue_test=not_found_yet)
      waiter.start()
      waiter.join()
      return res['status']

    else:

      db_status = None

      def check_collection(dbo_class):
        obj = dbo_class.collection().get(kind_id)
        return obj and obj['status']

      if kind == 'testrun':
        db_status = check_collection(testrun.TestRun)
      elif kind == 'comparison':
        db_status = check_collection(comparison.Comparison)

      if not db_status:
        logger.info('asked to waitfor unknown kind %s with id %s', kind, kind_id)
        return 'unknown'
      else:
        expected = 'published' if (kind == 'testrun') else 'compared'
        return 'finished' if (expected == db_status) else 'aborted'

  def __init__(self):
    pass

  def get_task(self, task_id):
    return tq.queue.get_active_task(task_id)

  def waiting_on(self, task_id):
    if not self.get_task(task_id):
      return []
    wait_key = tm.wait_key(task_id)
    waiting_on_ids = tq.queue.task_waiting_on(task_id)
    tasks = tq.queue.get_tasks_in(*waiting_on_ids)
    return [t['key'] for t in tasks ]

  def waiting_tasks(self):
    res = {}
    for k, v in tq.queue.active_tasks().items():
      waiting = self.waiting_on(k)
      res[k] = {
        'task': tm.task_identity(v),
        'waiting_on': sorted(waiting),
        }
    return res

  def find_task(self, task_function, **identities):
    def matches(task):
      return task and task['function'] == task_function and all([task[k] == v for k, v in identities.items()])


    for task in tq.queue.active_tasks().values():
      if matches(task):
        return task

  def task_status(self, task_id=None, **task_description):
    res = {}
    task = tq.queue.get_active_task(task_id) if task_id else self.find_task(**task_description)
    if not task:
      res['status'] = 'non-existent';  # TODO go to database?

    task_id = task_id or task['key']

    waiting_on = self.waiting_on(task_id)
    if waiting_on:
      return {'waiting_on': waiting_on}  # TODO expand report to report on what is waited on?
