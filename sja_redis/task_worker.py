from gtrace2.model import testrun, trinterface, comparison, interface
from gtrace2.tools import task_queue, pubsub, task, task_status
from gtrace2.tools import gtrace_redis 
from gtrace2.utils import misc
from gtrace2.mongo.utils import random_base_62
import random, time
import sys, os

import logging
logger = logging.getLogger('task_worker')

  
class TaskWorker(object):
  def __init__(self, worker_num=None, machine_id=None):
    self._done = False
    self._waiting = True
    self._machine_id = machine_id or misc.get_host()
    self._id = random_base_62(24)
    logger.name = '%s(%s)' % (logger.name, self._id)
    self._stop_watcher = pubsub.SubscriptionHandlingThread(channel='worker_stop',
                                                                 message_handler=self.done_if_this_machine,
                                                                 continue_test=(lambda channel, data: not self._done))
    self.start_ping_response()
    self._workers = gtrace_redis.WorkerConfig(all=True)
    self._description = {'worker': self._id, 'host': self._machine_id}

  def start_ping_response(self):
    self._ping_response = pubsub.SubscriptionHandlingThread(
      channel='ping',
      message_handler=(lambda *args: pubsub.publish('pong', id=self._id, host=self._machine_id)))
    self._ping_response.start()

  def __repr__(self):
    return 'worker %(host)s:%(worker)s' % self._description

  def done_if_this_machine(self, command, data):
    stop = False
    if isinstance(data, dict):
      workers = data.get('workers', [])
      stop = self._id in workers
    else: #general stop of all receiving workers
      stop = True
    if stop:
      self._done = True  # will stop after current task if not sooner
      logger.debug('worker %s is done', self._id)
      if self._waiting:
        self.exit()
        self._workers.remove_local_worker(self._id)
        misc.without_output('kill -9 %s' % os.getpid())

  def exit(self, msg=None):
    msg = msg or 'worker %(worker)s on %(host)s stopped' % self._description
    logger.info(msg)
    pubsub.publish('worker_exited', **self._description)
    self._workers.remove_local_worker(self._id)
    os._exit(0)

  def _note_task_event(self, kind, task):
    task[kind] = time.time()
    pubsub.publish(('%s_task' % kind), task.identity())
    logger.info('%s task %s at %s', kind, str(task.identity()), task[kind])

  def note_task_start(self, task):
    '''
    Log event and publish event
    :param name: name of task
    :param task: the task
    :return: None
    '''
    self._note_task_event('started', task)

  def note_task_end(self, task):
    '''
    Log event and publish event
    :param name: name of task
    :param task: the task
    :return: None
    '''
    self._note_task_event('finished', task)
    task_queue.queue.finished_task(task) # unblock depependent tasks

  def note_task_exception(self, task):
    self._note_task_event('aborted', task)
    task_queue.queue.finished_task(task) # unblock depependent tasks

  def _get_testrun(self, testrun_id):
    tr = testrun.TestRun.get_by_id(testrun_id)
    if not tr:
      logger.error('no TestRun with id %s' % testrun_id)
    
    return tr

  def _get_comparison(self, comparison_id):
    obj = comparison.Comparison.get_by_id(comparison_id)
    if not obj:
      logger.error('no Comparison with id %s' % comparison_id)
    return obj

  def publish_testrun(self, testrun_id, **kwargs):
    '''
    This is done after the subparts so only needs to do wrapup stuff. 
    '''
    tr = self._get_testrun(testrun_id)
    tr.on_completion()

  def mismatch_report(self, comparison_id, refresh_existing=False, produce_hr=True, **params):
    if refresh_existing:
      report = report2.MisMatchReport(comparison_id)
    else:
      report = report2.MisMatchReport.for_compare(comparison_id)
      if produce_hr:
        report.hr_to_file()

    
  def publish_interface(self, testrun_id, if_name, fdv_path, **params):
    tr = self._get_testrun(testrun_id)
    iface = interface.Interface.named(if_name)
    tr_iface = trinterface.TestRunInterface(tr, iface)
    tr_iface.publish_from_fdv(fdv_path)

  def publish_interface_exception(self, testrun_id, if_name, **kwargs):
    tr = self._get_testrun(testrun_id)
    if tr:
      tr.collection().update_one(testrun_id, {'exceptions.%s' % if_name : kwargs.get('exception')})
    else:
      logger.error('could not find testrun %s in publish_interface_exception', testrun_id)

  def compare_testruns(self, comparison_id, **params):
    comparer = self._get_comparison(comparison_id)
    if comparer:
      comparer.on_completion()
    
  def testrun_prelude(self, testrun_id, **kwargs):
    tr = self._get_testrun(testrun_id)
    tr.prelude()

  def comparison_prelude(self, comparison_id, **kwargs):
    obj = self._get_comparison(comparison_id)
    obj.prelude()


  def compare_interface(self, comparison_id, if_name, **params):
    comparer = self._get_comparison(comparison_id)
    if comparer:
      comparer.compare_interface(if_name, **params)

  def main_loop(self):
    logger.info('starting worker %(worker)s on %(host)s' % self._description)
    self._workers.add_local_worker(self._id)
    self._stop_watcher.start()
    logger.info('entering main loop on %s', task_queue.queue._queue)
    while not self._done:
      self._waiting=True
      task = task_queue.queue.get_task()
      if not task:
        continue #hackery to get around blocking get_task()

      self._waiting=False
      name = task['function']

      if task.get('exception'): # e.g., aborted due to exception in depended on task
        exception_fn_name = name + '_exception'
        exception_fn = getattr(self, exception_fn_name, None)
        if exception_fn:
          exception_fn(**task)
        self.note_task_exception(task)
        continue
      key = task.identity()
      if name == 'QUIT':
        self._done = True
        logger.info('worker %s done due to empty task received')
        continue
      try:
        fn = getattr(self, name, None)
        logger.debug('task from queue named %s was found = %s', name, fn != None)
        if fn:
          self.note_task_start(task)
          fn(**task)
          self.note_task_end(task)
      except Exception as e:
        task['exception'] = str(e)
        logger.exception('task exception %s', e)
        self.note_task_exception(task)
    self.exit('%r is done' % self)

if __name__ == '__main__':
  import django
  misc.find_env()
  django.setup()
  logger.info('in script so ensured file logging')
  #print('handled no config')
  #misc.ensure_file_logging(logger)
  logger.info('settings.configured')

  #print('handlers', [h.baseFilename for h in logger.handlers])
  try:
    worker = TaskWorker()
    worker.main_loop()
    # TODO consider multiprocessing with master per worker machine
  except Exception as e:
    print(e)


