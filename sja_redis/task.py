from gtrace2.utils import dict_object
from gtrace2.mongo.utils import random_base_62
from gtrace2.utils.misc import bytesToString

class Task(dict_object.DictObject):
  '''
  Task model for GTrace
  '''

  task_name = ''
  id_fields = ('function')

  @classmethod
  def find_class(cls, func_name):
    classes = cls.__subclasses__()
    for a_class in classes:
      if a_class.task_name == func_name:
        return a_class

  @classmethod
  def from_json(cls, data):
    if not isinstance(data, dict) or 'function' not in data:
      return None
    a_class = cls.find_class(data.pop('function'))
    if a_class:
      return a_class(**data)

  def __init__(self, dependent_on=None, finished=False, **kwargs):
    '''
    All except for the worker function desired all other arguments are free form key values
    :param task_function_name:
    :param kwargs:
    '''
    dependent_on = dependent_on or []
    if not 'key' in kwargs:
      kwargs['key'] = random_base_62(48)

    super().__init__(function=self.task_name, dependent_on=dependent_on, **kwargs)

  def __eq__(self, other):
    return hash(self) == hash(other)

  def __hash__(self):
    return hash(tuple([getattr(self, k,None) for k in self.id_fields]))

  def identity(self):
    '''
    This should be a subsection of the task data that uniquely identifies a task.
    This default definition is inadequate and should be overridden by subclasses.
    :return: the identity tuple
    '''
    fields = ['%s=%s' % (k, self[k]) for k in self.id_fields if k != 'function']
    return '%s(%s)' % (self['function'], ', '.join(fields))

  def __repr__(self):
    return self.identity()

  def __str__(self):
    return self.__repr__()
  
  def depends_on(self, *tasks):
    current = set(self.dependent_on)
    current.update(tasks)
    self.dependent_on = list(current)

  def other_args(self):
    return {k:v for k,v in self.items() if k not in self.id_fields}

  def run(self):
    pass

  def should_propagate_exception(self,task):
    '''
    Returns whether an exception in task that this task depends on propages
    to being an exception on this task
    :param task: The task depended on that had an exception
    :return: boolean
    '''
    return True
    
class TestRunPrelude(Task):
  task_name = 'testrun_prelude'
  id_fields = ('function', 'testrun_id')
  
  def __init__(self, testrun_id, **kwargs):
    super().__init__(testrun_id=testrun_id, **kwargs)

  def should_propagate_exception(self,task):
    return False

class MismatchReport(Task):
  task_name = 'mismatch_report'
  id_fields = ('function', 'comparison_id')

  def __init__(self, comparison_id, **kwargs):
    super().__init__(comparison_id=comparison_id, **kwargs)

class ComparisonPrelude(Task):
  task_name = 'comparison_prelude'
  id_fields = ('function', 'comparison_id')
  
  def __init__(self, comparison_id, **kwargs):
    super().__init__(comparison_id=comparison_id, **kwargs)

  def should_propagate_exception(self,task):
    return False

class PublishTestrun(Task):
  task_name = 'publish_testrun'
  id_fields = ('function', 'testrun_id')

  def __init__(self, testrun_id, **kwargs):
    super().__init__(testrun_id=testrun_id, **kwargs)


  def should_propagate_exception(self, task):
    return False


class PublishInterface(Task):
  task_name = 'publish_interface'
  id_fields = ('function', 'testrun_id', 'if_name')

  def __init__(self, testrun_id, if_name, fdv_path, **kwargs):
    super().__init__(testrun_id=testrun_id, if_name=if_name, fdv_path=fdv_path, **kwargs)

  def __hash__(self):
    return hash(tuple([self.fdv_path] + [getattr(self, k,None) for k in self.id_fields]))

class CompareTestruns(Task):
  task_name = 'compare_testruns'
  id_fields = ('function', 'comparison_id')

  def __init__(self, comparison_id, **kwargs):
    super().__init__(comparison_id=comparison_id, **kwargs)


  def should_propagate_exception(self, task):
    return False


class CompareInterface(Task):
  task_name = 'compare_interface'
  id_fields = ('function', 'comparison_id', 'if_name')
  
  def __init__(self, comparison_id, if_name, **kwargs):
    super().__init__(comparison_id=comparison_id, if_name=if_name, **kwargs)

def task_identity(dict_task):
  a_class = Task.find_class(dict_task['function'])
  if a_class:
    fields = ['%s=%s' % (k, dict_task[k]) for k in a_class.id_fields if k != 'function']
    return '%s(%s)' % (dict_task['function'], ', '.join(fields))

def wait_key(task_id):
  return 'waiting:' + bytesToString(task_id)
