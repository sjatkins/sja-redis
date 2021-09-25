__author__ = 'samantha'
from setuptools import setup, find_packages

setup(name='sja_redis',
      version='0.1',
      description='some useful redis abstractions and usages',
      author='Samantha Atkins',
      author_email='sjatkins@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires = ['redis'],
      zip_safe=False)
