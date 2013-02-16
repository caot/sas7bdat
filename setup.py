#!/usr/bin/env python
from distutils.core import setup


setup(name='sas7bdat',
      version='0.1.0',
      author='Jared Hobbs',
      author_email='jared@pyhacker.com',
      url='http://git.pyhacker.com/sas7bdat',
      description='A sas7bdat file reader for Python',
      py_modules=['sas7bdat'],
      scripts=['scripts/sas7bdat_to_csv'])

