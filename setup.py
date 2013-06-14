#!/usr/bin/env python
import sys
from distutils.core import setup


if sys.version_info < (2, 6):
    print "Sorry, this module only works on 2.6+"
    sys.exit(1)


setup(name='sas7bdat',
      version='0.2.1',
      author='Jared Hobbs',
      author_email='jared@pyhacker.com',
      license='MIT',
      url='http://git.pyhacker.com/sas7bdat',
      description='A sas7bdat file reader for Python',
      py_modules=['sas7bdat'],
      scripts=['scripts/sas7bdat_to_csv'])

