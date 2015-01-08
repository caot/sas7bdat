#!/usr/bin/env python
from __future__ import division, absolute_import, print_function,\
    unicode_literals
import sys
from distutils.core import setup


if sys.version_info < (2, 7):
    print("Sorry, this module only works on 2.7+, 3+")
    sys.exit(1)


setup(name='sas7bdat',
      version='2.0.2',
      author='Jared Hobbs',
      author_email='jared@pyhacker.com',
      license='MIT',
      url='http://git.pyhacker.com/sas7bdat',
      description='A sas7bdat file reader for Python',
      py_modules=['sas7bdat'],
      scripts=['scripts/sas7bdat_to_csv'],
      install_requires=['six>=1.8.0'],
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: MIT License',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Topic :: Text Processing',
          'Topic :: Utilities',
      ],
      keywords=['sas', 'sas7bdat', 'csv', 'converter'])
