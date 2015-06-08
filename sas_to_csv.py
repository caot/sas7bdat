#!python

from __future__ import print_function
from sas7bdat import SAS7BDAT

import glob


def to_csv(filename):
    fileout = '%s.csv' % filename
    fw = open(fileout, 'wb')

    def to_file(value):
        '''
        print(value, ..., sep=' ', end='\n', file=sys.stdout)
        '''
        print(value, sep=' ', end='\n', file=fw)

    with SAS7BDAT(filename) as f:
        for row in f:
            print(','.join(str(v) for v in row), sep=' ', end='\n', file=fw)

    fw.close()


def get_files_names(file_ext='*.sas7bdat'):
    return glob.glob(file_ext)


def to_csv_of_all_files():
    for filename in get_files_names():
        to_csv(filename)

    print('Done ... to csv')


if __name__ == "__main__":
    to_csv_of_all_files()
