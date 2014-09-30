sas7bdat.py
===========

This module will read sas7bdat files using pure Python (2.7+). No SAS software
required! The module started out as a port of the R script of the same name
found here: <https://github.com/BioStatMatt/sas7bdat>

Also included with this library is a simple command line script,
`sas7bdat_to_csv`, which converts sas7bdat files to csv files. It will also
print out header information and meta data using the `--header` option and it
will batch convert files as well. Use the `--help` option for more information.

As is, I've successfully tested the script on around a hundred sample files I
found on the internet. For the most part, it works well. The known issues right
now are:

1. Read only. No write support.
2. Can't read compressed data.

I'm sure there are more issues that I haven't come across yet. Please let me
know if you come across a data file that isn't supported and doesn't fall into
one of the known issues above and I'll see if I can add support for the file.

Usage
=====

To create a sas7bdat object, simply pass the constructor a file path:

```
#!python
from sas7bdat import SAS7BDAT
    f = SAS7BDAT('foo.sas7bdat')
```

Read the data using the `readData` method:

```
#!python
for line in f.readData():
    # do something...
```

If you'd like to get a pandas DataFrame, use the `toDataFrame` method:

```
#!python
df = f.toDataFrame()
```