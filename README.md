sas7bdat.py
===========

This module will read sas7bdat files using pure Python. No SAS software
required! The module started out as a port of the R script of the same name
found here: <https://github.com/BioStatMatt/sas7bdat>

As is, I've successfully tested the script on around a hundred sample files I
found on the internet. For the most part, it works well. The known issues right
now are:

    1. Read only. No write support.
    2. Can't read compressed data.
    3. Can't read 64-bit data (I don't have any 64 bit sample data that isn't
       compressed so I can't implement this yet).

I'm sure there are more issues that I haven't come across yet. Please let me
know if you come across a data file that isn't supported and doesn't fall into
one of the known issues above and I'll see if I can add support for the file.

Feel free to fork this project and send me pull requests!

