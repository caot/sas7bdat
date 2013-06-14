#!/usr/bin/env python
"""
A sas7bdat reader. File format taken from
https://github.com/BioStatMatt/sas7bdat/blob/master/inst/doc/sas7bdat.rst
"""
import os
import sys
import csv
import struct
import logging
import platform
from cStringIO import StringIO
from datetime import datetime, timedelta
from collections import namedtuple

__all__ = ['SAS7BDAT']


def _debug(t, v, tb):
    if hasattr(sys, 'ps1') or not sys.stderr.isatty():
        sys.__excepthook__(t, v, tb)
    else:
        import pdb
        import traceback
        traceback.print_exception(t, v, tb)
        print
        pdb.pm()
        os._exit(1)


def _getColorEmit(fn):
    # This doesn't work on Windows since Windows doesn't support
    # the ansi escape characters
    def _new(handler):
        levelno = handler.levelno
        if levelno >= logging.CRITICAL:
            color = '\x1b[31m'  # red
        elif levelno >= logging.ERROR:
            color = '\x1b[31m'  # red
        elif levelno >= logging.WARNING:
            color = '\x1b[33m'  # yellow
        elif levelno >= logging.INFO:
            color = '\x1b[32m'  # green or normal
        elif levelno >= logging.DEBUG:
            color = '\x1b[35m'  # pink
        else:
            color = '\x1b[0m'   # normal
        handler.msg = color + handler.msg + '\x1b[0m'  # normal
        return fn(handler)
    return _new


class SAS7BDAT(object):
    MAGIC = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc2\xea\x81\x60"\
            "\xb3\x14\x11\xcf\xbd\x92\x08\x00\x09\xc7\x31\x8c\x18\x1f\x10\x11"

    # Host systems known to work
    KNOWNHOSTS = set(["WIN_PRO", "WIN_NT", "WIN_NTSV", "WIN_SRV", "WIN_ASRV",
                      "XP_PRO", "XP_HOME", "NET_ASRV", "NET_DSRV", "NET_SRV",
                      "WIN_98", "W32_VSPRO", "WIN", "WIN_95", "X64_VSPRO",
                      "AIX", "X64_ESRV", "W32_ESRV", "W32_7PRO", "W32_VSHOME",
                      "X64_7HOME", "X64_7PRO", "X64_SRV0", "W32_SRV0",
                      "X64_ES08", "Linux", "HP-UX"])

    # Subheader signatures, 32 and 64 bit, little and big endian
    SUBH_ROWSIZE = set(["\xF7\xF7\xF7\xF7", "\x00\x00\x00\x00\xF7\xF7\xF7\xF7",
                        "\xF7\xF7\xF7\xF7\x00\x00\x00\x00"])
    SUBH_COLSIZE = set(["\xF6\xF6\xF6\xF6", "\x00\x00\x00\x00\xF6\xF6\xF6\xF6",
                        "\xF6\xF6\xF6\xF6\x00\x00\x00\x00"])
    SUBH_COLTEXT = set(["\xFD\xFF\xFF\xFF", "\xFF\xFF\xFF\xFD",
                        "\xFD\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                        "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFD"])
    SUBH_COLATTR = set(["\xFC\xFF\xFF\xFF", "\xFF\xFF\xFF\xFC",
                        "\xFC\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                        "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFC"])
    SUBH_COLNAME = set(["\xFF\xFF\xFF\xFF",
                        "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"])
    SUBH_COLLABS = set(["\xFE\xFB\xFF\xFF", "\xFF\xFF\xFB\xFE",
                        "\xFE\xFB\xFF\xFF\xFF\xFF\xFF\xFF",
                        "\xFF\xFF\xFF\xFF\xFF\xFF\xFB\xFE"])
    SUBH_COLLIST = set(["\xFE\xFF\xFF\xFF", "\xFF\xFF\xFF\xFE",
                        "\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                        "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE"])
    SUBH_SUBHCNT = set(["\x00\xFC\xFF\xFF", "\xFF\xFF\xFC\x00",
                        "\x00\xFC\xFF\xFF\xFF\xFF\xFF\xFF",
                        "\xFF\xFF\xFF\xFF\xFF\xFF\xFC\x00"])

    # Page types
    PAGE_META = 0
    PAGE_DATA = 256        # 1 << 8
    PAGE_MIX = [512, 640]  # 1 << 9, 1 << 9 | 1 << 7
    PAGE_AMD = 1024        # 1 << 10
    PAGE_METC = 16384      # 1 << 14 (compressed data)
    PAGE_COMP = -28672     # ~(1 << 14 | 1 << 13 | 1 << 12)
    PAGE_MIX_DATA = PAGE_MIX + [PAGE_DATA]
    PAGE_META_MIX_AMD = [PAGE_META] + PAGE_MIX + [PAGE_AMD]
    PAGE_ANY = PAGE_META_MIX_AMD + [PAGE_DATA, PAGE_METC, PAGE_COMP]

    def __init__(self, path, logLevel=logging.INFO):
        if logLevel == logging.DEBUG:
            sys.excepthook = _debug
        self.path = path
        self.logger = self._makeLogger(level=logLevel)
        self.header = self._readHeader()
        self.logger.debug(str(self.header))

    def _makeLogger(self, level=logging.INFO):
        """
        Create a custom logger with the specified properties.
        """
        logger = logging.getLogger(__file__)
        logger.setLevel(level)
        formatter = logging.Formatter("%(message)s", "%y-%m-%d %H:%M:%S")
        streamHandler = logging.StreamHandler()
        if platform.system() != 'Windows':
            streamHandler.emit = _getColorEmit(streamHandler.emit)
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)
        return logger

    def checkMagicNumber(self, header):
        return header[:len(self.MAGIC)] == self.MAGIC

    def readVal(self, fmt, h, start, size):
        newfmt = fmt
        if fmt == 's':
            newfmt = '%ds' % size
        elif fmt == 'numeric':
            newfmt = 'd'
            if size < 8:
                if self.endian == 'little':
                    h = '\x00' * (8 - size) + h
                else:
                    h += '\x00' * (8 - size)
                size = 8
        if self.endian == 'big':
            newfmt = '>%s' % newfmt
        else:
            newfmt = '<%s' % newfmt
        val = struct.unpack(newfmt, h[start:start + size])[0]
        if fmt == 's':
            val = val.strip('\x00')
        return val

    def readColumnAttributes(self, colattr):
        info = []
        Info = namedtuple('ColumnAttributes', ['offset', 'length', 'type'])
        inc = 16 if self.u64 else 12
        for subh in colattr:
            if self.u64:
                attrs = subh.raw[16:16 + ((subh.length - 28) / 16) * 16]
            else:
                attrs = subh.raw[12:12 + ((subh.length - 20) / 12) * 12]
            for i in xrange(0, len(attrs), inc):
                pointer = attrs[i:i + inc]
                if self.u64:
                    offset = self.readVal('q', pointer, 0, 8)
                    length = self.readVal('i', pointer, 8, 4)
                    ctype = self.readVal('b', pointer, 14, 1)
                else:
                    offset = self.readVal('i', pointer, 0, 4)
                    length = self.readVal('i', pointer, 4, 4)
                    ctype = self.readVal('b', pointer, 10, 1)
                assert ctype in (1, 2)
                ctype = 'numeric' if ctype == 1 else 'character'
                info.append(Info(offset, length, ctype))
        return info

    def readColumnNames(self, colname, coltext):
        info = []
        inc = 8 if self.u64 else 4
        for subh in colname:
            if self.u64:
                attrs = subh.raw[16:16 + ((subh.length - 28) / 8) * 8]
            else:
                attrs = subh.raw[12:12 + ((subh.length - 20) / 8) * 8]
            for i in xrange(0, len(attrs), 8):
                pointer = attrs[i:i + 8]
                txt = self.readVal('h', pointer, 0, 2)
                offset = self.readVal('h', pointer, 2, 2) + inc
                length = self.readVal('h', pointer, 4, 2)
                info.append(
                    self.readVal('s', coltext[txt].raw, offset, length)
                )
        return info

    def readColumnLabels(self, collabs, coltext, colcount):
        Info = namedtuple('ColumnLabels', ['format', 'label'])
        if len(collabs) < 1:
            return [Info('', '')] * colcount
        info = []
        inc = 8 if self.u64 else 4
        for subh in collabs:
            base = 46 if self.u64 else 34
            txt = self.readVal('h', subh.raw, base, 2)
            offset = self.readVal('h', subh.raw, base + 2, 2) + inc
            length = self.readVal('h', subh.raw, base + 4, 2)
            fmt = ''
            if length > 0:
                fmt = self.readVal('s', coltext[txt].raw, offset, length)
            base = 52 if self.u64 else 40
            txt = self.readVal('h', subh.raw, base, 2)
            offset = self.readVal('h', subh.raw, base + 2, 2) + inc
            length = self.readVal('h', subh.raw, base + 4, 2)
            label = ''
            if length > 0:
                label = self.readVal('s', coltext[txt].raw, offset, length)
            info.append(Info(fmt, label))
        return info or [Info('', '')] * colcount

    def readPages(self, f, pagecount, pagesize):
        # Read pages
        Page = namedtuple('Page', ['number', 'data', 'type', 'blockcount',
                                   'subheadercount'])
        for i in xrange(pagecount):
            page = f.read(pagesize)
            ptype = self.readVal('h', page, 32 if self.u64 else 16, 2)
            blockcount = 0
            subhcount = 0
            if ptype in self.PAGE_META_MIX_AMD:
                blockcount = self.readVal('h', page, 34 if self.u64 else 18, 2)
                subhcount = self.readVal('h', page, 36 if self.u64 else 20, 2)
            yield Page(i, page, ptype, blockcount, subhcount)

    def readSubheaders(self, f, pagecount, pagesize):
        SubHeader = namedtuple('SubHeader', ['page', 'offset', 'length', 'raw',
                                             'signature', 'compression'])
        oshp = 40 if self.u64 else 24
        lshp = 24 if self.u64 else 12
        lshf = 8 if self.u64 else 4
        dtype = 'q' if self.u64 else 'i'
        for page in self.readPages(f, pagecount, pagesize):
            if page.type not in self.PAGE_META_MIX_AMD:
                continue
            pointers = page.data[oshp:oshp + (page.subheadercount * lshp)]
            for i in xrange(0, len(pointers), lshp):
                pointer = pointers[i:i + lshp]
                offset = self.readVal(dtype, pointer, 0, lshf)
                length = self.readVal(dtype, pointer, lshf, lshf)
                comp = self.readVal('b', pointer, lshf * 2, 1)
                if length > 0:
                    raw = page.data[offset:offset + length]
                    signature = raw[:8 if self.u64 else 4]
                    if comp == 0:
                        comp = None
                    elif comp == 1:
                        comp = 'ignore'
                    elif comp == 4:
                        comp = 'rle'
                    else:
                        self.logger.error('[%s] unknown compression type: %d',
                                          os.path.basename(self.path), comp)
                    yield SubHeader(page.number, offset, length,
                                    raw, signature, comp)

    def _readHeader(self):
        fields = ['headerlength', 'endian', 'platform', 'datecreated',
                  'dataset', 'datemodified', 'pagesize', 'pagecount',
                  'sasrelease', 'sashost', 'osversion', 'osmaker', 'osname',
                  'u64', 'rowcount', 'colcount', 'cols', 'rowcountfp',
                  'rowlength', 'filename', 'compression', 'creator',
                  'creatorproc']
        Info = namedtuple('SAS7BDAT_Header', fields)

        def _repr(self):
            cols = [['Num', 'Name', 'Type', 'Length', 'Format', 'Label']]
            align = ['>', '<', '<', '>', '<', '<']
            colwidth = [len(x) for x in cols[0]]
            for i, col in enumerate(self.cols, 1):
                tmp = [i, col.name, col.attr.type, col.attr.length,
                       col.label.format, col.label.label]
                cols.append(tmp)
                for j, val in enumerate(tmp):
                    colwidth[j] = max(colwidth[j], len(str(val)))
            rows = [' '.join('{0:{1}}'.format(x, colwidth[i])
                    for i, x in enumerate(cols[0]))]
            rows.append(' '.join('-' * colwidth[i]
                                 for i in xrange(len(align))))
            for row in cols[1:]:
                rows.append(' '.join(
                    '{0:{1}{2}}'.format(x, align[i], colwidth[i])
                    for i, x in enumerate(row))
                )
            cols = '\n'.join(rows)
            hdr = 'Header:\n%s' % '\n'.join(
                ['\t%s: %s' % (k, v)
                 for k, v in sorted(self._asdict().iteritems())
                 if v != '' and k not in ('cols', 'rowcountfp', 'rowlength',
                                          'data')]
            )
            return '%s\n\nContents of dataset "%s":\n%s\n' % (
                hdr, self.dataset, cols
            )
        Info.__repr__ = _repr
        Column = namedtuple('Column', ['name', 'attr', 'label'])
        with open(self.path, 'rb') as f:
            # Check magic number
            h = f.read(288)
            if len(h) < 288:
                self.logger.error("[%s] header too short (not a sas7bdat "
                                  "file?)", os.path.basename(self.path))
                return
            if not self.checkMagicNumber(h):
                self.logger.error("[%s] magic number mismatch",
                                  os.path.basename(self.path))
                return
            # Check for 32 or 64 bit alignment
            if h[32] == '\x33':
                align2 = 4
                u64 = True
            else:
                align2 = 0
                u64 = False
            if h[35] == '\x33':
                align1 = 4
            else:
                align1 = 0
            # Check endian
            if h[37] == '\x01':
                endian = 'little'
            else:
                endian = 'big'
            # Check platform
            plat = h[39]
            if plat == '1':
                plat = 'unix'
            elif plat == '2':
                plat = 'windows'
            else:
                plat = 'unknown'
            u64 = u64 and plat == 'unix'
            self.u64 = u64
            self.endian = endian
            name = self.readVal('s', h, 92, 64).lstrip().strip()
            # Timestamp is epoch 01/01/1960
            datecreated = self.readVal('d', h, 164 + align1, 8)
            try:
                datecreated = datetime.strptime('1960/01/01', "%Y/%m/%d") +\
                    timedelta(seconds=datecreated)
            except:
                pass
            datemodified = self.readVal('d', h, 172 + align1, 8)
            try:
                datemodified = datetime.strptime('1960/01/01', "%Y/%m/%d") + \
                    timedelta(seconds=datemodified)
            except:
                pass
            # Read the rest of the header
            hl = self.readVal('i', h, 196 + align1, 4)
            if u64:
                assert hl == 8192
            h += f.read(hl - 288)
            if len(h) != hl:
                self.logger.error('[%s] header too short (not a sas7bdat '
                                  'file?)', os.path.basename(self.path))
                return
            # Get page size
            pagesize = self.readVal('i', h, 200 + align1, 4)
            if pagesize < 0:
                self.logger.error('[%s] page size is negative',
                                  os.path.basename(self.path))
                return
            # Get page count
            if u64:
                pagecount = self.readVal('q', h, 204 + align1, 4 + align2)
            else:
                pagecount = self.readVal('i', h, 204 + align1, 4 + align2)
            if pagecount < 1:
                self.logger.error('[%s] page count is not positive',
                                  os.path.basename(self.path))
                return
            # Get SAS release
            sasrelease = self.readVal('s', h, 216 + align1 + align2, 8)
            # Get SAS host (16 byte field but only first 8 bytes used)
            sashost = self.readVal('s', h, 224 + align1 + align2, 16)
            if sashost not in self.KNOWNHOSTS:
                self.logger.warning('[%s] unknown host: %s',
                                    os.path.basename(self.path),
                                    sashost)
            # Get OS info
            osversion = self.readVal('s', h, 240 + align1 + align2, 16)
            osmaker = self.readVal('s', h, 256 + align1 + align2, 16)
            osname = self.readVal('s', h, 272 + align1 + align2, 16)
            # Read row and column info
            rowsize = []
            colsize = []
            coltext = []
            colattr = []
            colname = []
            collabs = []
            data = []
            for x in self.readSubheaders(f, pagecount, pagesize):
                if x is None:
                    continue
                if x.signature in self.SUBH_ROWSIZE:
                    rowsize.append(x)
                elif x.signature in self.SUBH_COLSIZE:
                    colsize.append(x)
                elif x.signature in self.SUBH_COLTEXT:
                    coltext.append(x)
                elif x.signature in self.SUBH_COLATTR:
                    colattr.append(x)
                elif x.signature in self.SUBH_COLNAME:
                    colname.append(x)
                elif x.signature in self.SUBH_COLLABS:
                    collabs.append(x)
                elif x.signature in self.SUBH_SUBHCNT:
                    pass
                elif x.signature in self.SUBH_COLLIST:
                    pass
                elif x.compression == 'rle':
                    pass
                elif x.compression == 'ignore':
                    pass
                else:
                    pass
            if len(rowsize) != 1:
                self.logger.error('[%s] found %d row size subheaders when '
                                  'expecting 1', os.path.basename(self.path),
                                  len(rowsize))
                return
            rowsize = rowsize[0]
            if u64:
                rowlength = self.readVal('q', rowsize.raw, 40, 8)
                rowcount = self.readVal('q', rowsize.raw, 48, 8)
                colcountp1 = self.readVal('q', rowsize.raw, 72, 8)
                colcountp2 = self.readVal('q', rowsize.raw, 80, 8)
                rowcountfp = self.readVal('q', rowsize.raw, 120, 8)
                lcs = self.readVal('h', rowsize.raw, 682, 2)
                lcp = self.readVal('h', rowsize.raw, 706, 2)
            else:
                rowlength = self.readVal('i', rowsize.raw, 20, 4)
                rowcount = self.readVal('i', rowsize.raw, 24, 4)
                colcountp1 = self.readVal('i', rowsize.raw, 36, 4)
                colcountp2 = self.readVal('i', rowsize.raw, 40, 4)
                rowcountfp = self.readVal('i', rowsize.raw, 60, 4)
                lcs = self.readVal('h', rowsize.raw, 354, 2)
                lcp = self.readVal('h', rowsize.raw, 378, 2)
            if len(colsize) != 1:
                self.logger.error('[%s] found %d column size subheaders when '
                                  'expecting 1', os.path.basename(self.path),
                                  len(colsize))
                return
            colsize = colsize[0]
            if u64:
                colcount = self.readVal('q', colsize.raw, 8, 8)
            else:
                colcount = self.readVal('i', colsize.raw, 4, 4)
            if colcountp1 + colcountp2 != colcount:
                self.logger.warning('[%s] column count mismatch',
                                    os.path.basename(self.path))
            if len(coltext) < 1:
                self.logger.error('[%s] no column text subheaders found',
                                  os.path.basename(self.path))
                return
            creator = ''
            creatorproc = ''
            compression = self.readVal(
                's',
                coltext[0].raw,
                20 if self.u64 else 16,
                8
            ).lstrip().strip()
            if compression == '':
                compression = None
                lcs = 0
                creatorproc = self.readVal('s', coltext[0].raw,
                                           16 + (20 if u64 else 16), lcp)
            elif compression == 'SASYZCRL':
                compression = 'RLE'
                creatorproc = self.readVal('s', coltext[0].raw,
                                           24 + (20 if u64 else 16), lcp)
            elif lcs > 0:
                compression = None
                lcp = 0
                creator = self.readVal(
                    's',
                    coltext[0].raw,
                    20 if u64 else 16,
                    lcs
                ).lstrip().strip()
            else:
                # might be RDC (Ross Data Compression)
                self.logger.error('[%s] Unknown compression type: %s '
                                  '(possibly binary?)',
                                  os.path.basename(self.path), compression)
            if len(colattr) < 1:
                self.logger.error("[%s] no column attribute subheaders found",
                                  os.path.basename(self.path))
                return
            colattr = self.readColumnAttributes(colattr)
            if len(colattr) != colcount:
                self.logger.error('[%s] found %d column attributes when '
                                  'expecting %d', os.path.basename(self.path),
                                  len(colattr), colcount)
                return
            if len(colname) < 1:
                self.logger.error('[%s] no column name subheaders found',
                                  os.path.basename(self.path))
                return
            colname = self.readColumnNames(colname, coltext)
            if len(colname) != colcount:
                self.logger.error('[%s] found %d column names when expecting '
                                  '%d', os.path.basename(self.path),
                                  len(colname), colcount)
            collabs = self.readColumnLabels(collabs, coltext, colcount)
            if len(collabs) != colcount:
                self.logger.error('[%s] found %d column formats and labels '
                                  'when expecting %d',
                                  os.path.basename(self.path), len(collabs),
                                  colcount)
            cols = []
            for i in xrange(colcount):
                cols.append(Column(colname[i], colattr[i], collabs[i]))
        info = Info(hl, endian, plat, datecreated, name, datemodified,
                    pagesize, pagecount, sasrelease, sashost, osversion,
                    osmaker, osname, u64, rowcount, colcount, cols, rowcountfp,
                    rowlength, os.path.basename(self.path), compression,
                    creator, creatorproc)
        return info

    def uncompressData(self, data):
        result = []
        s = StringIO(data)
        while True:
            d = s.read(1)
            if len(d) < 1:
                break
            d = '%02X' % ord(d)
            command = d[0].upper()
            length = d[1]
            if command == '0':
                length = int(d, 16)
                result.append(s.read(length + 64))
            elif command == '1':
                # length = int(d, 16)
                # result.append(s.read(length + 21))  # ?
                pass
            elif command == '2':
                # length = int(d, 16)
                # result.append(s.read(length * 5))  # ?
                pass
            elif command == '3':
                # import pdb; pdb.set_trace()
                pass
            elif command == '4':
                pass
            elif command == '5':
                pass
            elif command == '6':
                length = int(d, 16)
                result.append('\x20' * (length + 17))
            elif command == '7':
                length = int(d, 16)
                result.append('\x00' * (length + 17))
            elif command == '8':
                length = int(length, 16)
                result.append(s.read(length + 1))
            elif command == '9':
                length = int(length, 16)
                result.append(s.read(length + 17))
            elif command == 'A':
                length = int(length, 16)
                result.append(s.read(length + 33))
            elif command == 'B':
                length = int(length, 16)
                result.append(s.read(length + 49))
            elif command == 'C':
                length = int(length, 16)
                result.append(s.read(1) * (length + 3))
            elif command == 'D':
                length = int(length, 16)
                result.append('\x40' * (length + 2))
            elif command == 'E':
                length = int(length, 16)
                result.append('\x20' * (length + 2))
            elif command == 'F':
                length = int(length, 16)
                result.append('\x00' * (length + 2))
        return ''.join(result)

    def readData(self):
        if self.header.compression is not None:
            self.logger.error('[%s] compressed data not yet supported',
                              os.path.basename(self.path))
        yield [x.name for x in self.header.cols]
        with open(self.path, 'rb') as f:
            f.seek(self.header.headerlength)
            for page in self.readPages(f, self.header.pagecount,
                                       self.header.pagesize):
                if page.type not in self.PAGE_MIX_DATA and not\
                        (page.type == self.PAGE_META and
                         self.header.compression == 'RLE'):
                    continue
                page = page._asdict()
                if page['type'] == self.PAGE_META:
                    page['data'] = self.uncompressData(page['data'])
                    rowcountp = self.header.rowcountfp
                    base = 129 + page['subheadercount'] * 24
                elif self.u64:
                    if page['type'] in self.PAGE_MIX:
                        rowcountp = self.header.rowcountfp
                        base = 40 + page['subheadercount'] * 24
                        base += (base % 8)
                    else:
                        rowcountp = self.readVal('h', page['data'], 34, 2)
                        base = 40
                else:
                    if page['type'] in self.PAGE_MIX:
                        rowcountp = self.header.rowcountfp
                        base = 24 + page['subheadercount'] * 12
                        base += (base % 8)
                    else:
                        rowcountp = self.readVal('h', page['data'], 18, 2)
                        base = 24
                if rowcountp > self.header.rowcount:
                    rowcountp = self.header.rowcount
                for _ in xrange(rowcountp):
                    row = []
                    for col in self.header.cols:
                        offset = base + col.attr.offset
                        if col.attr.length > 0:
                            # import pdb; pdb.set_trace()
                            raw = page['data'][offset:offset + col.attr.length]
                            try:
                                if col.attr.type == 'character':
                                    val = self.readVal('s', raw, 0,
                                                       col.attr.length)
                                    val = val.lstrip().strip()
                                else:
                                    val = self.readVal(col.attr.type, raw, 0,
                                                       col.attr.length)
                            except:
                                break
                            row.append(val)
                    base += self.header.rowlength
                    if row:
                        yield row

    def convertFile(self, outFile, delimiter=',', stepSize=100000):
        self.logger.debug("Input: %s\nOutput: %s", self.path, outFile)
        outF = None
        try:
            if outFile == '-':
                outF = sys.stdout
            else:
                outF = open(outFile, 'w')
            out = csv.writer(outF,
                             lineterminator='\n',
                             delimiter=delimiter)
            i = 0
            for i, line in enumerate(self.readData(), 1):
                if not line:
                    i -= 1
                    continue
                if not i % stepSize:
                    self.logger.info('%.1f%% complete',
                                     float(i) / self.header.rowcount * 100.0)
                try:
                    out.writerow(line)
                except IOError:
                    self.logger.warn('Wrote %d lines before interruption', i)
                    break
            self.logger.info('[%s] wrote %d of %d lines',
                             os.path.basename(outFile), i - 1,
                             self.header.rowcount)
        finally:
            if outF is not None:
                outF.close()


if __name__ == '__main__':
    pass  # TODO: write some unit tests
