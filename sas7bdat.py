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
from datetime import datetime, timedelta
from collections import namedtuple

MAGIC = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc2\xea\x81\x60" \
        "\xb3\x14\x11\xcf\xbd\x92\x08\x00\x09\xc7\x31\x8c\x18\x1f\x10\x11"

# Host systems known to work
KNOWNHOSTS = set(["WIN_PRO", "WIN_NT", "WIN_NTSV", "WIN_SRV", "WIN_ASRV",
                  "XP_PRO", "XP_HOME", "NET_ASRV", "NET_DSRV", "NET_SRV",
                  "WIN_98", "W32_VSPR", "WIN", "WIN_95", "X64_VSPR", "AIX",
                  "X64_ESRV", "W32_ESRV", "W32_7PRO", "W32_VSHO", "X64_7HOM",
                  "X64_7PRO", "X64_SRV0", "W32_SRV0", "X64_ES08", "Linux"])

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
SUBH_COLNAME = set(["\xFF\xFF\xFF\xFF", "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"])
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
PAGE_COMP = -28672     # 1 << 16 | 1 << 13
PAGE_MIX_DATA = PAGE_MIX + [PAGE_DATA]
PAGE_META_MIX_AMD = [PAGE_META] + PAGE_MIX + [PAGE_AMD]
PAGE_ANY = PAGE_META_MIX_AMD + [PAGE_DATA]


def _debug():
    if hasattr(sys, 'ps1') or not sys.stderr.isatty():
        sys.__excepthook__(type, value, tb)
    else:
        import pdb
        import traceback
        traceback.print_exception(type, value, tb)
        print
        pdb.pm()
        os._exit(1)

sys.excepthook = _debug


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


def makeLogger(level=logging.INFO):
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


def checkMagicNumber(header):
    return header[:len(MAGIC)] == MAGIC


def readVal(fmt, h, start, size, endian):
    newfmt = fmt
    if fmt == 's':
        newfmt = '%ds' % size
    elif fmt == 'numeric':
        newfmt = 'd'
        if size < 8:
            if endian == 'little':
                h = '\x00' * (8 - size) + h
            else:
                h += '\x00' * (8 - size)
            size = 8
    if endian == 'big':
        newfmt = '>%s' % newfmt
    else:
        newfmt = '<%s' % newfmt
    val = struct.unpack(newfmt, h[start:start + size])[0]
    if fmt == 's':
        val = val.strip('\x00')
    return val


def readColumnAttributes(colattr, u64, endian):
    info = []
    Info = namedtuple('ColumnAttributes', ['offset', 'length', 'type'])
    inc = 16 if u64 else 12
    for subh in colattr:
        if u64:
            attrs = subh.raw[16:16 + ((subh.length - 28) / 16) * 16]
        else:
            attrs = subh.raw[12:12 + ((subh.length - 20) / 12) * 12]
        for i in xrange(0, len(attrs), inc):
            pointer = attrs[i:i + inc]
            if u64:
                offset = readVal('q', pointer, 0, 8, endian)
                length = readVal('i', pointer, 8, 4, endian)
                ctype = readVal('b', pointer, 14, 1, endian)
            else:
                offset = readVal('i', pointer, 0, 4, endian)
                length = readVal('i', pointer, 4, 4, endian)
                ctype = readVal('b', pointer, 10, 1, endian)
            assert ctype in (1, 2)
            ctype = 'numeric' if ctype == 1 else 'character'
            info.append(Info(offset, length, ctype))
    return info


def readColumnNames(colname, coltext, u64, endian):
    info = []
    inc = 8 if u64 else 4
    for subh in colname:
        if u64:
            attrs = subh.raw[16:16 + ((subh.length - 28) / 8) * 8]
        else:
            attrs = subh.raw[12:12 + ((subh.length - 20) / 8) * 8]
        for i in xrange(0, len(attrs), 8):
            pointer = attrs[i:i + 8]
            txt = readVal('h', pointer, 0, 2, endian)
            offset = readVal('h', pointer, 2, 2, endian) + inc
            length = readVal('h', pointer, 4, 2, endian)
            info.append(readVal('s', coltext[txt].raw, offset, length,
                                endian))
    return info


def readColumnLabels(collabs, coltext, u64, endian, colcount):
    Info = namedtuple('ColumnLabels', ['format', 'label'])
    if len(collabs) < 1:
        return [Info('', '')] * colcount
    info = []
    inc = 8 if u64 else 4
    for subh in collabs:
        base = 46 if u64 else 34
        txt = readVal('h', subh.raw, base, 2, endian)
        offset = readVal('h', subh.raw, base + 2, 2, endian) + inc
        length = readVal('h', subh.raw, base + 4, 2, endian)
        fmt = ''
        if length > 0:
            fmt = readVal('s', coltext[txt].raw, offset, length, endian)
        base = 52 if u64 else 40
        txt = readVal('h', subh.raw, base, 2, endian)
        offset = readVal('h', subh.raw, base + 2, 2, endian) + inc
        length = readVal('h', subh.raw, base + 4, 2, endian)
        label = ''
        if length > 0:
            label = readVal('s', coltext[txt].raw, offset, length, endian)
        info.append(Info(fmt, label))
    return info or [Info('', '')] * colcount


def readPages(f, pagecount, pagesize, u64, endian):
    # Read pages
    Page = namedtuple('Page', ['number', 'data', 'type', 'subheadercount'])
    for i in xrange(pagecount):
        page = f.read(pagesize)
        ptype = readVal('h', page, 32 if u64 else 16, 2, endian)
        subhcount = 0
        if ptype in PAGE_META_MIX_AMD:
            subhcount = readVal('h', page, 36 if u64 else 20, 2, endian)
        yield Page(i, page, ptype, subhcount)


def readHeader(inFile, logger):
    fields = ['headerlength', 'endian', 'platform', 'datecreated',
              'dataset', 'datemodified', 'pagesize', 'pagecount', 'sasrelease',
              'sashost', 'osversion', 'osmaker', 'osname', 'u64', 'rowcount',
              'colcount', 'cols', 'rowcountfp', 'rowlength', 'filename',
              'compression', 'creator', 'creatorproc']
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
        rows.append(' '.join('-' * colwidth[i] for i in xrange(len(align))))
        for row in cols[1:]:
            rows.append(' '.join('{0:{1}{2}}'.format(x, align[i], colwidth[i])
                                 for i, x in enumerate(row)))
        cols = '\n'.join(rows)
        hdr = 'Header:\n%s' % '\n'.join(
            ['\t%s: %s' % (k, v) for k, v in sorted(self._asdict().iteritems())
             if v != '' and k not in ('cols', 'rowcountfp', 'rowlength')]
        )
        return '%s\n\nContents of dataset "%s":\n%s\n' % (hdr, self.dataset,
                                                          cols)
    Info.__repr__ = _repr
    Column = namedtuple('Column', ['name', 'attr', 'label'])
    with open(inFile, 'rb') as f:
        # Check magic number
        h = f.read(288)
        if len(h) < 288:
            logger.error("[%s] header too short (not a sas7bdat file?)",
                         os.path.basename(inFile))
            return
        if not checkMagicNumber(h):
            logger.error("[%s] magic number mismatch",
                         os.path.basename(inFile))
            return
        # Check for 32 or 64 bit alignment
        if h[32] == '\x33':
            align1 = 4
            u64 = True
        else:
            align1 = 0
            u64 = False
        if h[35] == '\x33':
            align2 = 4
        else:
            align2 = 0
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
        name = readVal('s', h, 92, 64, endian).lstrip().strip()
        # Timestamp is epoch 01/01/1960
        datecreated = readVal('d', h, 164 + align1, 8, endian)
        try:
            datecreated = datetime.strptime('1960/01/01', "%Y/%m/%d") +\
                timedelta(seconds=datecreated)
        except:
            pass
        datemodified = readVal('d', h, 172 + align1, 8, endian)
        try:
            datemodified = datetime.strptime('1960/01/01', "%Y/%m/%d") + \
                timedelta(seconds=datemodified)
        except:
            pass
        # Read the rest of the header
        hl = readVal('i', h, 196 + align2, 4, endian)
        if u64:
            assert hl == 8192
        h += f.read(hl - 288)
        if len(h) != hl:
            logger.error('[%s] header too short (not a sas7bdat file?)',
                         os.path.basename(inFile))
            return
        # Get page size
        pagesize = readVal('i', h, 200 + align2, 4, endian)
        if pagesize < 0:
            logger.error('[%s] page size is negative',
                         os.path.basename(inFile))
            return
        # Get page count
        if u64:
            pagecount = readVal('q', h, 204 + align2, 8, endian)
        else:
            pagecount = readVal('i', h, 204 + align2, 4, endian)
        if pagecount < 1:
            logger.error('[%s] page count is not positive',
                         os.path.basename(inFile))
            return
        # Get SAS release
        sasrelease = readVal('s', h, 216 + align1 + align2, 8, endian)
        # Get SAS host (16 byte field but only first 8 bytes used)
        sashost = readVal('s', h, 224 + align1 + align2, 8, endian)
        if sashost not in KNOWNHOSTS:
            logger.error('[%s] unknown host: %s', os.path.basename(inFile),
                         sashost)
            return
        # Get OS info
        osversion = readVal('s', h, 240 + align1 + align2, 16, endian)
        osmaker = readVal('s', h, 256 + align1 + align2, 16, endian)
        osname = readVal('s', h, 272 + align1 + align2, 16, endian)
        # Read subheaders
        SubHeader = namedtuple('SubHeader', ['page', 'offset', 'length', 'raw',
                                             'signature', 'compression'])
        subheaders = []
        for page in readPages(f, pagecount, pagesize, u64, endian):
            if page.type not in PAGE_META_MIX_AMD:
                continue
            inc = 24 if u64 else 12
            if u64:
                pointers = page.data[40:40 + (page.subheadercount * inc)]
            else:
                pointers = page.data[24:24 + (page.subheadercount * inc)]
            for i in xrange(0, len(pointers), inc):
                pointer = pointers[i:i + inc]
                if u64:
                    offset = readVal('q', pointer, 0, 8, endian)
                    length = readVal('q', pointer, 8, 8, endian)
                    comp = readVal('b', pointer, 16, 1, endian)
                else:
                    offset = readVal('i', pointer, 0, 4, endian)
                    length = readVal('i', pointer, 4, 4, endian)
                    comp = readVal('b', pointer, 8, 1, endian)
                if length > 0:
                    raw = page.data[offset:offset + length]
                    signature = raw[:8 if u64 else 4]
                    if comp == 0:
                        comp = None
                    elif comp == 1:
                        comp = 'ignore'
                    elif comp == 4:
                        comp = 'rle'
                    else:
                        logger.error('[%s] unknown compression type: %d',
                                     os.path.basename(inFile), comp)
                    subheaders.append(SubHeader(page.number, offset, length,
                                                raw, signature, comp))
        # Read row and column info
        rowsize = []
        colsize = []
        coltext = []
        colattr = []
        colname = []
        collabs = []
        for x in subheaders:
            if x is None:
                continue
            if x.signature in SUBH_ROWSIZE:
                rowsize.append(x)
            elif x.signature in SUBH_COLSIZE:
                colsize.append(x)
            elif x.signature in SUBH_COLTEXT:
                coltext.append(x)
            elif x.signature in SUBH_COLATTR:
                colattr.append(x)
            elif x.signature in SUBH_COLNAME:
                colname.append(x)
            elif x.signature in SUBH_COLLABS:
                collabs.append(x)
        if len(rowsize) != 1:
            logger.error('[%s] found %d row size subheaders when expecting 1',
                         os.path.basename(inFile), len(rowsize))
            return
        rowsize = rowsize[0]
        if u64:
            rowlength = readVal('q', rowsize.raw, 40, 8, endian)
            rowcount = readVal('q', rowsize.raw, 48, 8, endian)
            colcountp1 = readVal('q', rowsize.raw, 72, 8, endian)
            colcountp2 = readVal('q', rowsize.raw, 80, 8, endian)
            rowcountfp = readVal('q', rowsize.raw, 120, 8, endian)
            lcs = readVal('h', rowsize.raw, 682, 2, endian)
            lcp = readVal('h', rowsize.raw, 706, 2, endian)
        else:
            rowlength = readVal('i', rowsize.raw, 20, 4, endian)
            rowcount = readVal('i', rowsize.raw, 24, 4, endian)
            colcountp1 = readVal('i', rowsize.raw, 36, 4, endian)
            colcountp2 = readVal('i', rowsize.raw, 40, 4, endian)
            rowcountfp = readVal('i', rowsize.raw, 60, 4, endian)
            lcs = readVal('h', rowsize.raw, 354, 2, endian)
            lcp = readVal('h', rowsize.raw, 378, 2, endian)
        if len(colsize) != 1:
            logger.error('[%s] found %d column size subheaders when '
                         'expecting 1', os.path.basename(inFile),
                         len(colsize))
            return
        colsize = colsize[0]
        if u64:
            colcount = readVal('q', colsize.raw, 8, 8, endian)
        else:
            colcount = readVal('i', colsize.raw, 4, 4, endian)
        if colcountp1 + colcountp2 != colcount:
            logger.warning('[%s] column count mismatch',
                           os.path.basename(inFile))
        if len(coltext) < 1:
            logger.error('[%s] no column text subheaders found',
                         os.path.basename(inFile))
            return
        creator = ''
        creatorproc = ''
        compression = readVal('s', coltext[0].raw, 20 if u64 else 16, 8,
                              endian).lstrip().strip()
        if compression == '':
            compression = None
            lcs = 0
            creatorproc = readVal('s', coltext[0].raw,
                                  16 + (20 if u64 else 16), lcp, endian)
        elif compression == 'SASYZCRL':
            compression = 'RLE'
            creatorproc = readVal('s', coltext[0].raw,
                                  24 + (20 if u64 else 16), lcp, endian)
        elif lcs > 0:
            compression = None
            lcp = 0
            creator = readVal('s', coltext[0].raw, 20 if u64 else 16, lcs,
                              endian).lstrip().strip()
        else:
            logger.error('[%s] Unknown compression type: %s '
                         '(possibly binary?)', os.path.basename(inFile),
                         compression)
        if len(colattr) < 1:
            logger.error("[%s] no column attribute subheaders found",
                         os.path.basename(inFile))
            return
        colattr = readColumnAttributes(colattr, u64, endian)
        if len(colattr) != colcount:
            logger.error('[%s] found %d column attributes when expecting %d',
                         os.path.basename(inFile), len(colattr), colcount)
            return
        if len(colname) < 1:
            logger.error('[%s] no column name subheaders found',
                         os.path.basename(inFile))
            return
        colname = readColumnNames(colname, coltext, u64, endian)
        if len(colname) != colcount:
            logger.error('[%s] found %d column names when expecting %d',
                         os.path.basename(inFile), len(colname), colcount)
        collabs = readColumnLabels(collabs, coltext, u64, endian, colcount)
        if len(collabs) != colcount:
            logger.error('[%s] found %d column formats and labels when '
                         'expecting %d', os.path.basename(inFile),
                         len(collabs), colcount)
        cols = []
        for i in xrange(colcount):
            cols.append(Column(colname[i], colattr[i], collabs[i]))
    info = Info(hl, endian, plat, datecreated, name, datemodified, pagesize,
                pagecount, sasrelease, sashost, osversion, osmaker, osname,
                u64, rowcount, colcount, cols, rowcountfp, rowlength,
                os.path.basename(inFile), compression, creator, creatorproc)
    return info


def readData(inFile, header, logger):
    if header.compression is not None:
        logger.error('[%s] compressed data not yet supported',
                     os.path.basename(inFile))
    yield [x.name for x in header.cols]
    with open(inFile, 'rb') as f:
        f.seek(header.headerlength)
        for page in readPages(f, header.pagecount, header.pagesize, header.u64,
                              header.endian):
            if page.type not in PAGE_MIX_DATA:
                continue
            if header.u64:
                logger.error('[%s] 64-bit files not yet implemented',
                             os.path.basename(inFile))
                yield None
                continue
            else:
                if page.type in PAGE_MIX:
                    rowcountp = header.rowcountfp
                    base = 24 + page.subheadercount * 12
                    base = base + base % 8
                else:
                    rowcountp = readVal('h', page.data, 18, 2,
                                        header.endian)
                    base = 24
            if rowcountp > header.rowcount:
                rowcountp = header.rowcount
            for _ in xrange(rowcountp):
                row = []
                for col in header.cols:
                    offset = base + col.attr.offset
                    if col.attr.length > 0:
                        raw = page.data[offset:offset + col.attr.length]
                        try:
                            if col.attr.type == 'character':
                                val = readVal('s', raw, 0, col.attr.length,
                                              header.endian)
                                val = val.lstrip().strip()
                            else:
                                val = readVal(col.attr.type, raw, 0,
                                              col.attr.length, header.endian)
                        except:
                            break
                        row.append(val)
                base += header.rowlength
                if row:
                    yield row


def convertFile(inFile, outFile, header, logger, delimiter=',',
                stepSize=100000):
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
        for i, line in enumerate(readData(inFile, header, logger), 1):
            if not line:
                i -= 1
                continue
            if not i % stepSize:
                logger.info('%.1f%% complete',
                            float(i) / header.rowcount * 100.0)
            try:
                out.writerow(line)
            except IOError:
                logger.warn('Wrote %d lines before interruption', i)
                break
        logger.info('[%s] wrote %d of %d lines', os.path.basename(outFile),
                    i - 1, header.rowcount)
    finally:
        if outF is not None:
            outF.close()


if __name__ == '__main__':
    pass  # TODO: write some unit tests

