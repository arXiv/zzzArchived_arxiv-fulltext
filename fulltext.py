import os
import re
import sys
import shlex
import unicodedata

from time import ctime
from subprocess import check_output, CalledProcessError, TimeoutExpired

TIMELIMIT = 10*60

PDF2TXT = 'pdf2txt'
PDFTOTEXT = 'pdftotext'

RE_STAMP = r'(arXiv:.{20,60}\s\d{1,2}\s[A-Z][a-z]{2}\s\d{4})'
RE_REPEATS = r'(\(cid:\d+\)|lllll|\.\.\.\.\.|\*\*\*\*\*)'

# ============================================================================
#  elements for fixing the extracted text
# ============================================================================
def fixunicode(txt):
    output = ''
    stamp = ''
    look_for_stamp = True

    for i, line in enumerate(txt.split('\n')):
        if look_for_stamp:
            if i == 1 and re.findall(RE_STAMP, line):
                look_for_stamp = False
            elif i < 60 and len(line) < 3:
                stamp += line.strip()
                
                if ':viXra' in stamp:
                    look_for_stamp = False
            else:
                look_for_stamp = False

        output += '{}\n'.format(utf8_dumbdown(line))

    return unicodedata.normalize('NFKC', output)


def average_word_length(txt):
    txt = re.subn(RE_REPEATS, '', txt)[0]
    nw = len(txt.split())
    nc = len(txt)
    avgw = nc / (nw + 1)
    return nc, nw, avgw


# ============================================================================
#  functions for calling the text extraction services
# ============================================================================
def run_pdf2txt(pdffile, timelimit=TIMELIMIT, options=''):
    name, ext = os.path.splitext(pdffile)
    tmpfile = '{}.pdf2txt'.format(name)

    cmd = '{cmd} {options} -o {output} {pdf}'.format(
        cmd=PDF2TXT, options=options, output=tmpfile, pdf=pdffile
    )
    cmd = shlex.split(cmd)
    output = check_output(cmd, timeout=timelimit)

    with open(tmpfile) as f:
        return f.read()


def run_pdftotext(pdffile, timelimit=TIMELIMIT):
    name, ext = os.path.splitext(pdffile)
    tmpfile = '{}.pdftotxt'.format(name)

    cmd = '{cmd} {pdf} {output}'.format(
        cmd=PDFTOTEXT, pdf=pdffile, output=tmpfile
    )
    cmd = shlex.split(cmd)
    output = check_output(cmd, timeout=timelimit)

    with open(tmpfile) as f:
        return f.read()


def run_pdf2txt_A(pdffile, **kwargs):
    return run_pdf2txt(pdffile, options='-A', **kwargs)


# ============================================================================
#  elements for fixing the extracted text
# ============================================================================
def fulltext(pdffile, timelimit=600):
    if not os.path.isfile(pdffile):
        raise FileNotFoundError(pdffile)

    try:
        output = run_pdf2txt(pdffile, timelimit=timelimit)
    except (TimeoutExpired, CalledProcessError) as e:
        output = run_pdftotext(pdffile, timelimit=None)

    output = fix_unicode(output)
    stats = average_word_length(output)

    if stats[2] <= 45:
        return output, stats

    output = run_pdf2txt_A(pdffile, timelimit=timelimit)
    output = fix_unicode(output)
    stats = average_word_length(output)

    if stats[2] > 45:
        return '', stats

    return output, stats
