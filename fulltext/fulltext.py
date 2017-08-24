import os
import re
import shlex

from subprocess import check_output, CalledProcessError, TimeoutExpired

import logging
import fixunicode

log = logging.getLogger('fulltext')
TIMELIMIT = 10*60

PDF2TXT = 'pdf2txt'
PDFTOTEXT = 'pdftotext'

RE_STAMP = r'(arXiv:.{20,60}\s\d{1,2}\s[A-Z][a-z]{2}\s\d{4})'
RE_REPEATS = r'(\(cid:\d+\)|lllll|\.\.\.\.\.|\*\*\*\*\*)'


def average_word_length(txt):
    """
    Gather statistics about the text, primarily the average word length

    Parameters
    ----------
    txt : str

    Returns
    -------
    word_length : float
        Average word length in the text
    """
    txt = re.subn(RE_REPEATS, '', txt)[0]
    nw = len(txt.split())
    nc = len(txt)
    avgw = nc / (nw + 1)
    return avgw


# ============================================================================
#  functions for calling the text extraction services
# ============================================================================
def run_pdf2txt(pdffile: str, timelimit: int=TIMELIMIT, options: str=''):
    """
    Run pdf2txt to extract full text

    Parameters
    ----------
    pdffile : str
        Path to PDF file

    timelimit : int
        Amount of time to wait for the process to complete

    Returns
    -------
    output : str
        Full plain text output
    """
    name, ext = os.path.splitext(pdffile)
    tmpfile = '{}.pdf2txt'.format(name)

    cmd = '{cmd} {options} -o {output} {pdf}'.format(
        cmd=PDF2TXT, options=options, output=tmpfile, pdf=pdffile
    )
    cmd = shlex.split(cmd)
    output = check_output(cmd, timeout=timelimit)
    log.info(output)

    with open(tmpfile) as f:
        return f.read()


def run_pdftotext(pdffile: str, timelimit: int=TIMELIMIT) -> str:
    """
    Run pdftotext on PDF file for extracted plain text

    Parameters
    ----------
    pdffile : str
        Path to PDF file

    timelimit : int
        Amount of time to wait for the process to complete

    Returns
    -------
    output : str
        Full plain text output
    """
    name, ext = os.path.splitext(pdffile)
    tmpfile = '{}.pdftotxt'.format(name)

    cmd = '{cmd} {pdf} {output}'.format(
        cmd=PDFTOTEXT, pdf=pdffile, output=tmpfile
    )
    cmd = shlex.split(cmd)
    output = check_output(cmd, timeout=timelimit)
    log.info(output)

    with open(tmpfile) as f:
        return f.read()


def run_pdf2txt_A(pdffile: str, **kwargs) -> str:
    """
    Run pdf2txt with the -A option which runs 'positional analysis on images'
    and can return better results when pdf2txt combines many words together.

    Parameters
    ----------
    pdffile : str
        Path to PDF file

    kwargs : dict
        Keyword arguments to :func:`run_pdf2txt`

    Returns
    -------
    output : str
        Full plain text output
    """
    return run_pdf2txt(pdffile, options='-A', **kwargs)


# ============================================================================
#  main function which extracts text
# ============================================================================
def fulltext(pdffile: str, timelimit: int=TIMELIMIT):
    """
    Given a pdf file, extract the unicode text and run through very basic
    unicode normalization routines. Determine the best extracted text and
    return as a string.

    Parameters
    ----------
    pdffile : str
        Path to PDF file from which to extract text

    timelimit : int
        Time in seconds to allow the extraction routines to run

    Returns
    -------
    fulltext : str
        The full plain text of the PDF
    """
    if not os.path.isfile(pdffile):
        raise FileNotFoundError(pdffile)

    try:
        output = run_pdf2txt(pdffile, timelimit=timelimit)
    except (TimeoutExpired, CalledProcessError) as e:
        output = run_pdftotext(pdffile, timelimit=None)

    output = fixunicode.fix_unicode(output)
    wordlength = average_word_length(output)

    if wordlength <= 45:
        return output

    output = run_pdf2txt_A(pdffile, timelimit=timelimit)
    output = fixunicode.fix_unicode(output)
    wordlength = average_word_length(output)

    if wordlength > 45:
        raise RuntimeError(
            'No accurate text could be extracted from "{}"'.format(pdffile)
        )

    return output
