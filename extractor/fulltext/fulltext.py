import os
import re
import glob
import shlex

from subprocess import check_output, CalledProcessError, TimeoutExpired

from arxiv.base import logging
import fixunicode

log = logging.getLogger('fulltext')
TIMELIMIT = 10*60

PDF2TXT = 'pdf2txt.py'
PDFTOTEXT = 'pdftotext'

RE_STAMP = r'(arXiv:.{20,60}\s\d{1,2}\s[A-Z][a-z]{2}\s\d{4})'
RE_REPEATS = r'(\(cid:\d+\)|lllll|\.\.\.\.\.|\*\*\*\*\*)'


def reextension(filename: str, extension: str) -> str:
    """ Give a filename a new extension """
    name, _ = os.path.splitext(filename)
    return '{}.{}'.format(name, extension)


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
    log.debug('Running {} on {}'.format(PDF2TXT, pdffile))
    tmpfile = reextension(pdffile, 'pdf2txt')

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
    log.debug('Running {} on {}'.format(PDFTOTEXT, pdffile))
    tmpfile = reextension(pdffile, 'pdftotxt')

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


def sorted_files(globber: str):
    """
    Give a globbing expression of files to find. They will be sorted upon
    return.  This function is most useful when sorting does not provide
    numerical order,

    e.g.:
        9 -> 12 returned as 10 11 12 9 by string sort

    In this case use num_sort=True, and it will be sorted by numbers in the
    string, then by the string itself.

    Parameters
    ----------
    globber : str
        Expression on which to search for files (bash glob expression)


    """
    files = glob.glob(globber)
    files.sort()

    allfiles = []

    for fn in files:
        nums = re.findall(r'\d+', fn)
        data = [int(n) for n in nums] + [fn]
        allfiles.append(data)

    allfiles = sorted(allfiles)
    return [f[-1] for f in allfiles]


def convert_directory(path):
    """
    Convert all pdfs in a given `path` to full plain text. For each pdf, a file
    of the same name but extension .txt will be created. If that file exists,
    it will be skipped.

    Parameters
    ----------
    path : str
        Directory in which to search for pdfs and convert to text

    Returns
    -------
    output : list of str
        List of converted files
    """
    outlist = []

    globber = os.path.join(path, '*.pdf')
    pdffiles = sorted_files(globber)

    log.info('Searching "{}"...'.format(globber))
    log.info('Found: {}'.format(pdffiles))

    for pdffile in pdffiles:
        txtfile = reextension(pdffile, 'txt')

        if os.path.exists(txtfile):
            continue

        # we don't want this function to stop half way because of one failed
        # file so just charge onto the next one
        try:
            text = fulltext(pdffile)
            with open(txtfile, 'w') as f:
                f.write(text)
        except Exception as e:
            log.error("Conversion failed for '{}'".format(pdffile))
            log.exception(e)
            continue

        outlist.append(pdffile)
    return outlist


def convert(path: str) -> str:
    """
    Convert a single PDF to text.

    Parameters
    ----------
    path : str
        Location of a PDF file.

    Returns
    -------
    str
        Location of text file.
    """
    if not os.path.exists(path):
        raise RuntimeError('No such path: %s' % path)
    outpath = reextension(path, 'txt')
    try:
        content = fulltext(path)
        with open(outpath, 'w') as f:
            f.write(content)
    except Exception as e:
        msg = "Conversion failed for '%s': %s"
        log.error(msg, path, e)
        raise RuntimeError(msg % (path, e)) from e
    return outpath
