"""
Utilities to convert text extracted from a PDF (usually from pdf2txt) to clean
format in which only ascii characters are left and sentences are separated by
new lines (PSV). This utility was converted from arXiv::Overlap::TidyText in
Perl, retrieved 2012-05-06 [CVS: v1.3 2011/08/15 14:55:33 arxiv Exp].

This should be replaced by a new fulltext mechanism which provides an
alternative text extraction method available more broadly than just in docsim.
"""
import re
from typing import List, Tuple


def normalize_text_psv(txt: str) -> str:
    """
    Normalize text using the conversion to psv.

    Return the psv part of ``process_text`` ignoring the references.

    Parameters
    ----------
    txt : string
        The raw text to transform into 'simpler' form.

    Returns
    -------
    psv : string
        Normalized text, extracting newlines
    """
    psv, ref = process_text(txt)
    return psv.replace('\n', ' ')


def process_text(txt: str) -> Tuple[str, str]:
    """
    Convert a single string to a list of lines giving the PSV and references.

    Parameters
    ----------
    txt : string
        The full text of an article, typically as extracted from PDF

    Returns
    -------
    psv : string
        The extracted PSV as a single string object

    ref : string
        The cleaned reference section with lines separated by newline
    """
    txt = _recover_accents(txt)

    # rest of code expects \n terminated lines (^J, ^K, ^L, ^M)
    lines = [l+'\n' for l in re.split(r'[\x0a-\x0d]+', txt)]

    psv, ref = split_on_references(lines)
    psv = '\n'.join(tidy_txt_from_pdf(psv))
    ref = '\n'.join(tidy_txt_from_pdf(ref))
    return psv, ref


def tidy_txt_from_pdf(lines: List[str]) -> List[str]:
    """
    Clean up a text extracted from a PDF.

    Removes: keywords, whitespace, symbols, numbers, abbreviations, non-ascii
    and better separation of text into sentences.

    Parameters
    ----------
    lines : list of strings
        List of lines of text to clean

    Returns
    -------
    lines : list of strings
        Cleaned group of strings
    """
    lines = _remove_Keyword(lines)
    lines = _remove_WhiteSpace(lines)
    lines = _remove_BadEOL(lines)

    # Remove the following per sentence
    for i in range(len(lines)):
        lines[i] = expandWords(lines[i])
        lines[i] = _remove_Symbols(lines[i])
        lines[i] = _remove_Numbers(lines[i])
        lines[i] = _remove_Abbrev(lines[i])
        lines[i] = _remove_SingleAlphabet(lines[i])
        lines[i] = _remove_ExtraSpaces(lines[i])

    lines = _remove_WhiteSpace(lines)
    lines = _remove_BadEOL(lines)

    lines = _split_sentence(lines)
    lines = _clean_sentence(lines)

    return lines


def _remove_WhiteSpace(lines: List[str]) -> List[str]:
    """Change white spaces, including eols, to spaces."""
    out = []
    for line in lines:
        out.append(re.subn(r'[\n\r\f\t]', ' ', line)[0])
    return out


def _remove_BadEOL(lines: List[str]) -> List[str]:
    """Remove eols in the middle of sentence."""
    out = ['']
    prevline = ''

    for line in lines:
        line = re.sub(r'- $', '', line)

        if re.match(r'^[a-z]', line) and not re.match(r'\. $', prevline):
            out.append(out.pop() + line)
        else:
            out.append(line)
        prevline = line
    return out


def _remove_Keyword(lines: List[str]) -> List[str]:
    """Remove sentences with the following keywords."""
    out = []
    prevline = ''
    saveline = ''

    for line in lines:
        prevline = saveline
        saveline = line

        if line.lower().startswith('arxiv'):
            continue
        if 'will be inserted by hand later' in line:
            continue
        if 'was prepared with the aas' in line:
            continue
        if (re.match(r'^\d+$', prevline) and
                re.match(r'university|institute', line, flags=re.IGNORECASE)):
            continue

        out.append(line)
    return out


def expandWords(line: str) -> str:
    """
    Expand common abbreviations into full words.

    Convert Fig., Eq. Eqs., Sect into Figure, Equations, .. keywords
    In addition, it remove all greek alphabets
    2005-07-19 - Fixed bad Dr./Prof. conversion [Simeon]
    """
    flags = re.IGNORECASE

    line = re.subn(r'Fig[s]?[\.]?\s', 'Figure ', line, flags=flags)[0]
    line = re.subn(r'Eq[s]?[\.]?\s', 'Equation ', line, flags=flags)[0]
    line = re.subn(r'Sect[s]?[\.]?\s', 'Section ', line, flags=flags)[0]
    line = re.subn(r'Ref[s]?[\.]?\s', 'Reference ', line, flags=flags)[0]
    line = re.subn(r'Prof\.', 'Prof', line, flags=flags)[0]
    line = re.subn(r'Dr\.', 'Dr', line, flags=flags)[0]
    return line


def _remove_Symbols(line: str) -> str:
    """_Remove_ symbols."""
    line = re.subn(r'[^\.\w ]', ' ', line)[0]
    line = re.subn(r'\_', ' ', line)[0]
    return line


def _remove_Numbers(line: str) -> str:
    """Remove digits."""
    line = re.subn(r'\d+[\.]?\d+/', ' ', line)[0]
    line = re.subn(r'\d', ' ', line)[0]
    return line


def _remove_Abbrev(line):
    """
    Remove abbreviation #.#.#. #.#. #. (e.g. U.S., U.S.A., ...)
    Abbreviations often cause problems when separating sentences.
    """
    line = re.subn(r'\s\w\.\w\.\w\.\s', ' ', line)[0]
    line = re.subn(r'\s\w\.\w\.\s', ' ', line)[0]
    line = re.subn(r'\s\w\.\s', ' ', line)[0]
    return line


def _remove_SingleAlphabet(line):
    """ Remove single characters: [b-zB-Z] """
    line = re.subn(r'\s[a-zA-Z]\s', ' ', line)[0]
    line = re.subn(r'\s[a-zA-Z]\s', ' ', line)[0]
    line = re.subn(r'\s[a-zA-Z]\.', r'.', line)[0]
    return line


def _remove_ExtraSpaces(line):
    """ Remove extra spaces """
    line = re.subn(r'\s+', ' ', line)[0]
    line = re.subn(r'^\s+', '', line)[0]
    return line


def _split_sentence(lines):
    """ Split sentences using the ". " as the delimiter """
    out = []
    for line in lines:
        out.extend(re.split(r'\.\s', line))
    return out


def _clean_sentence(lines):
    """ Remove non-alphabet from the sentences. Convert to lower-case """
    out = []
    for line in lines:
        # continue if the line does not have any words
        if not re.match(r'\w', line):
            continue

        # replace all non-alphabet to space
        line = re.subn(r'\W', ' ', line)[0]
        line = _remove_ExtraSpaces(line)

        # remove all space in the beginning and end of the sentence
        line = re.sub(r'^\s+', '', line)
        line = re.sub(r'\s+$', '', line)

        # Remove "sentences" that has less than or equal to 3 characters
        if len(line) <= 3:
            continue

        out.append(line.lower())
    return out


def split_on_references(lines, max_refs_fraction=0.5):
    """
    Mark the start of the references by looking for the last occurrence
    of the word "Reference" or "Bibliography"
    """
    regex_refsection = re.compile(
        r'^[^a-zA-Z]*(Reference[s]?|Bibliography)[\W]*$', flags=re.IGNORECASE
    )

    psv, ref = [], []
    line_num = 0
    last_refs = 0

    for line in lines:
        line_num += 1
        if regex_refsection.match(line):
            last_refs = line_num

    if line_num:
        refs_fraction = 1.0 - last_refs / line_num
        if refs_fraction > max_refs_fraction:
            print(
                "Not removing references as they comprise {}% of lines".format(
                    int(100*refs_fraction)
                )
            )
            last_refs = line_num + 1

    line_num = 0

    for line_num, line in enumerate(lines):
        if last_refs > 0 and line_num >= last_refs-1:
            ref.append(line)
        else:
            psv.append(line)
    return psv, ref


def _recover_accents(txt):
    """
    Hack to try to recover plain text with accents removed from various
    outputs from xpdf pdf->txt which garble accented characters into multi-byte
    sequences often including linefeed characeters
    """
    # umlaut, acute, cedilla, Angstrom. LF is not always present.
    txt = re.subn(r'[\xa8|\xb4|\xb8|\xb0]\x0a?', '', txt)[0]

    # dangerous accents that get represented
    # a printable literals followed by LF
    # (e.g. grave is ` with LF following)
    txt = re.subn(r'[\x5e|\x60|\x7e]\x0a', '', txt)[0]

    # these are straightforward single character substitions (o and O slash)
    txt = txt.replace('\xf8', 'o')
    txt = txt.replace('\xd8', 'O')

    # multi character substitutions (beta->ss)
    txt = txt.replace('\xdf', 'ss')
    txt = txt.replace('\xe6', 'ae')
    txt = txt.replace('\xc6', 'AE')
    return txt
