"""Tests for :mod:`fulltext.process.psv`."""

from unittest import TestCase
from fulltext.process import psv

PAULI = """
**Pauli Virtanen** is SciPy's Benevolent Dictator For Life (BDFL).  He says:

*Truthfully speaking, we could have released a SciPy 1.0 a long time ago, so
I'm happy we do it now at long last. The project has a long history, and during
the years it has matured also as a software project.  I believe it has well
proved its merit to warrant a version number starting with unity.*

*Since its conception 15+ years ago, SciPy has largely been written by and for
scientists, to provide a box of basic tools that they need. Over time, the set
of people active in its development has undergone some rotation, and we have
evolved towards a somewhat more systematic approach to development.
Regardless, this underlying drive has stayed the same, and I think it will also
continue propelling the project forward in future. This is all good, since not
long after 1.0 comes 1.1.*
"""


class TestConvertToPSV(TestCase):
    """We have raw plain text extracted from a PDF."""

    def test_tidy_text(self):
        """All of the cleanup procedures are applied."""
        text = PAULI.replace('\n', ' \n').split('\n')
        expected = [
            'pauli virtanen is scipy benevolent dictator for life bdfl',
            'he says',
            'truthfully speaking we could have released scipy',
            'long time ago so',
            'i happy we do it now at long last',
            'the project has long history and during the years it has matured also as software project',
            'believe it has well proved its merit to warrant version number starting with unity',
            'since its conception years ago scipy has largely been written by and for scientists to provide box of basic tools that they need',
            'over time the set of people active in its development has undergone some rotation and we have evolved towards somewhat more systematic approach to development',
            'regardless this underlying drive has stayed the same and think it will also continue propelling the project forward in future',
            'this is all good since not long after',
            'comes']
        self.assertEqual(expected, psv.tidy_txt_from_pdf(text))

    def test_to_psv(self):
        expected = """pauli virtanen is scipy benevolent dictator for life bdfl he says truthfully speaking we could have released scipy long time ago so i happy we do it now at long last the project has long history and during the years it has matured also as software project believe it has well proved its merit to warrant version number starting with unity since its conception years ago scipy has largely been written by and for scientists to provide box of basic tools that they need over time the set of people active in its development has undergone some rotation and we have evolved towards somewhat more systematic approach to development regardless this underlying drive has stayed the same and think it will also continue propelling the project forward in future this is all good since not long after comes"""
        self.assertEqual(psv.normalize_text_psv(PAULI), expected)


class TestConvertToPSVUnits(TestCase):
    """We have raw plain text extracted from a PDF."""

    def test_expand_words(self):
        """The text contains common abbreviations."""
        raw = "Lorem Prof. Dr. ipsum dolor Fig. sit amet Sects. 1 Refs Eqs. 2"
        expanded = psv.expandWords(raw)
        self.assertEqual(
            expanded,
            "Lorem Prof Dr ipsum dolor Figure sit "
            "amet Section 1 Reference Equation 2",
            "Abbreviations should be expanded."
        )

    def test_text_has_symbols(self):
        """The text has symbols."""
        raw = "Bacon ipsum$@@ dolor amet lan!!!#djaeger chuc&&&^k bacon"
        expected = "Bacon ipsum    dolor amet lan    djaeger chuc    k bacon"
        self.assertEqual(psv._remove_Symbols(raw), expected,
                         "Symbols should be removed")

    def test_text_has_numbers(self):
        """The text has numbers."""
        raw = "Pork 2chop boudin5 picanha chic4ken"
        expected = "Pork  chop boudin  picanha chic ken"
        self.assertEqual(psv._remove_Numbers(raw), expected,
                         "Numbers should be removed")

    def test_text_has_unwanted_keywords(self):
        """The text has some unwanted content identified by keywords."""
        raw = [
            "Bacon ipsum dolor amet landjaeger chuck bacon boudin sausage",
            "arxiv ribs meatloaf chicken turducken bresaola shoulder. Pork",
            "chop boudin will be inserted by hand later picanha chicken short",
            "loin alcatra, turducken flank t-bone tail sirloin hamburger",
            "turkey short ribs prosciutto. Pork was prepared with the aas",
            "chop ribeye strip steak jerky, ball tip andouille leberkas cupim",
            "1234567890",
            "university",
            "ham. Pig meatloaf short ribs leberkas, cupim pork chop",
        ]
        expected = [
            "Bacon ipsum dolor amet landjaeger chuck bacon boudin sausage",
            "loin alcatra, turducken flank t-bone tail sirloin hamburger",
            "chop ribeye strip steak jerky, ball tip andouille leberkas cupim",
            "1234567890",
            "ham. Pig meatloaf short ribs leberkas, cupim pork chop",
        ]
        self.assertEqual(expected, psv._remove_Keyword(raw))

    def test_text_has_continuations(self):
        """The text has hyphenated continuations. Yuck."""
        raw = [
            "Bacon ipsum dolor amet landjaeger chuck bacon boudin saus- ",
            "age.",
            "Chop boudin picanha chicken short ",
            "hmmm"
        ]
        expected = [
            "",
            "Bacon ipsum dolor amet landjaeger chuck bacon boudin sausage.",
            "Chop boudin picanha chicken short hmmm"
        ]
        self.assertEqual(expected, psv._remove_BadEOL(raw))

    def test_whitespace_all_over_the_place(self):
        """The text has a variety of whitespace."""
        raw = [
            "Meatball\t pastrami chicken hamburger brisket ham hock capicola.",
            "Shankle turkey tongue\n\nsirloin meatloaf corned beef tail strip",
            "steak   sausage bacon beef ribs. "
        ]
        expected = [
            "Meatball  pastrami chicken hamburger brisket ham hock capicola.",
            "Shankle turkey tongue  sirloin meatloaf corned beef tail strip",
            "steak   sausage bacon beef ribs. "
        ]
        self.assertEqual(expected, psv._remove_WhiteSpace(raw))
