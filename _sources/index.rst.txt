.. _index:

arXiv Fulltext Extraction Service
=================================
The arXiv fulltext extraction service provides plain text of arXiv submissions
and announced papers, for use in QA/QC workflows and to support research.

Objectives & Requirements
-------------------------
1. Must be able to extract full text content from any well-formed submission,
   and for all announced arXiv e-prints.
2. The public API must only allow access to full text from announced papers.
3. A request for full text should return an extraction with the most recent
   version of the application, since our extraction process will improve over
   time.
4. Must provide both the raw plain text content and a PSV-tokenized format.
5. It must be possible to (re-)extract plain text content for the entire
   corpus.
6. Plain text extraction should occur automatically whenever a new e-print is
   announced.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   architecture.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
