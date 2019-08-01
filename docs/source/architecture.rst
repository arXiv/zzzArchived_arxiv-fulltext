Architecture
============

Context
-------
The fulltext extraction service is used during the submission process as part
of QA/QC workflows, and after announcement to support indexing and other
downstream applications.

.. _figure-context:

.. figure:: _static/diagrams/fulltext-service-context.png
   :width: 450px

   System context for the fulltext extraction service.

The submission worker process will request fulltext content for submissions by
making a GET request to the fulltext service. If an extraction for the
submission using the latest version of the extractor does not exist, the
service will retrieve the compiled PDF for the submission from the compilation
service and perform the extraction. The submission worker may force
re-extraction (e.g. if the submission content changes) by making a POST
request.

When a new e-print is announced, the publication process generates a
notification on a Kinesis stream that is consumed by the extraction service.
The extraction service retrieves the PDF for the e-print, performs the
extraction, and makes the result available via its API.

Clients (including both arXiv services and external API clients) may request
the latest extraction by making a GET request. If an extraction does not exist
for the paper using the most recent version of the extraction process, the
service retrieves the paper and performs the extraction.

Containers
----------
A web application implemented in Flask handles HTTP requests for
extractions. Extraction can be requested via POST request, which creates a
task for a worker process on a Redis queue (using Celery).

The worker process monitors the Redis queue, and does the work of retrieving
PDFs, performing extractions, and storing the results. A Docker-in-Docker
container is deployed as a side-car to the worker; the worker performs
extraction by running the ``arxiv/fulltext-extractor`` image in the dind
container.

Extractions are stored on a network filesystem, and organized by submission or
e-print ID and extractor version. When an extraction task is created, a
placeholder file with the task ID is stored where the content will eventually
be. This helps to prevent duplicate extraction tasks.


.. _figure-containers:

.. figure:: _static/diagrams/fulltext-service-containers.png
   :width: 450px

   Container-level view of the fulltext extraction service.


Components
----------
The fulltext extraction service implements the general architecture described
in :doc:`arxitecture:crosscutting/services`.

.. _figure-components:

.. figure:: _static/diagrams/fulltext-service-components.png
   :width: 600px

   Component-level view of the fulltext extraction service.

Two service modules, :mod:`fulltext.services.pdf` and
:mod:`fulltext.services.store`, provide integration with arXiv PDF content
and the filesystem, respectively.

:mod:`fulltext.routes` defines the HTTP API exposed by the
:mod:`fulltext.factory` application entry-point. Request handling is
performed by :mod:`fulltext.controllers`, which orchestrates loading of
fulltext content and generates extraction tasks via :mod:`fulltext.extract`.

The :mod:`fulltext.worker` module provides an entry-point for the extraction
worker process, which listens for tasks defined in :mod:`fulltext.extract`.

The extraction task itself (in :mod:`fulltext.extract`) uses a Docker image
(``extractor``) to perform the actual extraction. This is defined separately
from the main application.

Authentication & Authorization
------------------------------
The ``fulltext:create`` scope is required to request new extractions. The
``fulltext:read`` scope is required to access extracted plain text. The latter
will be more common with external API consumers, whereas ``fulltext:create``
is primarily intended for other services.

To extract text from submissions, the ``compile:read`` scope is also required.

Whereas announced e-prints require no special ownership privileges, submissions
are more protected. The preview service (from whence we obtain PDFs for
submissions from which to extract text) publishes the identity of the resource
owner (their user ID) via the ``ARXIV-OWNER`` header. The plaintext extraction
service must implement authorization checks to ensure that only the owner (or a
delegate with global or explicit privileges for that resource) may request
extraction and access the status/results of the extraction.
