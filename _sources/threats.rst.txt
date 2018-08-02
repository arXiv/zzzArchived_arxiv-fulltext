Threat model
============
This document gives an overview of the security considerations for arXiv user
and client sessions, and the services that support authentication and
authorization. It should be updated as our understanding of relevant threats
evolves.

.. todo:

   This version of the document focuses on user sessions. It should be updated
   to include API client sessions as that part of the system is implemented.


Environment
-----------
End users interact with the arXiv platform primarily via a web browser, on
either a desktop or a mobile device. During the arXiv-NG project, users may
interact with components deployed in on-premises infrastructure, or in our
Kubernetes cluster running in AWS.

.. _figure-auth-overview:

.. figure:: _static/diagrams/auth-overview.png

   Overview of the authentication and authorization infrastructure during the
   transition from legacy to NG system.

For the purposes of this analysis, we assume that:

1. TLS between the client and the on-premises web server, and between the
   client and the load balancer that proxies services in Kubernetes, is secure.
2. The user has a baseline level of trust in their own web browser. This
   includes trusting that the web browser properly implements TLS protocols,
   will enforce cookie security, and will enforce common security policies
   such as preventing cross-origin requests.
3. Infrastructure both on-premises and in AWS are properly configured for
   security, making direct access to protected resources (e.g. databases,
   session store) from the public internet impossible.


Protected data
--------------
Here are some of the kinds of data that we must protect from unauthorized
access and/or manipulation:

1. Account usernames and passwords.
2. Information about the user that they have not chosen to share publicly. For
   example, their e-mail address.
3. Submission metadata and files.

Threats/risks
-------------
Potential attack vectors and failure scenarios include:

1. **Click-jacking.** For example, a malicious site or application might
   attempt to display sensitive pages on the arXiv site in an iframe, and
   record or manipulate UI events to intercept information or perform
   unauthorized actions.
2. **Cross-site scripting (XSS).*** This encompasses a broad array of attacks
   that involve injection and execution of malicious code in another user's
   browser. Of particular interest in form-based views is a reflected XSS
   attack, in which malicious code is passed as input that is then reflected
   without sanitization on an error page or other message.
3. **Replay attacks.** A malicious site or application might trick users into
   issuing a fraudulent request to the arXiv site. For example, a malicious
   site might provide a fraudulent login form that generates POST requests
   to the arXiv login page, but also records the user's login credentials.
   Another example is an attack that causes a user to issue a request that
   results in deletion or corruption of content, such as submission content.
4. **Direct impersonation via a session cookie.** An attacker might get access
   to a cookie containing a key for an authenticated session, and attempt to
   impersonate a user by sending the cookie in their own requests.
5. **Direct impersonation via a permanent login token.** Users have the option
   of  being issued a permanent login token, which is stored in a cookie. That
   token can be used to automatically log the user in on subsequent visits to
   the arXiv site. If an attacker were to gain access to that token, they might
   be able to impersonate the victim by including the token in their own
   requests.
6. **Leakage in access or application logs.** Access to logs is heavily
   restricted. Nevertheless, access and/or application logs are at a higher
   risk of being  leaked into insecure contexts than other sensitive data. For
   example, a  developer might post an un-sanitized log sample to a message
   board, or send it via a messaging system, while attempting to troubleshoot a
   problem.  For this reason, sensitive data should be prevented from entering
   access and application logs wherever possible.
7. **Code execution (e.g. SQL injection).** Attackers may attempt to cause
   arXiv software to execute code, such as SQL commands, resulting in
   corruption or exfiltration of sensitive data.
8. **Brute force login attacks.** An attacker may attempt to gain access to
   another user's account by simply trying a large volume of username/password 
   combinations.
9. **Programmatic account creation.** It is important that a human user take
   overt and specific action to create an account for themselves. A malicious 
   actor may attempt to generate accounts programmatically, which would 
   undermine that policy.


Prevention & design considerations
----------------------------------

Same-origin policy protections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
All arXiv applications and services should prevent click-jacking attacks by
setting the ``Content-Security-Policy`` and ``X-Frame-Options`` headers on all
responses. For example:

.. code-block:: python

   response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
   response.headers["X-Frame-Options"] = "SAMEORIGIN"

This prevents arXiv pages from being displayed on other sites (e.g. via an
iframe).

Cross-Site Request Forgery protection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The risk of replay attacks or other attacks that involve generating fraudulent
requests can be mitigated through CSRF protection. This usually involves
issuing a token to the client that must be included in a subsequent request
(e.g. when POSTing form data). The :mod:`wtforms` package provides `tools for
CSRF protection
<http://wtforms.readthedocs.io/en/latest/csrf.html#module-wtforms.csrf>`_,
including an API for implementing a CSRF protection mechanism.

.. todo::

   Document CSRF implementation in :mod:`arxiv.base`.


Input sanitization
^^^^^^^^^^^^^^^^^^
We use :mod:`wtforms` for all form-handling in arXiv-NG applications. By
default, WTForms uses `MarkupSafe <https://github.com/pallets/markupsafe>`_ to
escape unsafe HTML in all form input. This means that form data accessed in
templates and other locations should be safe for display without risk of
reflection XSS attacks.

We use :mod:`sqlalchemy` to interact with the legacy database and other
relational databases. We can minimize the risk of SQL injection attacks by
never writing raw SQL statements, and instead using the SQLAlchemy ORM/DSL
to build queries.

Cookie security
^^^^^^^^^^^^^^^
Session keys and permanent login tokens that are stored in cookies should be
set with the following attributes:

- ``HttpOnly``: prevents access to the cookie by client-side scripts.
- ``Secure``: the cookie should only be transmitted via HTTPS.
- ``SameSite``: prevents sending the cookie with cross-site requests.

Enforcement of these cookie policies is up to the user's browser.

These attributes can be set when using the :meth:`flask.Response.set_cookie`
API. For example:

.. code-block:: python

   response = make_response(...)
   response.set_cookie('foocookie', 'secret value', secure=True,
                       httponly=True, samesite=True)

In addition, both legacy and NG session cookies are signed with a secret hash.

In the former case, the cookie is verified by generating a digest from its
contents along with a salt and comparing it to the digest transmitted in the
cookie itself.

In the latter case, the cookie is an encrypted JWT that must be decrypted
successfully using a secret key. In addition, a **pseudo-random nonce** created
at the start of the authenticated session is stored in both the session store
and in the token payload for comparison. This means that a compromised token
cannot be used to generate a new valid token.

In both cases, the **IP address** of the client that initiated the
authenticated  session is stored in the session data. Requests using a valid
session token  from a different IP address should be considered potentially
malicious, and  the session invalidated.

Short-lived sessions
^^^^^^^^^^^^^^^^^^^^
Authenticated sessions should be limited in duration to N hours. In the
unlikely event that a malicious actor gains access to a secure session cookie,
this limits the potential for impersonation.

Discontinue use of permanent login tokens
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The legacy system supports a "remember me" token that is stored as a secure
cookie on the user's browser. This permanent login token bypasses password
authentication, and is valid at any IP address. The permanent login token will
be discontinued in arXiv-NG.

Captcha
^^^^^^^
In parts of the system where we want to prevent programmatic access (e.g.
account creation, e-mail harvesting), some kind of robot-deterence should be
used. While not a perfect solution, an image-based Captcha does provide a
baseline level of confidence that a request has originated from a human 
user.

Log sanitization
^^^^^^^^^^^^^^^^

.. todo:

   Consider a sanitizing filter for :mod:`arxiv.base.logging`. See
   https://docs.python.org/3/library/logging.html#filter-objects
