FROM arxiv/base:latest

LABEL maintainer "mkb72@cornell.edu"

RUN yum -y install poppler-utils

RUN pip3 install --no-cache-dir pdfminer3k==1.3.1

RUN mkdir -p /scripts
ADD fulltext /scripts

VOLUME ["/pdfs"]

CMD /bin/bash /scripts/extract.sh
