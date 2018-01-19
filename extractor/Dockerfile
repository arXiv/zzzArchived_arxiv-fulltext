FROM python:3

LABEL maintainer "mkb72@cornell.edu"

# install the various packages necessary for python, pdftotext, pdf2txt
RUN apt update &&\
    apt install -f -y poppler-utils
    #apt install -f -y python3 &&\
    #apt install -f -y python3-pip &&\
    #apt autoclean -y &&\
    #apt clean &&\
    #rm -rf /tmp/* /var/tmp/* &&\
    #rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir pdfminer3k==1.3.1

RUN mkdir -p /scripts
ADD fulltext /scripts

VOLUME ["/pdfs"]

CMD /bin/bash /scripts/extract.sh
