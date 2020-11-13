# arxiv/fulltext-extractor:0.3

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

RUN pip3 install --no-cache-dir  pdfminer.six==20201018

ADD fulltext /scripts

VOLUME ["/pdfs"]

ENTRYPOINT ["python", "/scripts/launch_single.py"]
