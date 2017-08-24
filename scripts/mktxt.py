#!/usr/local/bin/python27

import sys
sys.path.append('/localdisk/scratch/pylib')

from os.path import isfile
import re
from time import ctime

#instead use https://pypi.python.org/pypi/subprocess32/
from subprocess32 import check_output,STDOUT,CalledProcessError,TimeoutExpired

#import signal
#class TimeoutException(Exception):
#        pass
#def receive_alarm(signum, stack):
#    print 'Alarm :', ctime()
#    raise TimeoutException()
#signal.signal(signal.SIGALRM, receive_alarm)
#stackoverflow.com/questions/15722523/python-anticipating-an-endless-loop
#http://pymotw.com/2/signal/

pdf2txt='/usr/local/bin/pdf2txt.py'
pdftotext='/usr/bin/pdftotext'
ftextnew='/home/ginsparg/bin/ftextnew.pl'


def mkchtxt(pdffile,txtfile,alrm=600):
    if not isfile(pdffile): return 'error: no ' + pdffile
    mktxt(pdffile,txtfile,alrm)
    return chtxt(pdffile,txtfile,alrm)
    

def mktxt(pdffile,txtfile,alrm=600,opt=''):
    tmpfile=txtfile[:-4]+'.tmp'
#    signal.alarm(alrm)  #unsets if alrm=0

    if not isfile(pdffile):
       print 'error: no ' + pdffile
       return

    try:
#      print check_output(' '.join([pdf2txt,pdffile,'>',tmpfile]),shell=True),
      pdf2txtopt = [pdf2txt]
      if opt != '': pdf2txtopt += [opt]
      print check_output(pdf2txtopt + ['-o',tmpfile,pdffile],timeout=alrm),
    except (TimeoutExpired,CalledProcessError) as e:
      print ' ***FAILURE*** pdf2txt.py {} > {}'.format(pdffile,tmpfile)
      print e
      print check_output([pdftotext,pdffile,tmpfile]),
      print 'Instead used pdftotext',pdffile,tmpfile
#    finally:
#      signal.alarm(0)

    ftn=check_output([ftextnew,tmpfile,txtfile]),
    return


def chtxt(pdffile,txtfile,alrm=600):
    avgw = getavgw(txtfile)
    if type(avgw) == str: return avgw
    if avgw[2] <= 45: return 'OK: {} {}'.format(txtfile,avgw[2])

    print 'redo',txtfile,avgw
    if not isfile(pdffile): return 'error: no ' + pdffile
    tmpfile=txtfile[:-4]+'.tmp'

    mktxt(pdffile,txtfile,alrm,'-A')

    navgw = getavgw(txtfile)
    if type(navgw) == str: return navgw

    if navgw[2] > 45: return 'error: {} still bad {}'.format(txtfile,navgw[2])
    return 'OK: {}, {} to {}'.format(txtfile,avgw[2],navgw[2])


def getavgw(txtfile):
    if not isfile(txtfile): return 'error: no ' + txtfile

    txt=open(txtfile).read()
    try:
      txt=txt.decode('utf-8')
    except UnicodeDecodeError as e:
      print txtfile,e
    txt=re.sub(r'(\(cid:\d+\)|lllll|\.\.\.\.\.|\*\*\*\*\*)','',txt)  ##.replace('llllll','')
    #txt=re.sub(r'(.)\1{4,}',r'\1',txt) #any string of five
    nw=len(txt.split())
    nc=len(txt)
    try:
      avgw=round(float(nc)/nw,2)
    except ZeroDivisionError as e:
      return 'error: {} for {}'.format(e,txtfile)
    return (nc,nw,avgw)


if __name__ == '__main__':
    if len(sys.argv) > 3 and sys.argv[3].isdigit():
       print 'using timeout=',int(sys.argv[3])
       print mkchtxt(sys.argv[1],sys.argv[2],int(sys.argv[3]))
    else:
       print mkchtxt(sys.argv[1],sys.argv[2])
