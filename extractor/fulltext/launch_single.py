import os
import sys
# sys.path.append(".")
from arxiv.base import logging
from fulltext import convert

log = logging.getLogger('fulltext')


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        sys.exit('No file path specified')
    path = sys.argv[1].strip()
    try:
        log.info('Path: %s\n' % path)
        log.info('Path exists: %s\n' % str(os.path.exists(path)))
        textpath = convert(path)
    except Exception as e:
        sys.exit(str(e))
    sys.stdout.write(textpath)
