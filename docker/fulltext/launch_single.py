import os
import sys
# sys.path.append(".")
from fulltext import logger
from fulltext import convert

log = logger.getLogger('fulltext')

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        sys.exit('No file path specified')
    path = sys.argv[1]
    try:
        textpath = convert(path)
    except Exception as e:
        sys.exit(str(e))
    sys.stdout.write(textpath)
