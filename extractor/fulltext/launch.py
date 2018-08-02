import os
import sys
sys.path.append(".")#/scripts")

import fulltext
import logging


log = logging.getLogger('fulltext')


if __name__ == '__main__':
    path = '/pdfs' if len(sys.argv) <= 1 else sys.argv[1]

    log.info("Convert pdfs in path '{}'".format(path))
    converts = fulltext.convert_directory(path)

    for convert in converts:
        log.info(
            "Sucessfully converted '{}' to plain text".format(
                os.path.basename(convert)
            )
        )
