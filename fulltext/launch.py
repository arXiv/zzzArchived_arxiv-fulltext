import os
import sys
sys.path.append("/scripts")

import logging
import fulltext

log = logging.getLogger('fulltext')

if __name__ == '__main__':
    converts = fulltext.convert_directory('/pdfs')

    for convert in converts:
        log.info(
            "Sucessfully converted '{}' to plain text".format(
                os.path.basename(convert)
            )
        )
