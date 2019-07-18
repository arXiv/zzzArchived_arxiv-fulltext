#!/bin/bash

set -e

PROJECT=$1

pipenv run pydocstyle --convention=numpy --add-ignore=D401 ${PROJECT}
PYDOCSTYLE_STATUS=$?
if [ $PYDOCSTYLE_STATUS -ne 0 ]; then PYDOCSTYLE_STATE="failure" && echo "pydocstyle failed"; else PYDOCSTYLE_STATE="success" &&  echo "pydocstyle passed"; fi

if [ -z ${GITHUB_TOKEN} ]; then
    echo "Github token not set; will not report results";
else 
    curl -u $USERNAME:$GITHUB_TOKEN \
        -d '{"state": "'$PYDOCSTYLE_STATE'", "target_url": "https://travis-ci.org/'$TRAVIS_REPO_SLUG'/builds/'$TRAVIS_BUILD_ID'", "description": "", "context": "'$PROJECT'/code-quality/pydocstyle"}' \
        -XPOST https://api.github.com/repos/$TRAVIS_REPO_SLUG/statuses/$SHA \
        > /dev/null 2>&1;
fi