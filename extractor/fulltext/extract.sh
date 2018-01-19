#!/bin/bash
echo $(python3.6 --version)
echo $(pip -V)
echo $(pip freeze | grep pdf)
python3.6 /scripts/launch_single.py $1
