#!/bin/bash

# Upload package note:
# Generate token and set file -> $HOME/.pypirc
#
# [pypi]
#   username = __token__
#   password = pypi-XXXXXXXX
#

# upload to pypi
twine upload --config-file .ssh/pypirc dist/*
