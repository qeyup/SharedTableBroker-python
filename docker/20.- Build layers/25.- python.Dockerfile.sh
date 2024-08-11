#!/bin/bash
set -e


# Upgrade system
PACKAGES=()
PACKAGES+=(psutil)
PACKAGES+=(ServiceDiscovery)
PACKAGES+=(wheel)
PACKAGES+=(setuptools)
PACKAGES+=(twine)

# Install all
pip3 install ${PACKAGES[@]}
