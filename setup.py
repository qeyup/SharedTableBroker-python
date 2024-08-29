#!/usr/bin/env python3


import SharedTableBroker
import setuptools


if __name__ == '__main__':
    setuptools.setup(
        name = 'SharedTableBroker',
        version = SharedTableBroker.version,
        packages = ["SharedTableBroker"],
        install_requires = [
            "ServiceDiscovery>=0.4.1"
        ],
        author = "Javier Moreno Garcia",
        author_email = "jgmore@gmail.com",
        description = "",
        long_description_content_type = "text/markdown",
        long_description = "",
        url = "https://github.com/qeyup/SharedTableBroker-python"
    )
