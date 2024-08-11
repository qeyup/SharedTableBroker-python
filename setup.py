#!/usr/bin/env python3


import DynamicMessagingBroker
import setuptools


if __name__ == '__main__':
    setuptools.setup(
        name = 'DynamicMessagingBroker',
        version = DynamicMessagingBroker.version,
        packages = ["DynamicMessagingBroker"],
        install_requires = [
            "ServiceDiscovery"
        ],
        author = "Javier Moreno Garcia",
        author_email = "jgmore@gmail.com",
        description = "",
        long_description_content_type = "text/markdown",
        long_description = "",
        url = "https://github.com/qeyup/DynamicMessagingBroker-python"
    )
