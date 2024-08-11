#! /usr/bin/python3
# 
# This file is part of the d2dcn distribution.
# Copyright (c) 2023 Javier Moreno Garcia.
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but 
# WITHOUT ANY WARRANTY; without even the implied warranty of 
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU 
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License 
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import unittest
import DynamicMessagingBroker
import threading
import ServiceDiscovery


class container():
    pass


class Test1(unittest.TestCase):

    def test1_d2dcnProperties(self):

        test1 = DynamicMessagingBroker.d2d()
        #self.assertTrue(test1.mac != "")
        #self.assertTrue(test1.service != "")


if __name__ == '__main__':
    unittest.main()