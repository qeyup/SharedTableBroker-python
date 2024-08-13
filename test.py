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
import SharedTableBroker
import threading
import time


class constants():
    DOMAIN = "TEST"
    TEST_INSTANCES = 30
    TEST_TIMEOUT = 10


class container():
    pass


class Test1(unittest.TestCase):

    def test1_startStop(self):

        table_broker = SharedTableBroker.SharedTableBroker(constants.DOMAIN + "_1")
        table_broker.updateTableEntry("entry_key", ["A", "B", "C"])
        del table_broker


    def test2_readUpdatesLastConnected(self):

        broker_list = []
        for i in range(constants.TEST_INSTANCES):
            table_broker = SharedTableBroker.SharedTableBroker(constants.DOMAIN + "_2")
            table_broker.updateTableEntry("entry_key" + str(i), ["A", "B", "C"])
            broker_list.append(table_broker)

        shared = container()
        shared.wait = threading.Lock()
        shared.wait.acquire()
        shared.new_count = 0
        def dataRecived(client_id, entry_key, data, shared):
            shared.new_count += 1
            if shared.new_count == constants.TEST_INSTANCES:
                shared.wait.release()

        table_broker_check = SharedTableBroker.SharedTableBroker(constants.DOMAIN + "_2", False)
        table_broker_check.onNewTableEntry = lambda client_id, entry_key, data, shared=shared: dataRecived(client_id, entry_key, data, shared)
        shared.wait.acquire(blocking=True, timeout=constants.TEST_TIMEOUT)
        current_table = table_broker_check.geTableData()
        self.assertTrue(len(current_table) == len(broker_list), str(len(current_table)) + "/" + str(len(broker_list)))


    def test3_readUpdatesFirstConnected(self):

        shared = container()
        shared.wait = threading.Lock()
        shared.wait.acquire()
        shared.new_count = 0
        def dataRecived(client_id, entry_key, data, shared):
            shared.new_count += 1
            if shared.new_count == constants.TEST_INSTANCES:
                shared.wait.release()

        table_broker_check = SharedTableBroker.SharedTableBroker(constants.DOMAIN + "_3", True)
        table_broker_check.onNewTableEntry = lambda client_id, entry_key, data, shared=shared: dataRecived(client_id, entry_key, data, shared)

        broker_list = []
        for i in range(constants.TEST_INSTANCES):
            table_broker = SharedTableBroker.SharedTableBroker(constants.DOMAIN + "_3")
            table_broker.updateTableEntry("entry_key" + str(i), ["A", "B", "C"])
            broker_list.append(table_broker)


        shared.wait.acquire(blocking=True, timeout=constants.TEST_TIMEOUT)
        current_table = table_broker_check.geTableData()
        self.assertTrue(len(current_table) == len(broker_list), str(len(current_table)) + "/" + str(len(broker_list)))



    def test4_readFirstMasterDisconected(self):

        broker_list = []
        for i in range(constants.TEST_INSTANCES):
            table_broker = SharedTableBroker.SharedTableBroker(constants.DOMAIN + "_4")
            table_broker.updateTableEntry("entry_key" + str(i), ["A", "B", "C"])
            table_broker.updateTableEntry("entry_key2" + str(i), ["A", "B", "C"])
            broker_list.append(table_broker)


        def localTest(broker_list):

            for broker in broker_list:
                if broker.isMaster():
                    del broker
                    break
 
            time.sleep(10)

            for i in range(len(broker_list)):
                if broker_list[i]:
                    current_table = broker_list[i].geTableData()
                    current = len(current_table)
                    expected = len(broker_list) * 2
                    self.assertTrue(current == expected, str(current) + "/" + str(expected))

        time.sleep(1)
        localTest(broker_list)
        localTest(broker_list)
        localTest(broker_list)
        localTest(broker_list)





if __name__ == '__main__':
    unittest.main()