#
# This file is part of the SharedTableBroker distribution.
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

import os
import socket
import threading
import time
import json
import ServiceDiscovery
import weakref
import uuid


version = "0.1.0"


class constants():

    DEFAULT_DOMAIN = "DYNAMIC_MESSAGING_BROKER_DOMAIN"
    MQTT_BROKER_PORT = 1883
    BROKER_PORT = 18832
    CLIENT_DISCOVER_WAIT = 5
    MTU = 4096
    START_TX = b'\xEF'
    END_OF_TX = b'\xFF'
    MAX_LISTEN_TCP_SOKETS = -1
    ENTRY_KEY_SEP = "/"
    WAIT_TO_REMOVE_TIME = 5
    WAIT_BROKER_RESPONSE = 5
    CLIENT_ID_TAG = 'clientID:'
    BROKER_ID_TAG = 'brokerID:'


class debug():


    def trace(*kwargs):
        return
        print(*kwargs)


    def rx(instance, msg):
        return
        print(" <~~", instance, "~~", msg)


    def tx(instance, msg):
        return
        print(" ~~", instance, "~~>", msg)


class tcpListener():


    class connection():

        def __init__(self, connection, ip, port):
            self.__sock = connection
            self.__ip = ip
            self.__port = port
            self.__open = True
            self.__sock.settimeout(0.1)


        def read(self, timeout=-1):

            current_epoch_time = int(time.time())
            while self.__open:
                try:
                    data = self.__sock.recv(constants.MTU)

                    if len(data) > 0:
                        debug.rx(hex(id(self)), data)
                        return data

                    else:
                        self.close()
                        self.__open = False
                        return None

                except socket.timeout:
                    if timeout >= 0 and int(time.time()) - current_epoch_time >= timeout:
                        return None

                except socket.error:
                    self.close()
                    self.__open = False
                    return None


        def send(self, msg):
            if isinstance(msg, str):
                msg = msg.encode()

            chn_msg = [msg[idx : idx + constants.MTU] for idx in range(0, len(msg), constants.MTU)]

            try:
                for chn in chn_msg:
                    self.__sock.sendall(chn)

            except:
                return False

            debug.tx(hex(id(self)), msg)
            return True


        def isConnected(self):
            return self.__open


        @property
        def port(self):
            return self.__sock.getsockname()[1]


        @property
        def clientIp(self):
            return self.__ip


        @property
        def clientPort(self):
            return self.__port


        def close(self):
            self.__open = False
            self.__sock.close()


    def __init__(self, port=0, max_connections=constants.MAX_LISTEN_TCP_SOKETS):
        super().__init__()
        self.__open = True
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.__sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__sock.bind(('', port))
        self.__sock.settimeout(0.1)
        self.__sock.listen(max_connections)


    def __del__(self):
        self.close()


    @property
    def port(self):
        return self.__sock.getsockname()[1]


    def waitConnection(self, timeout=-1):

        current_epoch_time = int(time.time())
        while self.__open:
            try:
                connection, (ip, port) = self.__sock.accept()
                return tcpListener.connection(connection, ip, port)


            except socket.timeout:
                if timeout >= 0 and (int(time.time()) - current_epoch_time >= timeout):
                    return None

            except socket.error:
                return None

        return None


    def close(self):
        self.__open = False
        self.__sock.close()


class tcpClient():
    def __init__(self, ip, port):
        self.__open = False
        self.__remote_ip = ip
        self.__remote_port = port
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.settimeout(0.1)


    def __del__(self):
        self.close()


    def connect(self):
        if not self.__open:
            self.__open = self.__sock.connect_ex((self.__remote_ip, self.__remote_port)) == 0
        return self.__open


    def read(self, timeout=-1):

        if self.connect():
            current_epoch_time = int(time.time())
            while self.__open:
                try:
                    data = self.__sock.recv(constants.MTU)
                    debug.rx(hex(id(self)), data)
                    return data

                except socket.timeout:
                    if timeout >= 0 and int(time.time()) - current_epoch_time >= timeout:
                        return None

                except socket.error:
                    self.close()
                    if not self.connect():
                        return None

        return None


    def send(self, msg):
        if self.connect():

            if isinstance(msg, str):
                msg = msg.encode()

            chn_msg = [msg[idx : idx + constants.MTU] for idx in range(0, len(msg), constants.MTU)]
            while True:

                try:
                    for chn in chn_msg:
                        self.__sock.sendall(chn)

                    debug.tx(hex(id(self)), msg)
                    return True

                except:
                    self.close()
                    if not self.__connect():
                        return False

        return False


    @property
    def connected(self):
        return self.__open


    @property
    def local_ip(self):
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname)


    @property
    def local_port(self):
        return self.__sock.getsockname()[1]


    @property
    def remote_ip(self):
        return self.__remote_ip


    @property
    def remote_port(self):
        return self.__remote_port


    def close(self):
        self.__open = False
        self.__sock.close()


class container():
    pass


class SharedTableBroker():

    instance_count = 0

    def __init__(self, domain=constants.DEFAULT_DOMAIN, master=True):

        SharedTableBroker.instance_count += 1

        self.__master_container = container()
        self.__client_container = container()

        self.__client_container.domain = domain

        self.__master_container.run = True
        self.__client_container.run = True

        if master:
            self.__master_container.master_listener = tcpListener()
            self.__master_container.discovery_daemon = ServiceDiscovery.daemon(domain)
            self.__master_container.discovery_daemon.setPort(self.__master_container.master_listener.port)

        else:
            self.__master_container.master_listener = None
            self.__master_container.discovery_daemon = None

        self.__client_container.discovery_daemon = self.__master_container.discovery_daemon

        self.__client_container.client_id = hex(uuid.getnode()).replace("0x", "") + ":" + str(os.getpid()) + str(SharedTableBroker.instance_count)
        self.__master_container.client_id = self.__client_container.client_id
        self.__client_container.discovery_client = ServiceDiscovery.client()
        self.__client_container.client = None

        self.__master_container.master_clients = []
        self.__client_container.client_table_data = {}
        self.__master_container.connected_clients_id = []
        self.__master_container.table_data_broker = {}
        self.__client_container.table_data = {}
        self.__client_container.table_data_old = {}

        self.__master_container.mutex = threading.RLock()
        self.__client_container.mutex = threading.RLock()
        self.__client_container.start_mutex = threading.Lock()
        self.__client_container.start_mutex.acquire()


        # Callcaks
        self.__client_container.on_new_table_entry = None
        self.__client_container.on_update_table_entry = None
        self.__client_container.on_remove_table_entry = None


        # Launch broker master
        if master:
            self.__master_container.discovery_daemon.run()
            self.__broker_thread = threading.Thread(target=SharedTableBroker.__masterDaemonThread, daemon=True, args=[self.__master_container])
            self.__broker_thread.start()

        else:
            self.__broker_thread = None

        # Launch broker client
        self.__client_thread = threading.Thread(target=SharedTableBroker.__clientDaemonThread, daemon=True, args=[self.__client_container])
        self.__client_thread.start()

        # Wait first conection
        self.__client_container.start_mutex.acquire()


    def __del__(self):
        self.stop()


    def __readFrames(connection):

        read_buffer = bytearray()
        while True:
            input_data = connection.read()
            if not input_data or len(input_data) == 0:
                return None

            read_buffer += input_data

            last_byte = bytes([read_buffer[-1]])
            if last_byte == constants.END_OF_TX:
                break

        return read_buffer[:-1].split(constants.END_OF_TX)


    def __masterDaemonThread(shared):
        while shared.run:

            # wait new client connection
            connection = shared.master_listener.waitConnection()
            if shared.discovery_daemon.getEnable():
                threading.Thread(target=SharedTableBroker.__masterConnectionThread, daemon=True, args=[shared, connection]).start()
            elif connection:
                connection.close()


    def __cleanOldEntries(shared):

        time.sleep(constants.WAIT_TO_REMOVE_TIME)


        with shared.mutex:
            shared.table_data_old = {}


    def __masterConnectionThread(shared, connection):

        # Check valid connection
        if not connection:
            return


        # Thread safe
        with shared.mutex:

            # Read broker-ID and get Client-ID
            connected_client_id = None
            input_data = connection.read(constants.WAIT_BROKER_RESPONSE)
            if input_data and input_data.startswith(bytearray(constants.CLIENT_ID_TAG, encoding="utf8")):
                connected_client_id = input_data.decode("utf8").replace(constants.CLIENT_ID_TAG, "")
                connection.send(bytearray(constants.BROKER_ID_TAG + shared.client_id, encoding="utf8"))
                input_data = connection.read(constants.WAIT_BROKER_RESPONSE)
                if input_data != constants.START_TX:
                    connection.close()
                    return


            if not shared.client_id or not connected_client_id:
                connection.close()
                return


            # add client DB
            if connected_client_id not in shared.table_data_broker:
                shared.table_data_broker[connected_client_id] = {}


            # Send current DB to new connection
            debug.trace("New client", connection.clientIp)


            # Send all current info to new client
            all_db_send = True
            for client_id in shared.table_data_broker:
                for entry_key in shared.table_data_broker[client_id]:

                    if not connection.send(shared.table_data_broker[client_id][entry_key] + constants.END_OF_TX):
                        debug.trace("Client error")
                        all_db_send = False
                        break


            # Start listen thread
            if all_db_send:
                shared.master_clients.append(connection)
                shared.connected_clients_id.append(connected_client_id)

            else:
                debug.trace("Client closed")
                connection.close()
                return


        # Read loop
        while shared.run and connection.isConnected():

            # Wait client incoming
            input_frames = SharedTableBroker.__readFrames(connection)
            if not input_frames:
                break


            # Thread safe
            with shared.mutex:

                # Save and propagate
                clients_with_errors = []
                for msg in input_frames:
                    try:
                        input_dict = json.loads(msg)
                    except:
                        debug.trace("Input error", msg)
                        continue

                    # Save / Remove
                    if len(input_dict) == 1:

                        # Remove client entries
                        if input_dict[0] in shared.table_data_broker:
                            del shared.table_data_broker[input_dict[0]]

                    elif len(input_dict) == 2:

                        # Remove client entry
                        if (input_dict[0] in shared.table_data_broker
                            and input_dict[1] in shared.table_data_broker[input_dict[0]][input_dict[1]]):
                            del shared.table_data_broker[input_dict[0]][input_dict[1]]

                    elif len(input_dict) > 2:
                        shared.table_data_broker[input_dict[0]][input_dict[1]] = msg


                    # Propagate to other clients (included sender client and own client)
                    for client in shared.master_clients:
                        if not client.send(msg + constants.END_OF_TX):
                            clients_with_errors.append(client)
                            debug.trace("Client error 2")
                            continue


                # Remove clients with errors
                for client in clients_with_errors:
                    debug.trace("Client closed", client.clientPort)
                    client.close()


        # Close connection
        connection.close()


        # Thread safe
        with shared.mutex:

            # Remove client from list
            del shared.table_data_broker[connected_client_id]
            shared.master_clients.remove(connection)
            shared.connected_clients_id.remove(connected_client_id)


            # Notify to remove client entries
            for client in shared.master_clients:
                raw = json.dumps([connected_client_id])
                if not client.send(bytearray(raw, encoding='utf8') + constants.END_OF_TX):
                    debug.trace("Client error")


    def __clientDaemonThread(shared):

        while shared.run:

            # Discover master
            master_ip, port = shared.discovery_client.getServiceIPAndPort(shared.domain, timeout=constants.CLIENT_DISCOVER_WAIT)
            if master_ip != None and master_ip != "":

                debug.trace("[Broker] -> ", port)

                # Connect client
                client = tcpClient(master_ip, port)
                if not client.connect():
                    debug.trace("[Broker disconnection] -> ", client.local_port)
                    continue


                # Send Client-ID and get broker-ID
                broker_id = None
                client.send(bytearray(constants.CLIENT_ID_TAG + shared.client_id, encoding="utf8"))
                input_data = client.read(constants.WAIT_BROKER_RESPONSE)
                if input_data and input_data.startswith(bytearray(constants.BROKER_ID_TAG, encoding="utf8")):
                    broker_id = input_data.decode("utf8").replace(constants.BROKER_ID_TAG, "")
                    client.send(constants.START_TX)


                if not shared.client_id or not broker_id:
                    continue


                # Disable master
                if shared.discovery_daemon != None and not shared.discovery_daemon.isMaster():
                    shared.discovery_daemon.setEnable(False)


                # Thread safe
                with shared.mutex:


                    # Connect to master
                    shared.client = client


                    # Send current data
                    for client_entry in shared.client_table_data:
                        SharedTableBroker.__updateTableEntry(shared, client_entry, shared.client_table_data[client_entry], False)


                    # Enable API usage
                    if shared.start_mutex.locked():
                        shared.start_mutex.release()


                # Read loop
                debug.trace("Start client", broker_id, shared.client_id)
                while shared.run and shared.client.connected:

                    input_frames = SharedTableBroker.__readFrames(shared.client)
                    if not input_frames:
                        break

                    # Thread safe
                    with shared.mutex:

                        # Process incoming
                        for msg in input_frames:
                            SharedTableBroker.__incomingMsg(shared, msg)


                # Remove broker ip from list
                debug.trace("Broker disconnection", shared.client.remote_ip)

                # Thread safe
                with shared.mutex:

                    # Clean data
                    SharedTableBroker.__removeAllClientEntries(shared, broker_id)
                    shared.table_data_old = shared.table_data
                    shared.table_data = {}
                    if shared.discovery_daemon:
                        shared.discovery_daemon.setEnable(True)


                # Program old data remove
                threading.Thread(target=SharedTableBroker.__cleanOldEntries, daemon=True, args=[shared]).start()


    def __removeAllClientEntries(shared, client_id, entry_key=""):

        with shared.mutex:

            keys_to_remove = []
            keys_to_remove_old = []
            if client_id not in shared.table_data:
                return

            for entry in shared.table_data[client_id]:
                if entry == entry_key or entry_key == "":
                    keys_to_remove.append(entry)

            if client_id in shared.table_data_old:
                for entry in shared.table_data_old[client_id]:
                    if entry not in keys_to_remove and entry == entry_key or entry_key == "":
                        keys_to_remove_old.append(entry)


            for entry in keys_to_remove:
                del shared.table_data[client_id][entry]
                debug.trace("[Remove entry ", entry, "]")
                if shared.on_remove_table_entry:
                    shared.on_remove_table_entry(client_id, entry)

            for entry in keys_to_remove_old:
                del shared.table_data_old[client_id][entry]
                debug.trace("[Remove old entry ", entry, "]")
                if shared.on_remove_table_entry:
                    shared.on_remove_table_entry(client_id, entry)


    def __incomingMsg(shared, input_data):

        try:
            input_dict = json.loads(input_data)
        except:
            debug.trace("Input error", input_data)
            return


        # Remove all client entries
        if len(input_dict) == 1:
            SharedTableBroker.__removeAllClientEntries(shared, input_dict[0])


        # Remove client key
        elif len(input_dict) == 2:
            SharedTableBroker.__removeAllClientEntries(shared, input_dict[0], input_dict[1])


        # Add / update entry
        elif len(input_dict) >= 4:
            client_id = input_dict[0]
            entry_key = input_dict[1]
            is_master = input_dict[2]
            data = input_dict[3:]

            if client_id not in shared.table_data:
                shared.table_data[client_id] = {}

            if entry_key in shared.table_data[client_id]:

                # Update topic
                shared.table_data[client_id][entry_key] = [data, is_master]

                debug.trace("[Topic updated", entry_key, "]")
                if shared.on_update_table_entry:
                    shared.on_update_table_entry(client_id, entry_key, data)


            else:

                # Add new topic
                shared.table_data[client_id][entry_key] = [data, is_master]

                if client_id in shared.table_data_old and entry_key in shared.table_data_old[client_id]:
                    debug.trace("[Topic restored", entry_key, "]")
                    if shared.on_update_table_entry:
                        shared.on_update_table_entry(client_id, entry_key, data)

                else:
                    debug.trace("[Topic new", entry_key, "]")
                    if shared.on_new_table_entry:
                        shared.on_new_table_entry(client_id, entry_key, data)


        else:
            debug.trace("Ignored", input_dict)


    def __updateTableEntry(shared, instance_key:str, entry:list, save_restore) -> bool:

        data_list = []

        # Position 1
        data_list.append(shared.client_id)

        # Position 2
        data_list.append(instance_key)
        
        # Position 3
        data_list.append(False if not shared.discovery_daemon else shared.discovery_daemon.isMaster())

        # Position 4 + n
        data_list += entry
        raw = json.dumps(data_list)

        if save_restore:
            with shared.mutex:
                shared.client_table_data[instance_key] = entry

        return shared.client.send(bytearray(raw, encoding="utf8") + constants.END_OF_TX)


    def stop(self):

        self.__master_container.run = False
        self.__client_container.run = False

        with self.__master_container.mutex:
            for client in self.__master_container.master_clients:
                client.close()

        if self.__master_container.master_listener:
            self.__master_container.master_listener.close()

        with self.__client_container.mutex:
            if self.__client_container.client:
                self.__client_container.client.close()

        if self.__master_container.discovery_daemon:
            self.__master_container.discovery_daemon.stop()

        if self.__broker_thread:
            self.__broker_thread.join()

        self.__client_thread.join()


    def updateTableEntry(self, instance_key:str, entry:list) -> bool:

        return SharedTableBroker.__updateTableEntry(self.__client_container, instance_key, entry, True)


    def geTableData(self) -> list:
        with self.__client_container.mutex:
            table_data = []
            added_entries = []

            for client_id in self.__client_container.table_data:
                for entry_key in self.__client_container.table_data[client_id]:
                    row = [client_id, entry_key]
                    row += self.__client_container.table_data[client_id][entry_key][0]
                    table_data.append(row)
                    added_entries.append(client_id+entry_key)

            for client_id in self.__client_container.table_data_old:
                for entry_key in self.__client_container.table_data_old[client_id]:
                    if client_id+entry_key not in added_entries:
                        row = [client_id, entry_key]
                        row += self.__client_container.table_data_old[client_id][entry_key][0]
                        table_data.append(row)

        return table_data


    def geMapData(self) -> dict:
        with self.__client_container.mutex:
            table_map = {}
            added_entries = []

            for client_id in self.__client_container.table_data:
                table_map[client_id] = {}
                for entry_key in self.__client_container.table_data[client_id]:
                    table_map[client_id][entry_key] = self.__client_container.table_data[client_id][entry_key][0]
                    added_entries.append(client_id+entry_key)

            for client_id in self.__client_container.table_data_old:
                for entry_key in self.__client_container.table_data_old[client_id]:
                    if client_id + entry_key not in added_entries:
                        table_map[client_id][entry_key] = self.__client_container.table_data_old[client_id][entry_key][0]


    def isMaster(self) -> bool:
        return self.__master_container.discovery_daemon and self.__master_container.discovery_daemon.isMaster()


    @property
    def clientID(self):
        self.__client_container.client_id


    @property
    def onNewTableEntry(self):
        with self.__client_container.mutex:
            return self.__client_container.on_new_table_entry


    @onNewTableEntry.setter
    def onNewTableEntry(self, callback):
        with self.__client_container.mutex:
            self.__client_container.on_new_table_entry = callback


    @property
    def onUpdateTableEntry(self):
        with self.__client_container.mutex:
            return self.__client_container.on_update_table_entry


    @onUpdateTableEntry.setter
    def onUpdateTableEntry(self, callback):
        with self.__client_container.mutex:
            self.__client_container.on_update_table_entry = callback


    @property
    def onRemoveTableEntry(self):
        with self.__client_container.mutex:
            return self.__client_container.on_remove_table_entry


    @onRemoveTableEntry.setter
    def onRemoveTableEntry(self, callback):
        with self.__client_container.mutex:
            self.__client_container.on_remove_table_entry = callback

