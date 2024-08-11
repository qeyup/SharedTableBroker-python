#
# This file is part of the DynamicMessagingBroker distribution.
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


version = "0.1.0"


class constants():

    DEFAULT_DOMAIN = "DYNAMIC_MESSAGING_BROKER_DOMAIN"
    MQTT_BROKER_PORT = 1883
    BROKER_PORT = 18832
    CLIENT_DISCOVER_WAIT = 5
    MTU = 4096
    END_OF_TX = b'\xFF'
    MAX_LISTEN_TCP_SOKETS = -1


class container():
    pass


class udpRandomPortListener():

    def __init__(self):
        super().__init__()
        self.__open = True
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__sock.bind(('', 0))
        self.__sock.settimeout(0.1)


    def __del__(self):
        self.close()


    def read(self, timeout=-1):

        current_epoch_time = int(time.time())
        while self.__open:
            try:
                data, (ip, port) = self.__sock.recvfrom(constants.MTU)
                return data, ip, port

            except socket.timeout:
                if timeout >= 0 and int(time.time()) - current_epoch_time >= timeout:
                    return None, None, None

            except socket.error:
                return None, None, None

        return None, None, None


    def send(self, ip, port, msg):
        if isinstance(msg, str):
            msg = msg.encode()

        chn_msg = [msg[idx : idx + constants.MTU] for idx in range(0, len(msg), constants.MTU)]

        for chn in chn_msg:
            self.__sock.sendto(chn, (ip, port))


    @property
    def port(self):
        return self.__sock.getsockname()[1]


    def close(self):
        self.__open = False
        self.__sock.close()


class udpClient():
    def __init__(self, ip, port):
        self.__open = True
        self.__remote_ip = ip
        self.__remote_port = port
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__sock.settimeout(0.1)


    def __del__(self):
        self.close()


    def read(self, timeout=-1):

        current_epoch_time = int(time.time())
        while self.__open:
            try:
                data = self.__sock.recv(constants.MTU)
                return data

            except socket.timeout:
                if timeout >= 0 and int(time.time()) - current_epoch_time >= timeout:
                    return None

            except socket.error:
                return None

        return None


    def send(self, msg):
        if isinstance(msg, str):
            msg = msg.encode()

        chn_msg = [msg[idx : idx + constants.MTU] for idx in range(0, len(msg), constants.MTU)]

        for chn in chn_msg:
            self.__sock.sendto(chn, (self.__remote_ip, self.__remote_port))


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
        self.__sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
                if timeout >= 0 and int(time.time()) - current_epoch_time >= timeout:
                    return None

            except socket.error:
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
                    return data

                except socket.timeout:
                    if timeout >= 0 and int(time.time()) - current_epoch_time >= timeout:
                        return None

                except socket.error:
                    self.close()
                    if not self.__connect():
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


class DynamicMessagingBroker():

    def __init__(self, domain=constants.DEFAULT_DOMAIN, master=True):

        self.__domain = domain
        self.__discovery_daemon = ServiceDiscovery.daemon(self.__domain) if master else None
        self.__discovery_client = ServiceDiscovery.client()
        self.__master_listener = tcpListener(constants.BROKER_PORT)
        self.__client = None
        self.__master_clients = []
        self.__broker_data = {}
        self.__broker_own_topics = []
        self.__on_new_topic = None
        self.__on_update_topic = None
        self.__on_remove_topic = None
        self.__mutex = threading.RLock()
        self.__start_mutex = threading.Lock()
        self.__start_mutex.acquire()

        # Launch broker master
        if master:
            self.__discovery_daemon.run(True)
            threading.Thread(target=DynamicMessagingBroker.__masterDaemonThread, daemon=True, args=[weakref.ref(self)]).start()

        # Launch broker client
        threading.Thread(target=DynamicMessagingBroker.__clientDaemonThread, daemon=True, args=[weakref.ref(self)]).start()

        # Wait first conection
        self.__start_mutex.acquire()


    def __del__(self):

        self.__master_listener.close()
        with self.__mutex:
            if self.__client:
                self.__client.close()


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
            else:
                print("not all")

        return read_buffer[:-1].split(constants.END_OF_TX)


    def __masterDaemonThread(weak_self):
        while True:
            shared_self = weak_self()
            if not shared_self:
                break

            # wait new client connection
            connection = shared_self.__master_listener.waitConnection()
            threading.Thread(target=DynamicMessagingBroker.__connectionThread, daemon=True, args=[weak_self, connection]).start()


    def __connectionThread(weak_self, connection):

        # Check valid connection
        if not connection:
            return


        # Get instance
        shared_self = weak_self()
        if not shared_self:
            return


        # Send current DB
        with shared_self.__mutex:
            print("New client", connection.clientIp, "send", len(shared_self.__broker_data))

            # Send all current info to new client
            all_db_send = True

            for topic in shared_self.__broker_data:
                master_topic = True if topic in shared_self.__broker_own_topics else False
                msg = json.dumps([topic, shared_self.__broker_data[topic], master_topic])

                if not connection.send(bytearray(msg, encoding='utf8') + constants.END_OF_TX):
                    print("Client error")
                    all_db_send = False
                    break

                print("--- >", topic, connection.clientIp)


            # Start listen thread
            if all_db_send:
                shared_self.__master_clients.append(connection)

            else:
                print("Client closed")
                connection.close()
                return


        # Read loop
        clients_topics = []
        while connection.isConnected():
            shared_self = weak_self()
            if not shared_self:
                break

            # Wait client incoming
            input_frames = DynamicMessagingBroker.__readFrames(connection)
            if not input_frames:
                break

            clients_with_errors = []
            for msg in input_frames:

                # Get message topic:
                try:
                    input_dict = json.loads(msg)
                    topic = input_dict[0]
                    if topic not in clients_topics:
                        clients_topics.append(topic)

                except:
                    print("Message ignored")
                    continue

                # Propagate to other clients (included sender client and own client)
                with shared_self.__mutex:
                    for client in shared_self.__master_clients:
                        if not client.send(msg + constants.END_OF_TX):
                            clients_with_errors.append(client)
                            print("Client error 2")
                            continue


            # Remove clients with errors
            with shared_self.__mutex:
                for client in clients_with_errors:
                    client.close()


        # Notify client discontion
        with shared_self.__mutex:
            shared_self.__master_clients.remove(connection)

            for topic in clients_topics:
                if topic in shared_self.__broker_data:
                    del shared_self.__broker_data[topic]

                    for client in shared_self.__master_clients:
                        raw = json.dumps([topic])
                        if not client.send(bytearray(raw, encoding='utf8') + constants.END_OF_TX):
                            print("Client error")

        shared_self.__printDB()


    def __clientDaemonThread(weak_self):
        while True:
            shared_self = weak_self()
            if not shared_self:
                break


            # Discover master
            master_ip = shared_self.__discovery_client.getServiceIP(shared_self.__domain, timeout=constants.CLIENT_DISCOVER_WAIT)
            print("Found broker", master_ip)
            if master_ip != "":

                # Connect to master
                with shared_self.__mutex:
                    shared_self.__client = tcpClient(master_ip, constants.BROKER_PORT)
                    shared_self.__client.connect()

                    if shared_self.__client.connected:
                        if not shared_self.__discovery_daemon.isMaster():
                            shared_self.__broker_data = {}

                        if shared_self.__start_mutex.locked():
                            shared_self.__start_mutex.release()

                broker_topics = []


                # Read loop
                print("Start client")
                while shared_self.__client.connected:

                    input_frames = DynamicMessagingBroker.__readFrames(shared_self.__client)
                    if not input_frames:
                        break

                    for msg in input_frames:

                        # Get broker topic
                        try:
                            input_dict = json.loads(msg)
                            topic = input_dict[0]
                            isMaster = True if len(input_dict) >= 3 and bool(input_dict[2]) else False
                            if isMaster and topic not in broker_topics:
                                broker_topics.append(topic)

                        except:
                            continue


                        # Process incoming
                        DynamicMessagingBroker.__incomingMsg(shared_self, msg)


                # Remove broker ip from list
                print("Broker disconnection", shared_self.__client.remote_ip)
                with shared_self.__mutex:
                    for topic in broker_topics:
                        if topic in shared_self.__broker_data:
                            del shared_self.__broker_data[topic]

                shared_self.__printDB()


    def __printDB(self):

        with self.__mutex:
            print("----------------")
            for topic in self.__broker_data:
                print("#", topic, self.__broker_data[topic].replace("\n", "").replace(" ", ""))
            print("----------------")


    def __incomingMsg(shared_self, input_data):

        try:
            input_dict = json.loads(input_data)

            topic = input_dict[0]

            print("<---", topic, input_data)

            with shared_self.__mutex:

                if len(input_dict) == 1:

                    if topic in shared_self.__broker_data:


                        # Remove topic
                        del shared_self.__broker_data[topic]

                        print("[Topic removed ", topic, "]")
                        if shared_self.__on_update_topic:
                            shared_self.__on_remove_topic(topic)

                        shared_self.__printDB()


                else:
                    data = json.loads(input_dict[1])
                    if isinstance(data, dict):

                        if topic in shared_self.__broker_data:

                            # Update topic
                            shared_self.__broker_data[topic] = input_dict[1]

                            print("[Topic updated", topic, "]")
                            if shared_self.__on_update_topic:
                                shared_self.__on_update_topic(topic, data)

                            shared_self.__printDB()


                        else:

                            # Add new topic
                            shared_self.__broker_data[topic] = input_dict[1]

                            print("[Topic new", topic, "]")
                            if shared_self.__on_update_topic:
                                shared_self.__on_new_topic(topic, data)


                            shared_self.__printDB()

        except:
            print("Error!", input_data)
            pass


    def publishData(self, topic, msg=None):
        with self.__mutex:
            if self.__client:
                topic = topic.replace("publish_command_example/command/example/command_example1", "")
                if msg:
                    msg="{}"
                    raw = json.dumps([topic, msg, self.__discovery_daemon.isMaster()])

                    if topic not in self.__broker_own_topics:
                        self.__broker_own_topics.append(topic)

                else:
                    raw = json.dumps([topic])

                    if topic in self.__broker_own_topics:
                        self.__broker_own_topics.remove(topic)

                return self.__client.send(bytearray(raw, encoding="utf8") + constants.END_OF_TX)

        return False


    @property
    def onNewTopic(self):
        with self.__mutex:
            return self.__on_new_topic

    @onNewTopic.setter
    def onNewTopic(self, callback):
        with self.__mutex:
            self.__on_new_topic = callback


    @property
    def onUpdateTopic(self):
        with self.__mutex:
            return self.__on_update_topic


    @onUpdateTopic.setter
    def onUpdateTopic(self, callback):
        with self.__mutex:
            self.__on_update_topic = callback


    @property
    def onRemoveTopic(self):
        with self.__mutex:
            return self.__on_remove_topic


    @onRemoveTopic.setter
    def onRemoveTopic(self, callback):
        with self.__mutex:
            self.__on_remove_topic = callback

