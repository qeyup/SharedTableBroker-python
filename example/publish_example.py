import sys
sys.path.append('../DynamicMessagingBroker/')
sys.path.append('.')

import time
import argparse
import DynamicMessagingBroker



def main():

    parser = argparse.ArgumentParser(description="Dynamic messaging broker example")
    parser.add_argument(
        "topic",
        nargs=1,
        help="Topic name",
        type=str)
    parser.add_argument(
        "payload",
        nargs=1,
        help="message payload",
        type=str)
    args = parser.parse_args(sys.argv[1:])


    dynamic_broker = DynamicMessagingBroker.d2d()
    dynamic_broker.publishData(args.topic, args.payload)


    while True:
        time.sleep(10)




if __name__ == '__main__':
    main()