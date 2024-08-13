import sys
sys.path.append('../SharedTableBroker/')
sys.path.append('.')

import time
import argparse
import SharedTableBroker



def main():

    parser = argparse.ArgumentParser(description="Dynamic messaging broker example")
    parser.add_argument(
        "key",
        nargs=1,
        help="Entry key",
        type=str)
    parser.add_argument(
        "data",
        nargs=1,
        help="entry data",
        type=str)
    args = parser.parse_args(sys.argv[1:])


    table_broker = SharedTableBroker.SharedTableBroker()
    table_broker.updateTableEntry(args.key[0], args.data)

    last_table = {}
    while True:

        table = table_broker.geTableData()
        if table != last_table:
            last_table = table
            print("--- " + str(len(table)) + " -------------------")
            for entry in table:
                print(entry)
            print("-------------------------")

        time.sleep(1)


if __name__ == '__main__':
    main()