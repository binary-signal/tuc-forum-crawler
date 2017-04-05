import config
import argparse
from mongo import MDB
from time import sleep

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='simple stat app for mongo db')
    parser.add_argument('-t', '--time', type=int, help='time interval update')
    args = parser.parse_args()

    db = MDB(port=config.db_port, host=config.db_host, dbname=config.db_name,
             dbcollection=config.db_collection)
    if db is None:
        print('Can\'t connect to database')
        exit(-1)

    last_count = 0
    cur_count = 0
    max_rate = 0
    start = True
    t = args.time
    width = 15

    print("\n\n{:{align}{width}}".format("*Mongo DB stats*", align='^',
                                         width=width * 3), end='\n\n')
    print(
        '{:{align}{width}}{:{align}{width}}{:{align}{width}}'.format('Num docs',
                                                                     'posts/s',
                                                                     'max',
                                                                     align='>',
                                                                     width=width))
    print('-' * width * 3)
    try:
        while True:
            cur_count = db.num_of_docs()
            rate = int((cur_count - last_count) / t)
            last_count = cur_count
            if start:  # skip first comparison of rate > max_rate
                start = False
                continue

            if rate > max_rate:
                max_rate = rate

            print('{:{align}{width}}{:{align}{width}}{:{align}{width}}'.format(
                cur_count, rate, max_rate, align='>',
                width=width), end='\r')
            sleep(t)
    except KeyboardInterrupt:
        print("Keyboard interrupt received, stopping ...")
    finally:
        db.destroy()
        exit(0)
