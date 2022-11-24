from queue import Queue
from threading import Thread
import time
import sys
import psycopg2
import json
import tweepy
from kafka import KafkaConsumer

import configparser


def worker_credentials(worker_id):
    """Retrieves api credentials by id from config file and return and api entrypoint object"""

    worker = f"worker_{worker_id}"
    parser = configparser.ConfigParser()
    parser.read("./app/auth.cfg")

    CONSUMER_KEY = parser.get(worker, "consumer_key")
    CONSUMER_SECRET = parser.get(worker, "consumer_secret")
    ACCESS_TOKEN = parser.get(worker, "access_token")
    ACCESS_TOKEN_SECRET = parser.get(worker, "access_token_secret")

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    return tweepy.API(auth,
                      wait_on_rate_limit=True)


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn


def api_call(q, api, i):
    """ Each worker (thread) execute this function. It receives the main queue, an api object and  a worker id"""
    while True:

        #Here we implement all the functionality. A typical pattern consist of decoding the message which contains the parameters for the function. 

        # first it processes the message. 
        msg = q.get()
        split_msg = msg.split("-")
        id_user, party = int(split_msg[0]), split_msg[1]


        # some api call and database updates. 
        try:
            friends = api.get_friend_ids(user_id=id_user)
            data = {}
            data[id_user] = friends
            with open("./data/{}/friends/{}.json".format(party, id_user), "w") as file:
                json.dump(data, file)

            conn = connect(param_dic)
            cursor = conn.cursor()
            cursor.execute(
                '''UPDATE labeled_data_test SET status = 'done' WHERE id_user = '{}';'''.format(id_user))
            conn.commit()

            print("WORKER{}: ".format(i), id_user, " --> ",
                  len(friends), " Party: {}".format(party))
            time.sleep(70)

        except Exception as err:

            conn = connect(param_dic)
            cursor = conn.cursor()
            cursor.execute('''UPDATE labeled_data_test SET status = '{}' WHERE id_user = '{}';'''.format(
                type(err).__name__, id_user))
            conn.commit()

            print("WORKER{}: ".format(i), id_user, " -->  Error")
            print(f"{type(err).__name__} was raised: {err}")
            time.sleep(70)

        finally:
            # commit each message/task
            q.task_done()


if __name__ == "__main__":

    param_dic = {
        "host": "localhost",
        "user": "postgres",
        "password": "admin",
        "port": 5433
    }

    #here we specify the number of api accounts we are going to use.  
    num_workers = 5
    apis = [worker_credentials(i) for i in range(num_workers)]

    q = Queue(maxsize=0)

    for i in range(num_workers):
        #we run thread, each one uses an api account object. All of them work with the same queue. 
        api = apis[i]
        worker = Thread(target=api_call, args=(q, api, i))
        worker.setDaemon(True)
        worker.start()



    #in this implementation I use a kafka queue to feed the other queue. Instead of this you can load tasks from a file, database, etc. 
    consumer = KafkaConsumer("test", bootstrap_servers=[
        "localhost:29092"], value_deserializer=lambda m: m.decode('ascii'))

    for msg in consumer:
        q.put(msg.value)

    q.join()
