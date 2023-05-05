from kafka import KafkaConsumer, TopicPartition
from json import loads
import numpy as np

class XactionConsumer:
    def __init__(self, limit):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        #Go back to the readme.
        self.limit = limit

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            if message['amt'] <= self.limit:
                print(f'VALID TRANSACTION')
            elif message['amt'] > self.limit:
                print(f'ERROR! TRANSACTION PASSED LIMIT FOR {message["custid"]}')
                break


if __name__ == "__main__":
    c = XactionConsumer(5000)
    c.handleMessages()