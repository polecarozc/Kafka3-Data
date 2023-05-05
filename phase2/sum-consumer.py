from kafka import KafkaConsumer, TopicPartition
from json import loads
import numpy as np

class XactionConsumer:
    def __init__(self):
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
        self.deposits = []
        self.withdraws = []
    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            if message['type'] == 'dep':
                self.deposits.append(message['amt'])
                print(f'There was a deposit for {message["amt"]} ')
                print(f'Average for deposits: {np.mean(self.deposits)}')
                print(f'Standard deviation for deposits: {np.std(self.deposits)}')
            else:
                self.withdraws.append(message['amt'])
                print(f'There was a withdraw for {message["amt"]} ')
                print(f'Average for withdraws: {np.mean(self.withdraws)}')
                print(f'Standard deviation for withdraws: {np.std(self.withdraws)}')
if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()