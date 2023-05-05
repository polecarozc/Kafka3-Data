import sqlite3

conn = sqlite3.connect('transactions.db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE transaction1 (id Integer Primary key,custid Integer,type Text,date Integer,amt Integer)''')
conn.close()