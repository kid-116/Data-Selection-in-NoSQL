import pandas as pd
import mysql.connector
import os
import sys
import pymongo
from dotenv import load_dotenv

load_dotenv()

def load(folder):
    users =  pd.read_csv(f'output/{folder}/users.csv', sep='\t')
    comments = pd.read_csv(f'output/{folder}/comments.csv', sep='\t')
    return users, comments

def sql_insert(users, comments):
    def insert(query, items, conn, cursor):
        GROUPS = 10
        total = items.shape[0]
        group = total // GROUPS
        items = list(items.itertuples(name=None, index=None))
        for i in range(GROUPS):
            cursor.executemany(query, items[group * i : group * (i + 1)])
            conn.commit()
            print('#', end='')
        print()

    def delete(table, cursor, conn, col):
        GROUPS = 10
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        total = cursor.fetchall()[0][0]
        if(total < 100000):
            cursor.execute(f'DELETE FROM {table};')
            return
        group = total // GROUPS
        for i in range(GROUPS):
            cursor.execute(f'DELETE FROM {table} WHERE {col} <= {(i + 1) * group};')
            conn.commit()

    try:
        conn = mysql.connector.connect(
            host='localhost',
            database='social',
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASS'),
            auth_plugin='mysql_native_password'
        )
        cursor = conn.cursor(buffered=True)
        # Deletion
        print("Deleting old records")
        delete('comment', cursor, conn, 'comment_id')
        delete('user', cursor, conn, 'user_id')
        # Optimization
        cursor.execute('OPTIMIZE TABLE user;')
        cursor.execute('OPTIMIZE TABLE comment;')
        # Insertion
        # user
        print("Inserting users")
        query = "INSERT INTO user VALUES(%s, %s, %s, %s)"
        insert(query, users, conn, cursor)
        # comment
        print("Inserting comments")
        query = "INSERT INTO comment VALUES(%s, %s, %s, %s)"
        insert(query, comments, conn, cursor)
        conn.close()
    except mysql.connector.Error as error:
        print(f"Failed to connect to MySQL: {error}")

def mongo_insert(users, comments):
    def insert(col, items):
        GROUPS = 20
        total = len(items)
        group = total // GROUPS
        for i in range(GROUPS):
            col.insert_many(list(items[group * i : group * (i + 1)].T.to_dict().values()))
            print('#', end='')
        print()

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['test']

    user_col = db['user']
    comment_col = db['comment']

    users = users.rename(columns={'user_id': '_id'})
    comments = comments.rename(columns={'comment_id': '_id'})

    print("Deleting old records")
    user_col.delete_many({})
    comment_col.delete_many({})

    print("Inserting users")
    insert(user_col, users)
    print("Inserting comments")
    insert(comment_col, comments)

    users = users.rename(columns={'_id': 'user_id'})
    comments = comments.rename(columns={'_id': 'comment_id'})

def head(title):
    print()
    print(title)
    print('----------')

def ins(folder):
    print(f"Loading {folder}")
    users, comments = load(folder)

    head('MySQL')
    sql_insert(users, comments)

    head('MongoDB')
    mongo_insert(users, comments)