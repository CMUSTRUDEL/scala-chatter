import mysql.connector

PW_FILE = '../../../.pw'
DB_USER = 'samyong'
DB_NAME = 'scala_chatter'

pw = [s for s in open(PW_FILE)][0].strip()
db = mysql.connector.connect(
        host='localhost',
        user=DB_USER,
        password=pw,
        database=DB_NAME,
        )

def execute(query, has_res = True):
    cursor = db.cursor()
    cursor.execute(query)
    if not has_res:
        return
    res = cursor.fetchall()
    return res

