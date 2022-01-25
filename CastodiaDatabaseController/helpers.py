import json
from CastodiaErrorClass import *
from db_psycopg2 import db_pg, DB_postgres
import db_psycopg2 as access_cast

# for cryptography
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


DECRYPTED = "48e9fad0-d2c3-4bb6-9892-de8844424d9cb5fff144-f9c8-4ed2-9855-9b8bce2f056c"


def createFernet(owner):
    cstPassword = bytes(DECRYPTED, "utf-8")
    salt = bytes((owner), "utf-8")
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(cstPassword))
    f = Fernet(key)
    return f


def buildResponse(statusCode, message):
    responseObject = {}
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['statusCode'] = statusCode
    responseObject['body'] = json.dumps(
        {"data": message}, default=str)
    return responseObject


def add_workspace_database(user_id, dbConnectInfo, workspace_id):
    creds = encrpyt_creds(user_id, dbConnectInfo)
    db_type = dbConnectInfo["dbType"]
    nickname = dbConnectInfo['nickname']
    database_id = _add_workspace_database(user_id, creds, nickname, db_type, workspace_id)
    return database_id

def _add_workspace_database(user_id, creds, nickname, db_type, workspace_id):
    sql = "INSERT INTO databases (user_id, nickname, creds, dbtype, workspace_id) VALUES (%s,%s,%s,%s,%s) RETURNING id;"
    data = (user_id, nickname, creds, db_type, workspace_id)
    res = db_pg.query_get(sql, data)
    if not res:
        raise ResourceCreationError('database')
    return res

    
def get_all_databases_names(workspace_id, user_id):
    sql = 'SELECT id, db_name, nickname FROM databases WHERE user_id = %s AND workspace_id = %s;'
    data = (user_id, workspace_id)
    res = db_pg.query_get(sql, data, fetch_all=True)
    # if not res:
    #     raise ResourceGetError('databases')
    return res


def get_database_creds(workspace_id, user_id, database_id):
    encrpyed_creds = _get_database_creds(workspace_id, user_id, database_id)
    decrypted_creds = decrypt_creds(encrpyed_creds, user_id)
    return decrypted_creds


def _get_database_creds(workspace_id, user_id, database_id):
    sql = 'SELECT creds FROM databases WHERE user_id = %s AND workspace_id = %s AND id = %s;'
    data = (user_id, workspace_id, database_id)
    res = db_pg.query_get(sql, data)
    if not res:
        raise ResourceGetError('database')
    return res
    
def update_workspace_database(user_id, workspace_id, database_id, dbConnectInfo):
    creds = encrpyt_creds(user_id, dbConnectInfo)
    print(creds)
    _update_workspace_database(user_id, workspace_id, database_id, creds, dbConnectInfo['dbType'], dbConnectInfo['nickname'])


def _update_workspace_database(user_id, workspace_id, database_id, creds, db_type, nickname):
    sql = '''
    UPDATE databases SET (creds, dbtype, nickname) = (%s, %s, %s)
    WHERE id = %s AND workspace_id = %s AND user_id = %s RETURNING id;'''
    data = (creds, db_type, nickname, database_id, workspace_id, user_id)
    # print(data)
    res = db_pg.query_get(sql, data)
    if not res:
        raise ResourceUpdateError('database')
    return res


def delete_workspace_database(user_id, workspace_id, database_id):
    sql = 'DELETE FROM databases WHERE user_id = %s AND workspace_id = %s AND id = %s;'
    data = (user_id, workspace_id, database_id)
    res = db_pg.query_run(sql, data)
    if not res:
        raise ResourceDeleteError('database')
    return res

def decrypt_creds(creds, user_id):
    f = createFernet(user_id)
    return json.loads(f.decrypt(bytes(creds['creds'])).decode("utf-8"))


def encrpyt_creds(user_id, dbConnectInfo):
    f = createFernet(user_id)
    message = json.dumps(dbConnectInfo)
    return f.encrypt(bytes(message, "utf-8"))

def test_database_connection(dbConnectInfo):
    if dbConnectInfo['dbType'] == 'postgres':
        ## Overwrite default creds
        access_cast.castConnectInfo = {
            "dbHost": dbConnectInfo['dbHost'],
            "dbName": dbConnectInfo['dbName'],
            "dbPort": dbConnectInfo['dbPort'],
            "dbUser": dbConnectInfo['dbUser'],
            "dbPass": dbConnectInfo['dbPass'],
        }
        ## Create a new db_class
        db_pg_test = DB_postgres()
        sql = 'select * from information_schema.tables limit 1;'
        data = tuple()
        res = db_pg_test.query_get(sql, data)
        if not res:
            raise ConnectionError()
        return ''
    else:
        pass