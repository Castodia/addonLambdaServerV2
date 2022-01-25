from db_psycopg2 import db_pg
from CastodiaErrorClas import *
import json


def buildResponse(statusCode, message):
    responseObject = {}
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['statusCode'] = statusCode
    responseObject['body'] = json.dumps(
        {"data": message}, default=str)
    return responseObject


def get_active_account(user_id):
    sql = 'SELECT active FROM owner WHERE id =%s;'
    data = (user_id,)
    res = db_pg.query_get(sql, data)
    if not res:
        raise ResourceGetError('user')
    return res


def verifyCustomQuery(query):

    dangerWords = ["insertinto", "deletefrom", "createschema", "dropschema", "alterschema", "createdatabase", "alterdatabase", "dropdatabase",
                   "createtable", "altertable", "droptable", "createtemptable", "truncatetable", "renameto", "addcolumn", "dropcolumn", "altercolumn", "renamecolumn"]

    queryTrimmed = query.lower().replace(" ", "")

    for word in dangerWords:
        if word in queryTrimmed:
            return False

    queryList = query.lower().split()
    if 'update' in queryList:
        loc = queryList.index('update')
        for v in queryList[loc+1:]:
            if v.lower() == 'set':
                return False

    return True



def addCustQuery(qVersion, qText, qOptions):
    sql = 'INSERT INTO custQueryV1 (qText, qOptions) VALUES (%s,%s,%s) RETURNING qid;'
    data = (qVersion, qText, json.dumps(qOptions, default=str))
    res = db_pg.query_get(sql, data)
    if not res:
        raise ResourceCreationError('custom query')
    return res['qid']

def addQueryRouter(user_id, qTableName, QID, qVersion, qName, qDescr, database_id):
    sql = 'SELECT id FROM databases WHERE user_id = %s AND id = %s AND active = TRUE;'
    data = (user_id, database_id)
    res = db_pg.query_get(sql, data)
    if not res:
        raise ResourceGetError('database')
    # return res
    query_router_id = _addQueryRouter(
        user_id, qTableName, QID, qVersion, qName, qDescr, database_id)
    return query_router_id

def _addQueryRouter(user_id, qTableName, QID, qVersion, qName, qDescr, database_id):
    sql = '''
        INSERT INTO queryRouter (user_id, qTableName, QID, qVersion, qName, qDescr, dbrouterid, active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
    '''
    data = (user_id, qTableName, QID, qVersion,
            qName, qDescr, database_id, True)
    res = db_pg.query_get(sql, data)
    if not res:
        raise ResourceCreationError('query router')
    return res['id']


def get_saved_queries_by_workspace_id(user_id, workspace_id):
    sql = '''SELECT cst.*, d.nickname FROM custqueryv2 cst 
    INNER JOIN databases d 
    ON cst.database_id = d.id
    WHERE cst.user_id = %s AND cst.workspace_id = %s'''
    data = (user_id, workspace_id)
    res = db_pg.query_get(sql, data, fetch_all=True)

    print(res)
    # if not res:
    #     raise ResourceGetError('custom query')
    return res


def addCustQuery2(user_id, database_id, workspace_id, query_name, query_text, query_description, qVars, qOptions):
    sql = '''INSERT INTO custQueryV2
        (user_id, database_id, workspace_id, query_name, query_text, query_description, qvars, qoptions)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
        RETURNING id
    '''
    data = (
        user_id,
        database_id,
        workspace_id,
        query_name,
        query_text,
        query_description,
        json.dumps(qVars, default=str),
        json.dumps(qOptions, default=str)
    )
    res = db_pg.query_get(sql, data)
    if not res:
        raise ResourceCreationError('custom query')
    return res['id']

def updateCustQuery2(user_id, database_id, workspace_id, query_name, query_text, query_description, qVars):
    sql = '''UPDATE custQueryV2 SET 
            query_name = %s AND query_text = %s AND query_description = %s AND qvars = %s 
            WHERE user_id = %s AND database_id = %s AND workspace_id = %s
        '''
    data = (query_name, query_text, query_description, qVars, user_id, database_id, workspace_id)
    res = db_pg.query_run(sql, data)
    if not res:
        raise ResourceUpdateError('custom query')
    return res