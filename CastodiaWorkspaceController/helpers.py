import json
import psycopg2
import psycopg2.extras
import datetime
from CastodiaErrorClass import *
from db_psycopg2 import db_pg

def buildResponse(statusCode, message):
    responseObject = {}
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['statusCode'] = statusCode
    responseObject['body'] = json.dumps(
        {"data": message}, default=str)
    return responseObject

def _create_workspace(name, description):
    sql = 'INSERT INTO workspace (name, description) VALUES (%s, %s) RETURNING id;'
    data = (name, description)
    res = db_pg.query_get(sql, data)

    if not res:
        return buildResponse(400, f'Cannot create workspace')
    return res['id']
        
        

def _add_user_to_workspace(user_id, workspace_id, role):
    sql = 'INSERT INTO user_space_role (user_id, workspace_id, role) VALUES (%s, %s, %s) RETURNING id;'
    data = (user_id, workspace_id, role)
    res = db_pg.query_get(sql, data)

    if not res:
        return buildResponse(400, f'Cannot add member to workspace')
    return res['id']


def _delete_workspace(workspace_id):
    sql = 'DELETE FROM workspace WHERE id = %s;'
    data = (workspace_id, )
    res = db_pg.query_run(sql, data)

    if not res:
        return buildResponse(400, f'Error deleting workspace')
    return res
        

def _delete_databases(workspace_id):
    sql = 'DELETE FROM databases WHERE workspace_id = %s;'
    data = (workspace_id, )
    res = db_pg.query_run(sql, data)
    if not res:
        raise ResourceDeleteError('databases')
    return res

def _delete_workspace_members(workspace_id):
    sql = 'DELETE FROM user_space_role WHERE workspace_id = %s;'
    data = (workspace_id, )
    res = db_pg.query_run(sql, data)

    if not res:
        return buildResponse(400, f'Error deleting members')
    return res

def _update_workspace(workspace_id, description):
    sql = 'UPDATE workspace SET description = %s where id = %s;'
    data = (description, workspace_id)
    res = db_pg.query_run(sql, data)
    
    if not res:
        return buildResponse(400, f'Error updating workspace')
    return res
        

def _add_new_user(email):
    sql = "INSERT INTO owner (email) VALUES (%s) RETURNING id;"
    data = (email, )
    res = db_pg.query_get(sql, data)

    if not res:
        return buildResponse(400, f'Error adding user')
    return res['id']

def _delete_member(email, workspace_id):
    # sql = '''' DELETE FROM user_space_role WHERE id = 
    #     (SELECT id FROM owner WHERE email = %s) 
    #     '''
    sql = 'DELETE FROM user_space_role WHERE user_id = %s;'
    data = (email, )
    res = db_pg.query_run(sql, data)
    
    if not res:
        return buildResponse(400, f'Error deleting member')
    return res

def _update_member(role, member_id, workspace_id):
    sql = 'UPDATE user_space_role SET role = %s WHERE user_id = %s AND workspace_id = %s RETURNING id;'
    data = (role, member_id, workspace_id)
    res = db_pg.query_get(sql, data)
    if not res:
        return buildResponse(400, f'Error updating members')
    return res

def _get_all_workspaces(user_id):
    sql = '''
        SELECT  w.id as id,
                w.name as name,
                w.description as description,
                w.date as date,
                usr.role as role
        FROM user_space_role usr INNER JOIN workspace w
        ON w.id = usr.workspace_id
        WHERE usr.user_id = %s
        ORDER BY w.date DESC;
        '''
    data = (user_id, )
    res = db_pg.query_get(sql, data, fetch_all=True)

    if not res:
        raise ResourceGetError('workspace')
    return res

def _get_workspace(user_id, workspace_id):
    role = _get_workspace_role(user_id, workspace_id)
    workspace = _get_workspace_by_id(workspace_id)
    payload = {
        "id": workspace["id"],
        "name": workspace["name"],
        "description": workspace["description"],
        "role": role,
        "created_at": workspace["date"]
    }
    return payload

def _get_workspace_role(user_id, workspace_id):
    sql = 'SELECT role FROM user_space_role WHERE user_id = %s AND workspace_id = %s;'
    data = (user_id, workspace_id)
    res = db_pg.query_get(sql, data)

    if not res:
        raise ResourceGetError('role')
    return res['role']

def _get_workspace_by_id(workspace_id):
    sql = 'SELECT * FROM workspace WHERE id = %s;'
    data = (workspace_id, )
    res = db_pg.query_get(sql, data)

    if not res:
        raise ResourceGetError('workspace')
    return res

######

def getMembers(workspace_id, user_id):
    sql = '''
        SELECT  usr.id,
                usr.user_id, 
                usr.role, 
                owner.email 
        FROM user_space_role usr INNER JOIN owner 
        ON usr.user_id = owner.id
        WHERE usr.workspace_id = %s AND user_id != %s
        ORDER BY usr.date DESC;
    '''
    data = (workspace_id, user_id)
    res = db_pg.query_get(sql, data, fetch_all=True)
    if not res:
        raise ResourceGetError('members')
    return res