# import sentry_sdk
# from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# postgresql
from db_psycopg2 import db_pg

import helpers

# loading libraries
import json
import uuid
import re
import hashlib
import binascii
import os
import datetime
import requests
import pickle

import boto3
from botocore.exceptions import ClientError


def getOwner(email):
    try:
        # conn, cur = sc(castConnectInfo)
        sql = "SELECT id from owner where email = %s"
        # sql = "SELECT id, workspace_id from owner where email = %s"
        data = (email, )
        res = db_pg.query_get(sql, data)

        if not res:
            return False
        return res['id']
    except Exception as e:
        print('Psycopg2 SELECT owner error', e)
        return False


def check_workspace_ownership(user_id, workspace_id):
    print('check ownership')
    print(user_id)
    print(workspace_id)
    sql = "SELECT role FROM user_space_role WHERE user_id = %s AND workspace_id = %s AND role = 'owner';"
    data = (user_id, workspace_id)
    res = db_pg.query_get(sql, data)
    if not res:

        return False
    return True


def lambda_handler(event=None, context=None):
    # GET DATA
    data = json.loads(event['body'])
    print('===========')
    print(data)
    print('===========')
    action = data['action']

    # Get user_id
    user_id = getOwner(data['email'])

    workspace_id = data['workspace_id']

    if action == 'getDatabasesNames':
        databases = helpers.get_all_databases_names(workspace_id, user_id)
        return helpers.buildResponse(200, databases)

    if action == 'getDatabaseCreds':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')

        database_id = data['database_id']
        database_creds = helpers.get_database_creds(workspace_id, user_id, database_id)
        database_creds['database_id'] = database_id
        return helpers.buildResponse(200, database_creds)

    if action == 'addDatabase':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')

        dbConnectInfo = data['dbConnectInfo']
        # helpers.test_database_connection(dbConnectInfo)
        
        database_id = helpers.add_workspace_database(user_id, dbConnectInfo, workspace_id)
        return helpers.buildResponse(200, database_id)

    elif action == 'updateDatabase':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')
        
        dbConnectInfo = data['dbConnectInfo']
        database_id = data['database_id']
        helpers.update_workspace_database(user_id, workspace_id, database_id, dbConnectInfo)
        return helpers.buildResponse(200, 'Database Updated')

    elif action == 'deleteDatabase':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')
        
        database_id = data['database_id']
        helpers.delete_workspace_database(user_id, workspace_id, database_id)
        return helpers.buildResponse(200, 'Databse deleted!')
    
    elif action == 'testConnectionDb':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')

        dbConnectInfo = data['dbConnectInfo']
        helpers.test_database_connection(dbConnectInfo)

        return helpers.buildResponse(200, 'Connection is good!')
    
    else:
        return helpers.buildResponse(400, 'Not valid action')
