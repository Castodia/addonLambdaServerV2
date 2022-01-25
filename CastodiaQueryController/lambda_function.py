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


def deconstruct_query_info(query_info):
    query_text = query_info['qText']
    query_name = query_info['qName']
    query_description = query_info['qDescr']
    qOptions = query_info['qOptions']
    try:
        qVars = query_info['qVars']
    except Exception as e:
        qVars = ""

    return query_text, query_name, query_description, qOptions, qVars


def lambda_handler(event=None, context=None):
    # GET DATA
    data = json.loads(event['body'])
    print('===========')
    print(data)
    print('===========')
    action = data['action']
    qTableName = "custQueryV1"

    # Get user_id
    user_id = getOwner(data['email'])

    workspace_id = data['workspace_id']

    if action == "getSavedQueries":
        savedQueries = helpers.get_saved_queries_by_workspace_id(
            user_id, workspace_id)
        return helpers.buildResponse(200, savedQueries)
    # elif action == 'getCustQueryById':
    #     query_id = data["query_id"]
    #     # GET RESULTS
    #     result = getQueryByName(user_id, workspace_id, query_id)
    #     if result:
    #         return helpers.buildResponse(200, )

    database_id = data['database_id']

    if action == 'saveCustQuery2':
        query = data['query_information']
        isActive = helpers.get_active_account(user_id)
        if not isActive:
            return helpers.buildResponse(403, "Your account is inactive")

        readOnly = helpers.verifyCustomQuery(query['qText'])
        query_text, query_name, query_description, qOptions, qVars = deconstruct_query_info(query)
        if not readOnly:
            problem = json.dumps({"qText": query})
            return helpers.buildResponse(501, f"Only read queries allowed! Problem at:{problem}")

        query_id = helpers.addCustQuery2(
            user_id, database_id, workspace_id, query_name, query_text, query_description, qVars, qOptions)
        # query_router_id = helpers.addQueryRouter(
        #     user_id, qTableName, query_id, qName, qDescr, database_id)
        return helpers.buildResponse(200, query_id)
    elif action == 'updateCustQuery2':

        query_id = data['query_id']
        query = data['query_information']

        query_text, query_name, query_description, qOptions, qVars = deconstruct_query_info(query)

        helpers.updateCustQuery2(
            user_id, database_id, workspace_id, query_name, query_text, query_description, qVars)
        

    elif action == "execute":
        # print("RUNNING SCHEDULED JOB")
        # # JOB DATA
        # jobID = data["jobID"]
        # job = getJob(jobID)

        # print("JOB DETAILS")
        # print(jobID)
        # print(job)
        print('inside execute')
        return helpers.buildResponse(200, 'scheduling query')

    elif action == "query":
        print('inside query')
        return helpers.buildResponse(200, 'running query')

    elif action == "getCustQueryByName":
        pass
    elif action == '':
        pass
    elif action == '':
        pass
    elif action == '':
        pass
    else:
        pass
