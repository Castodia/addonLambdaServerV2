# import sentry_sdk
# from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# postgresql
import psycopg2
import psycopg2.extras
from db_psycopg2 import db_pg

# for cryptography
import base64
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

# castConnectInfo = {
#     "dbHost": "172.17.0.1",
#     "dbName": "postgres",
#     "dbPort": 5432,
#     "dbUser": "postgres",
#     "dbPass": "password"
# }



def serializer(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

# def sc(connectInfo, dataAsDict=False):
#     if connectInfo["dbPort"] == None:
#         connectInfo["dbPort"] = 5432
#     try:
#         conn = psycopg2.connect(
#             host=connectInfo["dbHost"],
#             port=connectInfo["dbPort"],
#             database=connectInfo["dbName"],
#             user=connectInfo["dbUser"],
#             password=connectInfo["dbPass"])
#         if dataAsDict:
#             cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
#         else:
#             cur = conn.cursor()
#         return conn, cur
#     except psycopg2.Error as e:
#         print(e)
#         return False, False

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

def check_for_next_owner(workspace_id, user_id):
    print(user_id)
    sql = """SELECT id, role FROM user_space_role 
    WHERE workspace_id = %s AND role = 'owner' AND user_id != %s;"""
    data = (workspace_id, user_id)
    res = db_pg.query_get(sql, data)
    # if not res:
    #     return helpers.buildResponse(400, f'Cannot delete workspace before giving ownership to another user')
    return res


def createWorkspace(name, description, user_id):
    try:
        workspace_id = helpers._create_workspace(name, description)
        helpers._add_user_to_workspace(user_id, workspace_id, 'owner')
        return workspace_id
    except Exception as e:
        print(e)
        # raise ('Cannot create workspace:', str(e))
        return helpers.buildResponse(400, f'Cannot create workspace: {str(e)}')

def deleteWorkspace(workspace_id):
    try:
        helpers._delete_workspace_members(workspace_id)
        helpers._delete_workspace(workspace_id)
        helpers._delete_databases(workspace_id)
        ## DELETE SAVED QUERIES
        ## DELETE STRIPE INFORMATION
        ## STOP STRIPE SUBSCRIPTION
    except Exception as e:
        print(e)
        return helpers.buildResponse(400, f'Error deleting workspace: {str(e)}')

def updateWorkspace(workspace_id, description):
    try:
        helpers._update_workspace(workspace_id, description)
    except Exception as e:
        print(e)
        return helpers.buildResponse(400, f'Error updating organizatin: {str(e)}')

def addNewMember(member_info, workspace_id):
    try:
        user_id = getOwner(member_info['email'])
        if user_id == False:
            user_id = helpers._add_new_user(member_info['email'])
        helpers._add_user_to_workspace(user_id, workspace_id, member_info['role'])
    except Exception as e:
        print(e)
        return helpers.buildResponse(400, f'Error adding user: {str(e)}')

def deleteMember(member_id, workspace_id):
    try:
        helpers._delete_member(member_id, workspace_id)
    except Exception as e:
        print(e)
        return helpers.buildResponse(400, f'Error deleting member: {str(e)}')

def updateMember(role, member_id, workspace_id):
    try:
        helpers._update_member(role, member_id, workspace_id)
    except Exception as e:
        print(e)
        return helpers.buildResponse(400, f'Error updating member: {str(e)}')

def transferOnwership(user_id, member_id, workspace_id):
    try:
        # helpers._updateOwnership(user_id, workspace_id)
        helpers._update_member('owner', member_id, workspace_id)
        helpers._update_member('member', user_id, workspace_id)
    except Exception as e:
        print(e)
        return helpers.buildResponse(400, f'Error updating member: {str(e)}')

def getWorkspacesByUser(user_id):
    try:
        return helpers._get_all_workspaces(user_id)
    except Exception as e:
        print(e)
        return helpers.buildResponse(400, f'Error fetching workspace: {str(e)}')

def getWorkspaceById(user_id, workspace_id):
    try:
        return helpers._get_workspace(user_id, workspace_id)
    except Exception as e:
        print(e)
        return helpers.buildResponse(400, f'Error fetching workspace: {str(e)}')

def lambda_handler(event=None, context=None):
    ## GET DATA
    data = json.loads(event['body'])
    print('======')
    print(data)
    ## Get user information and action
    # user_id = data["user_id"]
    action = data['action']

    # Get user_id
    user_id = getOwner(data['email'])
    

    if action == 'newWorkspace':
        workspace_id = createWorkspace(data['name'], data['description'], user_id)
        return helpers.buildResponse(201, workspace_id)

    elif action == 'getAllWorkspaces':
        workspaces = getWorkspacesByUser(user_id)
        print(workspaces)
        return helpers.buildResponse(200, workspaces)
    

    ## Get information from client / parent lambda
    ## OR
    ## Get infomration from user => workspace_owner

    workspace_id = data['workspace_id']


    if action == 'getWorkspace':
        workspace = getWorkspaceById(user_id, workspace_id)
        return helpers.buildResponse(200, workspace)

    elif action == 'deleteWorkspace':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')
        deleteWorkspace(workspace_id)
        return helpers.buildResponse(200, 'Workspace deleted')

    elif action == 'updateWorkspace':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')
        updateWorkspace(workspace_id, data['description'])
        return helpers.buildResponse(200, 'Workspace updated')

    elif action == 'getMembers':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')
        
        members = helpers.getMembers(workspace_id, user_id)
        return helpers.buildResponse(200, members)
    elif action == 'addMembers':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')
        member_info = data['member_info']
        addNewMember(member_info, workspace_id)
        return helpers.buildResponse(201, 'Member added and invite sent')
        
    elif action == 'deleteMembers':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')
        member_id = data['member_id']
        transfered_ownership = check_for_next_owner(workspace_id, member_id)
        if transfered_ownership == None:
            return helpers.buildResponse(400, "You need to transfer workspace ownership before removing this user")
        else:
            deleteMember(member_id, workspace_id)
            return helpers.buildResponse(200, 'Member removed from workspace')
            

    elif action == 'updateMembers':
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')
        member_info = data['member_info']
        updateMember(member_info['role'], member_info['member_id'], workspace_id)
        return helpers.buildResponse(200, 'Member updated')

    elif action == 'transferOwnership':
        print('inside transfer onwership')
        if not check_workspace_ownership(user_id, workspace_id):
            return helpers.buildResponse(403, 'The user is not owner of this workspace')

        member_id = data['member_id']
        
        transferOnwership(user_id, member_id, workspace_id)
        return helpers.buildResponse(404, 'Onwership transfered')
        
    else:
        return helpers.buildResponse(404, 'Action not found')