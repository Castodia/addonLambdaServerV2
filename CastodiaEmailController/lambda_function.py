import simplejson as json
import psycopg2
import psycopg2.extras
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, date

from CastodiaErrorClass import *

from db_psycopg2 import db_pg

import requests

# import sentry_sdk
# from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# sentry_sdk.init(
#     dsn="https://e906981fe9f7423cb270401c19ffec79@o503540.ingest.sentry.io/5613571",
#     integrations=[AwsLambdaIntegration()]
# )

# schedulerEndpoint = "http://54.145.107.123/api/schedule"
schedulerEndpoint = "http://54.145.107.100/api/schedule"

emailThreeDaysAfter = {
    "subject": "Do you need support?",
    "text": """
    Hey there, do you need help or support using the Castodia Database Connector?\n\n
- Jimmy.""",
    "html":
    """<html><body><p>Hey there, do you need help or support using the Castodia Database Connector?<br /><br />
- Jimmy.</p></body> </html>"""
}


emailThreeDays = {
    "subject": "Your Castodia Trial ends in 3 days",
    "text": """
    Hey there, this is a reminder that your trial of Castodia Database Connector for Google Sheets will end in 3 days. Please upgrade your plan to avoid interruptions with our service.\n\n
Click to upgrade your Plan - https://www.castodia.com/pricing\n\n
- Jimmy.""",
    "html":
    """<html><body><p>Hey there, this is a reminder that your trial of Castodia Database Connector for Google Sheets will end in 3 days. Please upgrade your plan to avoid interruptions with our service.<br /><br />
<a href="https://www.castodia.com/pricing">Click to upgrade your Plan</a><br /><br />
- Jimmy.</p> </body> </html>"""
}

emailToday = {
    "subject": "Your Castodia Trial ended",
    "text": """
    Hey there, your trial of Castodia Database Connector has ended, which means you will no longer be able to run or schedule queries on Google Sheets.\n\n
Click to upgrade your Plan - https://www.castodia.com/pricing)\n\n
If you've already upgraded, please disregard this email.\n\n
- Jimmy.
""",
    "html": """<html><body><p>
    Hey there, your trial of Castodia Database Connector has ended, which means you will no longer be able to run or schedule queries on Google Sheets.<br /><br />
<a href="https://www.castodia.com/pricing">Click to upgrade your Plan</a><br /><br />
If you've already upgraded, please disregard this email.<br /><br />
- Jimmy.</p></body></html>"""
}


# def sc(connectInfo, dataAsDict=False):
#     # IN CASE PORT IS NOT SPECIFIED
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
#         print('Psycopg2 connection error: ', e)
#         return False


def buildResponse(statusCode, message):
    responseObject = {}
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['headers']["Access-Control-Allow-Origin"] = '*'
    #responseObject['headers']["Access-Control-Allow-Credentials"] = 'true'
    responseObject['headers']['Access-Control-Allow-Methods'] = 'OPTIONS,POST'
    responseObject['statusCode'] = statusCode
    responseObject['body'] = json.dumps(
        {"result": message}, default=str)
    return responseObject


def sendEmail(receiver_email, subject, text, html, replyTo='support@castodia.com'):
    SENDER = "The Castodia Team <jimmy@email.castodia.com>"
    AWS_REGION = "us-east-1"
    CHARSET = "UTF-8"
    client = boto3.client(
        'ses',
        aws_access_key_id='AKIASFSSQBRDSSDEI3KZ',
        aws_secret_access_key='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',
        region_name=AWS_REGION
    )
    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    receiver_email,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': html,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': text,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': subject,
                },
            },
            Source=SENDER,
            ReplyToAddresses=[replyTo]
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print('ERROR SENDING EMAIL')
        print(e.response['Error']['Message'])
        return False
    else:
        print("Email sent to :", receiver_email),
        print("Message ID", response['MessageId'])
        return True


def getEmail(owner):
    try:
        conn, cur = sc(castConnectInfo)

        sql = "SELECT email from owner where id = %s"
        data = (owner, )

        cur.execute(sql, data)

        owner = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if owner:
            return owner[0]
        else:
            return False
    except Exception as e:
        print('Psycopg2 SELECT owner error:', e)
        return False


def deactivateAllJobs(deactivate_reason, owner):
    conn, cur = sc(castConnectInfo, True)
    result = None
    # DEACTIVATE ALL THE JOBS IN JOBSCRON WITH deactivate_reason = 'backend'
    try:
        sql = "UPDATE jobscron SET active = FALSE, deactivate_reason = %s WHERE owner = %s and active = TRUE RETURNING id, original;"
        data = (deactivate_reason, owner)
        cur.execute(sql, data)
        result = cur.fetchall()
        conn.commit()
    except Exception as e:
        print('ERROR DEACTIVATING ALL JOBS')
        print(e)
        result = None

    # DELETE ALL THE SCHEDULES ASSOCAITED WITH THESE JOBS
    if result and len(result) > 0:
        session = requests.Session()
        for schedule in result:
            print("removing ", schedule['id'])
            session.delete('{}/{}'.format(schedulerEndpoint, schedule['id']))
    cur.close()
    conn.close()
    return result


def checkTrialExpiry():
    try:
        # ...select owner where plan_start = today + 3 and plan='Trial' -> new email
        conn, cur = sc(castConnectInfo, True)

        # 3 DAYS AFTER SUPPORT
        sql = "SELECT id from owner where cast (NOW() as date) - cast (plan_start as date) = 3 and plan='Trial'"
        cur.execute(sql)
        ownersThreeDaysAfter = cur.fetchall()
        conn.commit()
        print('Three days after install: ', ownersThreeDaysAfter)
        for owner in ownersThreeDaysAfter:
            email = getEmail((owner['id']))
            sendEmail(email, emailThreeDaysAfter['subject'],
                      emailThreeDaysAfter['text'], emailThreeDaysAfter['html'], 'jimmy@castodia.com')

        # 3 DAYS BEFORE EXPIRY

        sql = "SELECT id from owner where cast (plan_end as date) - cast (NOW() as date) = 3 and plan='Trial'"
        cur.execute(sql)
        ownersThreeDays = cur.fetchall()
        conn.commit()
        print('Owners due in three days: ', ownersThreeDays)
        for owner in ownersThreeDays:
            email = getEmail((owner['id']))
            sendEmail(email, emailThreeDays['subject'],
                      emailThreeDays['text'], emailThreeDays['html'], 'jimmy@castodia.com')

        # DAY OF EXPIRY
        sql = "SELECT id from owner where cast (NOW() as date) = cast (plan_end as date) and plan='Trial'"
        cur.execute(sql)
        ownersToday = cur.fetchall()
        for owner in ownersToday:
            print('expired trial for: ', owner['id'])
            data = (owner['id'], )
            deactivateAllJobs('trial_expired', owner['id'])
            sql = "UPDATE owner SET active = FALSE where id=%s;"
            cur.execute(sql, data)

            email = getEmail((owner['id']))
            sendEmail(email, emailToday['subject'],
                      emailToday['text'], emailToday['html'], 'jimmy@castodia.com')

        conn.commit()

        # RESET QUOTA FOR THOSE WHOSE END DATE IS TODAY AND ACTIVE  = TRUE
        sql = """UPDATE usage_total as ut SET warning_sent=0, manual_success=0, manual_fail=0, schedule_success=0, schedule_fail = 0
WHERE ut.owner IN (SELECT id from owner as o where cast (o.plan_end as date) = cast (NOW() as date) and o.active = TRUE);"""
        cur.execute(sql)
        conn.commit()

        cur.close()
        conn.close()
    except Exception as e:
        print('Psycopg2 OWNER error:', e)
        return False


# RESET MONTHLY QUOTAS FOR ANNUAL STRIPE CUSTOMERS
def checkAnnualUsers():
    users_monthly_usage = get_users_to_renew()
    renewed_monthly_usage = renew_monthyl_usage(users_monthly_usage)


def get_users_to_renew():
    # try:
    ## Check for users where their cycle ends on any month and any day
    sql = '''
    select id, cast(plan_end as date) from owner
        where
        plan_end >= NOW()
        AND EXTRACT(DAY from plan_end) = EXTRACT(DAY from NOW())
        AND interval = 'year';
    '''
    today = date.today()
    ## Check for users where their cycle ends after the 30th
    ## It will check for all 30 days months and renew it
    if today.month in [4, 6, 9, 11] and today.day == 30:
        sql = '''
        select id, cast(plan_end as date) from owner
            where 
                plan_end >= NOW()
                AND (
                    EXTRACT(DAY from plan_end) = 30 
                    OR EXTRACT(DAY from plan_end) = 31
                    )
                AND interval = 'year';
        '''
    ## Check for users where their cycle ends after the 28th
    ## This will check for February, in which will renew all plans 
    elif today.month == 2 and today.day == 28:
        sql = '''
        select id, cast(plan_end as date) from owner
            where 
                plan_end >= NOW()
                AND (
                    EXTRACT(DAY from plan_end) = 28 
                    OR EXTRACT(DAY from plan_end) = 29 
                    OR EXTRACT(DAY from plan_end) = 30 
                    OR EXTRACT(DAY from plan_end) = 31
                    )
                AND interval = 'year';
        '''
    res = db_pg.query_get(sql, None, fetch_all=True)
    if not res:
        # raise Exception()
        # raise Exception('Malformed input ...')
        # raise ClientException('qweqewqeqwweqw')
        return False
    return res
    # except Exception as e:
    #     exception_type = e.__class__.__name__
    #     exception_message = str(e)
    #     api_exception_obj = {
    #             "isError": True,
    #             "type": exception_type,
    #             "message": exception_message
    #         }
    #     api_exception_json = json.dumps(api_exception_obj)
    #     raise ClientException(api_exception_json)


def renew_monthyl_usage(users_usage):
    try:
        for monthly_usage in users_usage:
            sql = 'UPDATE usage_total SET warning_sent=0, manual_success=0, manual_fail=0, schedule_success=0, schedule_fail = 0 WHERE owner=%s;'
            data = (monthly_usage['id'])
            res = db_pg.query_run(sql, data)
            
            if not res:
                ## Raise Exception
                return False
            return res
    except Exception as e:
        print(e)
        ## Raise Exception
        return False


def lambda_handler(event=None, context=None):
    print('Eventbridge called.')
    # checkTrialExpiry()
    checkAnnualUsers()
    return buildResponse(200, "Webhook didn't crash & burn ggwp")
    
