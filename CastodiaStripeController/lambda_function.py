import simplejson as json
import stripe
import os
import psycopg2
import psycopg2.extras
from datetime import datetime

import requests

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import os
import boto3
from botocore.exceptions import ClientError

# import sentry_sdk
# from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# sentry_sdk.init(
#     dsn="https://bb29fc5479bf4ddfa6e4027b7f0b2767@o503540.ingest.sentry.io/5613574",
#     integrations=[AwsLambdaIntegration()]
# )


SCHEDULER_ENDPOINT = os.environ.get('SCHEDULER_ENDPOINT')
TEST_API_KEY = os.environ.get('TEST_API_KEY')
TEST_WEBHOOK_SECRET = os.environ.get('TEST_WEBHOOK_SECRET')

LIVE_API_KEY = os.environ.get('LIVE_API_KEY')
LIVE_WEBHOOK_SECRET = os.environ.get('LIVE_WEBHOOK_SECRET')

# print(os.environ)
stripe.api_key = 'sk_test_51JsT3kAEwWyiL9c5t0BlfcGZ5xFX2CmDUJSrpe0blShctfib8m9L7OLMA3mcTwlVvEfTXqZLOeY275o1jZehC2vf00uqAYXRjO'
webhook_secret = 'whsec_r4keOjbl9udEvEPUHcMfY1kL236AYjli'


live_prices = {
    "Starter": "price_1JzprQAEwWyiL9c5X4IPr4at",
    "StarterAnnual": "price_1JzptkAEwWyiL9c5Z2HzdcOS",

    # "Basic": "price_1Gru6vE2xldxFSOh8p4iggOa",
    # "BasicAnnual": "price_1HteaEE2xldxFSOhbNRnDgQf",

    # "Pro": "plan_H872SMVCvvoWcw",
    # "ProAnnual": "price_1HteZEE2xldxFSOhbNJrd40l",

    # "Pro 10k": "price_1Ia3kkE2xldxFSOhQFvzUeLF",
    # "ProAnnual 10k": "price_1Ia3m8E2xldxFSOhvjhFVJ72",

    # "Pro 20k": "price_1Ia3rTE2xldxFSOh3opNZelH",
    # "ProAnnual 20k": "price_1Ia3rTE2xldxFSOhNlBssX58",

    # "Pro 40k": "price_1Ia4TrE2xldxFSOhPhw8Go4Y",
    # "ProAnnual 40k": "price_1Ia4TrE2xldxFSOh0HcYqaTF",

    # "Pro 80k": "price_1Ia4UvE2xldxFSOhCKm2i4k7",
    # "ProAnnual 80k": "price_1Ia4UvE2xldxFSOhlzomiUx4",

    # "Pro 100k": "price_1Ia4XCE2xldxFSOhxVdIaGVI",
    # "ProAnnual 100k": "price_1Ia4XCE2xldxFSOhubWLHwwT",

    # "Team": "price_1HqoTeE2xldxFSOhvqHz1RaA",
    # "TeamAnnual": "price_1HteXXE2xldxFSOhp4zrCg4u",

    # "Founder": "price_1HzDtdE2xldxFSOhfbfLq620",
    # "FounderAnnual": "price_1HzDtdE2xldxFSOhPLb0HRKB"
}


products = {
    "prod_KfABDACKRP1c0Y": "Starter",
    "prod_Gs4v7K6BdGKmFO": "Basic",
    "prod_H872v0bCIPTUSc": "Pro",
    "prod_IaLQjSal9AOZPn": "Pro 10k",
    "prod_JCSnesQZCds6qU": "Pro 20k",
    "prod_JCTQnDBVwMSv9w": "Pro 40k",
    "prod_JCTRn7eABc01j0": "Pro 80k",
    "prod_JCTUm00VRzVEIn": "Pro 100k",
    "prod_IRhtUtb50ZF698": "Team",
    "prod_GtwsERMnxb7A9U": "Pro Early",
    "prod_HCAdK3kFAOSNSh": "Team Early",
    "prod_IaOhE7wrPQ49ZE": "Founder"
}

manual_limits = {
    "Starter": 100,
    "Basic": 500,
    "Pro": 5000,
    "Pro 10k": 10000,
    "Pro 20k": 20000,
    "Pro 40k": 40000,
    "Pro 80k": 80000,
    "Pro 100k": 100000,
    "Team": 25000,
    "Pro Early": 5000,
    "Team Early": 20000
}

schedule_limits = {
    "Starter": 100,
    "Basic": 500,
    "Pro": 5000,
    "Pro 10k": 10000,
    "Pro 20k": 20000,
    "Pro 40k": 40000,
    "Pro 80k": 80000,
    "Pro 100k": 100000,
    "Team": 25000,
    "Pro Early": 5000,
    "Team Early": 20000
}



prices = live_prices

resetEmailSmtpSettings = {
    "sender_email": "luiz@dropbase.io",
    "password": "Dropbase15975369#"
}

invoiceEmail = {
    "subject": "",
    "text": "",
    "html": ""
}

castConnectInfo = {
    "dbHost": "172.17.0.1",
    "dbName": "postgres",
    "dbPort": 5432,
    "dbUser": "postgres",
    "dbPass": "password"
} 


def format_date(val):
    return datetime.fromtimestamp(val).strftime('%Y-%m-%d %H:%M:%S')


def sc(connectInfo, dataAsDict=False):
    # IN CASE PORT IS NOT SPECIFIED
    if connectInfo["dbPort"] == None:
        connectInfo["dbPort"] = 5432

    try:
        conn = psycopg2.connect(
            host=connectInfo["dbHost"],
            port=connectInfo["dbPort"],
            database=connectInfo["dbName"],
            user=connectInfo["dbUser"],
            password=connectInfo["dbPass"])

        if dataAsDict:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = conn.cursor()
        return conn, cur
    except psycopg2.Error as e:
        print('Psycopg2 connection error: ', e)
        return False


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


def getOwner(email):
    try:
        conn, cur = sc(castConnectInfo)

        sql = "SELECT id, active from owner where email = %s"
        data = (email, )
        cur.execute(sql, data)

        owner = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if owner:
            return owner
        else:
            return False
    except Exception as e:
        print('Psycopg2 SELECT owner error:', e)
        return False

# TODO: maybe make updateUserPlan() for RETURNING (i.e. plan change for existing customers)


def createUserPlan(owner, customer_id, prod_id, interval, plan_start, plan_end):
    print('NEW USER UPGRADED!')
    print(owner, customer_id, prod_id, interval, plan_start, plan_end)
    try:
        conn, cur = sc(castConnectInfo, True)

        plan = products[prod_id]
        data1 = (customer_id, plan, interval,
                 plan_start, plan_end, None, owner)
        sql1 = "UPDATE owner SET customer_id = %s, plan=%s, interval=%s, plan_start=%s, plan_end=%s, active = TRUE, deactivate_reason = %s where id=%s;"
        cur.execute(sql1, data1)

        # RESET success and failure counts
        sql2 = "UPDATE usage_total SET manual_success=0, manual_fail=0, schedule_success=0, schedule_fail = 0 WHERE owner = %s;"
        data2 = (owner, )
        print('sql data2: ', data2)
        cur.execute(sql2, data2)

        conn.commit()
        cur.close()
        conn.close()

        return True
    except Exception as e:
        print('Psycopg2 UPDATE error:', e)
        return False


def resetQuota(owner, plan_end):
    try:
        conn, cur = sc(castConnectInfo, True)

        sql1 = "UPDATE owner SET plan_end=%s WHERE id=%s;"
        data1 = (plan_end, owner)
        cur.execute(sql1, data1)

        sql2 = "UPDATE usage_total SET warning_sent=0, manual_success=0, manual_fail=0, schedule_success=0, schedule_fail = 0 WHERE owner=%s;"
        data2 = (owner, )
        cur.execute(sql2, data2)

        conn.commit()
        cur.close()
        conn.close()

        print('RESET FINE')

        return True
    except Exception as e:
        print('Psycopg2 UPDATE error:', e)
        return False


def resetEndDate(owner, plan_end):
    try:
        conn, cur = sc(castConnectInfo, True)

        sql1 = "UPDATE owner SET plan_end=%s WHERE id=%s;"
        data1 = (plan_end, owner)
        cur.execute(sql1, data1)

        conn.commit()
        cur.close()
        conn.close()

        return True
    except Exception as e:
        print('Psycopg2 UPDATE error:', e)
        return False


def sendEmail(receiver_email, subject, text, html, replyTo='support@castodia.com'):
    print('TRYING TO SEND EMAIL')
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


def sendEmail_smtp(receiver_email, subject, text, html):

    sender_email = resetEmailSmtpSettings['sender_email']
    password = resetEmailSmtpSettings['password']

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = receiver_email

    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    try:
        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(
                sender_email, receiver_email, message.as_string()
            )
        return True
    except Exception as e:
        print('SMTP error: ', e)
        return False

#############
# SCHEDULES #
#############

# SAVE SCHEDULE


def saveSchedule(schedules, repeating, timezone, humanReadableStr, jobID, owner):

    # schedules is an array of cron strings
    data = {
        'schedules': schedules,
        'repeating': repeating,
        'timezone': timezone,
        'humanReadableStr': humanReadableStr,
        'jobID': str(jobID),
        'owner': str(owner)
    }

    try:
        # result = requests.post(SCHEDULER_ENDPOINT, data=json.dumps(data))
        header = {'Content-Type': 'application/json; charset=UTF-8'}
        session = requests.Session()
        result = session.post(
            SCHEDULER_ENDPOINT, json.dumps(data), headers=header)
    except Exception as e:
        print(e)
        return False, None

    if result.status_code == 201:  # 201 is created
        return True, result.text  # if successful, return the json contained created schedule
    else:
        print(result.status_code)
        print(result.text)
        return False, None  # return nothing otherwise


def deactivateAllJobs(deactivate_reason, owner):
    conn, cur = sc(castConnectInfo, True)
    result = None
    # DEACTIVATE ALL THE JOBS IN JOBSCRON WITH deactivate_reason = 'unpaid'
    try:
        data = (owner,)
        sql1 = "UPDATE owner SET plan='unpaid' WHERE id=%s"
        cur.execute(sql1, data)
        conn.commit()

        sql2 = "UPDATE jobscron SET active = FALSE, deactivate_reason = %s WHERE owner = %s and active = TRUE RETURNING id, original;"
        data2 = (deactivate_reason, owner)
        cur.execute(sql2, data2)
        result = cur.fetchall()
        conn.commit()
    except Exception as e:
        print(e)
        result = None
        # TODO: HANDLE EXCEPTION

    # DELETE ALL THE SCHEDULES ASSOCAITED WITH THESE JOBS
    if result and len(result) > 0:
        session = requests.Session()
        for i in result:
            print("removing ", i['id'])
            session.delete('{}/{}'.format(SCHEDULER_ENDPOINT, i['id']))
    cur.close()
    conn.close()
    return result

# REACTIVATE SCHEDULE


def reactivateSchedule(owner, jobID, conn, cur):
    try:
        #conn, cur = sc(castConnectInfo, True)
        sql1 = "SELECT * from jobscron WHERE id = %s"
        data1 = (jobID,)
        cur.execute(sql1, data1)
        originalSchedule = cur.fetchone()
        if originalSchedule:
            # ADD BACK TO MONGODB
            successMg, savedScheduleMg = saveSchedule(
                originalSchedule["schedules"],  # COULD BE JSON
                originalSchedule["repeating"],
                originalSchedule["timezone"],
                originalSchedule["humanreadablestr"],
                jobID,
                owner
            )
            # REACTIVATE IN PG
            reactivatedJob = None
            if successMg and savedScheduleMg:
                sql = "UPDATE jobscron SET active = TRUE, deactivate_reason = '' WHERE owner = %s AND id = %s RETURNING id"
                data = (owner, jobID)
                cur.execute(sql, data)
                reactivatedJob = cur.fetchone()
                conn.commit()

            cur.close()
            conn.close()

            if reactivatedJob:
                return reactivatedJob
            else:
                return False
        else:
            return False
    except Exception as e:
        print('Error reactivating schedules: ', e)
        return False

# TODO: add customer_id to systemlogs if there is an exception


def lambda_handler(event=None, context=None):
    # body = json.loads(event['body'])
    body = event['body']
    if webhook_secret:
        # Retrieve the event by verifying the signature using the raw body and secret if webhook signing is configured.
        headers = (event['headers'])
        #print('got headers: ', headers)
        signature = headers['Stripe-Signature']
        #print('got signature: ', signature)
        try:
            event = stripe.Webhook.construct_event(payload=(event['body']),
                                                   sig_header=signature,
                                                   secret=webhook_secret)
            data = event['data']
        except Exception as e:
            print('Stripe webhook exception: ', e)
            return str(e)
        event_type = event['type']
    else:
        data = body
        event_type = event['type']

    data_object = data['object']
    print('===========event_type===============')
    print(event_type)
    #print('got event: ', data_object)
    if event_type == 'customer.subscription.created':
        try:
            customer_id = data_object['customer']
            customer = stripe.Customer.retrieve(customer_id)
            email = customer['email']

            owner, owner_status = getOwner(email)
            # maybe just get schedules[] in first call to reduce queries?
            if owner_status == 'inactive':
                conn, cur = sc(castConnectInfo)
                data = (owner, )
                sql = "select * from  jobscron where deactivate_reason='expired_trial' and owner = %s"
                cur.execute(sql, data)
            

                for job in cur.fetchall():
                    print(job[0])
                    reactivateSchedule(owner, job[0], conn, cur)

            subscription_info = stripe.Subscription.list(customer='cus_KXZkshPSr3yjfo')
            subscription = subscription_info['data'][0]
            prod_id = subscription['plan']['product']
            interval = subscription['plan']['interval']

            print('got subscription: ', subscription)

            plan_start = subscription['current_period_start']
            plan_end = subscription['current_period_end']

            plan_start = format_date(plan_start)
            plan_end = format_date(plan_end)

            print(owner)
            print(customer_id)
            print(prod_id)
            print(interval)
            print(plan_start)
            print(plan_end)
            createUserPlan(owner, customer_id, prod_id,
                           interval, plan_start, plan_end)

        except Exception as e:
            print('Subscription create error: ', e)
            return buildResponse(500, str(e))

    if event_type == 'customer.subscription.deleted':
        try:
            customer_id = data_object['customer']
            customer = stripe.Customer.retrieve(customer_id)
            email = customer['email']

            owner = getOwner(email)[0]

            deactivateAllJobs('cancelled', owner)

        except Exception as e:
            print('Subscription delete error: ', e)
            return buildResponse(500, str(e))

    if event_type == 'customer.subscription.updated':
        try:
            status = data_object['status']
            print('data object: ', data_object)
            customer_id = data_object['customer']
            customer = stripe.Customer.retrieve(customer_id)

            if status == 'active':
                email = customer['email']
                owner = getOwner(email)[0]

                subscription = customer['subscriptions']['data'][0]
                prod_id = subscription['plan']['product']
                conn, cur = sc(castConnectInfo)
                data = (products[prod_id], owner)
                sql = "select * from owner where plan != %s and id = %s"
                cur.execute(sql, data)
                planHasChanged = cur.fetchone()
                cur.close()
                conn.close()

                if planHasChanged:
                    interval = subscription['plan']['interval']

                    plan_start = subscription['current_period_start']
                    plan_end = subscription['current_period_end']

                    plan_start = format_date(plan_start)
                    plan_end = format_date(plan_end)

                    createUserPlan(owner, customer_id, prod_id,
                                   interval, plan_start, plan_end)

            if status == 'incomplete_expired':
                email = customer['email']
                owner = getOwner(email)[0]

                _subject = "New Subscription Incomplete"
                _text = "Dear Castodia Customer,\nWe were not able to start your subscription upgrade due to issues. Sorry for the inconvenience. Please click on the link below to select your plan and retry or contact us at support@castodia.com for help.\nUpgrade your Castodia Plan\nThe Castodia Team.\n\n"
                _html = "<p>Dear Castodia Customer,</p><p>We were not able to start your subscription upgrade due to issues. Sorry for the inconvenience. Please click on the link below to select your plan and retry or contact us at <a href='mailto:support@castodia.com'>support@castodia.com</a> for help.</p> <p><a href='https://www.castodia.com/pricing'>Upgrade your Castodia Plan</a></p> <p>The Castodia Team.</p>"

                sendEmail(
                    email, subject=_subject, text=_text, html=_html)

            if status == 'past_due':
                email = customer['email']
                owner = getOwner(email)[0]

                subscription = customer['subscriptions']['data'][0]
                invoice_id = (subscription['latest_invoice'])
                invoice = stripe.Invoice.retrieve(invoice_id,)

                url = (invoice['hosted_invoice_url'])

                _subject = "Invoice Payment Failed"
                _text = "Dear Castodia Customer,\nWe were not able to process your payment. This may result in interruptions in your scheduled auto-refreshes. Please click on the link below to update your payment method or contact us at support@castodia.com for help.\n Update Payment Method:"
                _text += url
                _html = "<p>Dear Castodia Customer,</p> <p>We were not able to process your payment. This may result in interruptions in your scheduled auto-refreshes. Please click on the link below to update your payment method or contact us at <a href='mailto:support@castodia.com'>support@castodia.com</a> for help.</p>"
                _html += "<p><a href=" + url + \
                    ">Update Payment Method.</a></p> <p>The Castodia Team.</p>"

                sendEmail(
                    email, subject=_subject, text=_text, html=_html)

            if status == 'unpaid':
                email = customer['email']
                owner = getOwner(email)[0]
                # deactivateUserPlan(owner)

                subscription = customer['subscriptions']['data'][0]
                invoice_id = (subscription['latest_invoice'])
                invoice = stripe.Invoice.retrieve(invoice_id,)

                url = (invoice['hosted_invoice_url'])

                _subject = "Castodia - Account Suspension"
                _text = "Dear Castodia Customer,\nWe attempted to process payment for your Castodia subscription again using your saved payment method, but were unable to complete it. As a result, your account is scheduled for suspension within the next 48 hours.\nPlease update your payment method to avoid any service interruptions. Use the link below or contact us at support@castodia.com for further instructions.\nUpdate Payment Method:\n"
                _text += url
                _html = "<p>Dear Castodia Customer,</p> <p>We attempted to process payment for your Castodia subscription again using your saved payment method, but were unable to complete it. As a result, your account is scheduled for suspension within the next 48 hours.</p> <p>Please update your payment method to avoid any service interruptions. Use the link below or contact us at <a href='mailto:support@castodia.com'>support@castodia.com</a> for further instructions.</p>"
                _html += "<p><a href=" + url + \
                    ">Update Payment Method.</a></p> <p>The Castodia Team.</p>"

                sendEmail(
                    email, subject=_subject, text=_text, html=_html)

                plan_end = subscription['current_period_end']
                plan_end = format_date(plan_end)
                resetQuota(owner, plan_end)
                deactivateAllJobs('unpaid', owner)

        except Exception as e:
            print('Subscription update error: ', e)
            return buildResponse(500, str(e))

    if event_type == 'invoice.upcoming':
        # TODO: if owner is inactive, and job deactivate_reason = 'limit_exceeded', reset them all with reactivateSchedule()
        try:
            customer_id = data_object['customer']
            customer = stripe.Customer.retrieve(customer_id)
            email = customer['email']
            owner = getOwner(email)[0]
            subscription = customer['subscriptions']['data'][0]
            plan_end = subscription['current_period_end']
            plan_end = format_date(plan_end)
            # resetQuota(owner, plan_end)
            resetEndDate(owner, plan_end)

            return buildResponse(200, 'all good')

        except Exception as e:
            print('Invoice error: ', e)
            return buildResponse(500, str(e))

    else:
        print('=============something=============')
        return buildResponse(200, "Webhook didn't crash & burn ggwp")
