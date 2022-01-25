import simplejson as json
import stripe
import os

import psycopg2
import psycopg2.extras

# for cryptography
import hashlib
import binascii
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# import sentry_sdk
# from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# sentry_sdk.init(
#     dsn="https://215be4c2271a4a4a84401ba3b71efc66@o503540.ingest.sentry.io/5613577",
#     integrations=[AwsLambdaIntegration()]
# )

SCHEDULER_ENDPOINT = os.environ.get('SCHEDULER_ENDPOINT')
TEST_API_KEY = os.environ.get('TEST_API_KEY')
TEST_WEBHOOK_SECRET = os.environ.get('TEST_WEBHOOK_SECRET')

LIVE_API_KEY = os.environ.get('LIVE_API_KEY')
LIVE_WEBHOOK_SECRET = os.environ.get('LIVE_WEBHOOK_SECRET')


stripe.api_key = LIVE_API_KEY if LIVE_API_KEY else TEST_API_KEY
webhook_secret = LIVE_WEBHOOK_SECRET if LIVE_WEBHOOK_SECRET else TEST_WEBHOOK_SECRET
live_prices = {
    "Starter": "price_1HqoSUE2xldxFSOhQDYHGRUE",
    "StarterAnnual": "price_1HteYCE2xldxFSOhK0WVpG0Y",

    "Basic": "price_1Gru6vE2xldxFSOh8p4iggOa",
    "BasicAnnual": "price_1HteaEE2xldxFSOhbNRnDgQf",

    "Pro": "plan_H872SMVCvvoWcw",
    "ProAnnual": "price_1HteZEE2xldxFSOhbNJrd40l",

    "Pro 10k": "price_1Ia3kkE2xldxFSOhQFvzUeLF",
    "ProAnnual 10k": "price_1Ia3m8E2xldxFSOhvjhFVJ72",

    "Pro 20k": "price_1Ia3rTE2xldxFSOh3opNZelH",
    "ProAnnual 20k": "price_1Ia3rTE2xldxFSOhNlBssX58",

    "Pro 40k": "price_1Ia4TrE2xldxFSOhPhw8Go4Y",
    "ProAnnual 40k": "price_1Ia4TrE2xldxFSOh0HcYqaTF",

    "Pro 80k": "price_1Ia4UvE2xldxFSOhCKm2i4k7",
    "ProAnnual 80k": "price_1Ia4UvE2xldxFSOhlzomiUx4",

    "Pro 100k": "price_1Ia4XCE2xldxFSOhxVdIaGVI",
    "ProAnnual 100k": "price_1Ia4XCE2xldxFSOhubWLHwwT",

    "Team": "price_1HqoTeE2xldxFSOhvqHz1RaA",
    "TeamAnnual": "price_1HteXXE2xldxFSOhp4zrCg4u",

    "Founder": "price_1HzDtdE2xldxFSOhfbfLq620",
    "FounderAnnual": "price_1HzDtdE2xldxFSOhPLb0HRKB"
}

prices = live_prices

castConnectInfo = {
    "dbHost": "castodiav2-proxy.proxy-c8ovevjmjfel.us-west-1.rds.amazonaws.com",
    "dbName": "castodia",
    "dbPort": 5432,
    "dbUser": "DdBa5VAa",
    "dbPass": "Mjm6FkwpSbUpa6P4"
}


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

        # IF YOU WANT TO RETURN DATA AS A DICT:
        if dataAsDict:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = conn.cursor()

        print("SUCCESSFULLY CONNECTED TO DB")

        return conn, cur
    except psycopg2.Error as e:
        print('Psycopg2 connection error: ', e)
        return False


def buildResponse(statusCode, message):
    responseObject = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        },
        'body': json.dumps(message, default=str)
    }
    return responseObject


def new_checkout_session(plan):
    print("PLAN: ", plan)
    if plan not in prices:
        print('Invalid plan')
        return False
        # Possible values: {}".format(prices.keys()), 400
    # TODO: add proper URL for success
    try:
        checkout_session = stripe.checkout.Session.create(
            success_url="https://www.castodia.com/",
            # "https://dropbase.io/success?session_id={CHECKOUT_SESSION_ID}
            cancel_url="https://www.castodia.com/pricing",
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": prices[plan],
                "quantity": 1,
            }],
            subscription_data={"metadata": {
                "productId": prices[plan],
            }})
        return checkout_session["id"]

    except Exception as e:
        print('Stripe checkout error: ', e)
        return False


def verify_password(stored_password, provided_password):
    try:
        "Verify a stored password against one provided by user"
        salt = stored_password[:64]
        stored_password = stored_password[64:]
        pwdhash = hashlib.pbkdf2_hmac('sha512',
                                      provided_password.encode('utf-8'),
                                      salt.encode('ascii'),
                                      100000)
        pwdhash = binascii.hexlify(pwdhash).decode('ascii')
        return pwdhash == stored_password
    except Exception as e:
        print('Verification error', e)
        return False


def verifyUser(email, secret):
    # VERIFY USER BY SECRET
    try:
        conn, cur = sc(castConnectInfo)

        sql = "SELECT secret, id FROM owner WHERE email = %s;"
        data = (email,)
        cur.execute(sql, data)

        secretowner = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if secretowner:
            verified = verify_password(secretowner[0], secret)
            return verified, secretowner[1]
        else:
            return False, False
    except Exception as e:
        print('Psycopg2 SELECT owner error', e)
        return False, False

# TODO: try-catch


def lambda_handler(event=None, context=None):

    # could i just get this from request headers...?
    body = event['body']
    print('body: ', body)
    print('body type: ', type(body))
    if body.find('website') != -1:
        print('from website')
        plan_loc = body.find('plan=')
        plan = body[plan_loc+5:]
        #print('got plan: ', plan)
        session = new_checkout_session(plan)
        if session:
            return buildResponse(200, str(session))
        else:
            # TODO: BETTER FAIL MESSAGE. WHERE IS IT USED?
            return buildResponse(500, "Fail :(")
    else:
        print('from addon')
        print('json body: ', json.loads(body))
        body = json.loads(body)

        verified, owner = verifyUser(body['email'], body['secret'])

        print('verified, owner: ', verified, owner)

        if verified and owner:
            conn, cur = sc(castConnectInfo)
            data = (owner, )
            sql = "SELECT customer_id from owner where id=%s;"
            cur.execute(sql, data)

            customer_id = cur.fetchone()

            cur.close()
            conn.close()

            print('got customer_id: ', customer_id)
            try:
                if customer_id:
                    session = stripe.billing_portal.Session.create(
                        customer=customer_id[0],
                        return_url='https://www.castodia.com/pricing',
                    )
                    print('got session: ', session['url'])
                    return buildResponse(200, session['url'])
                else:
                    return buildResponse(200, "https://www.castodia.com/pricing")
            except Exception as e:
                return buildResponse(200, "https://www.castodia.com/pricing")

        else:
            return buildResponse(403, "User not authorized")
