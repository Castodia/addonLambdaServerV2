# import sentry_sdk
# from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# postgresql
import psycopg2
import psycopg2.extras

# for cryptography
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# loading libraries
import json
import uuid
import re
import hashlib
import binascii
import os
import datetime
import pytz
import requests
import pickle

import boto3
from botocore.exceptions import ClientError

# sentry_sdk.init(
#     dsn="https://bf0125faef39470abb2377a1420676e2@o503540.ingest.sentry.io/5613538",
#     integrations=[AwsLambdaIntegration()]
# )

DECRYPTED = "48e9fad0-d2c3-4bb6-9892-de8844424d9cb5fff144-f9c8-4ed2-9855-9b8bce2f056c"

qTableName = "custQueryV1"
# schedulerEndpoint = "http://54.145.107.123/api/schedule"
schedulerEndpoint = "http://54.145.107.100/api/schedule"
# ELASTIC BEANSTOCK URL
# schedulerEndpoint = "http://addonscheduler-env.eba-ahxp6zrf.us-west-1.elasticbeanstalk.com/api/schedule"

castConnectInfo = {
    "dbHost": "172.17.0.1",
    "dbName": "postgres",
    "dbPort": 5432,
    "dbUser": "postgres",
    "dbPass": "password"
}

resetEmailSmtpSettings = {
    "sender_email": "luiz@dropbase.op",
    "password": "Padros15975369#"
}

lambdaNames = {
    "mysql":    "mySqlConnector",
    "oracle":   "OracleConnector",
    "mssql":    "MSSqlConnector",
    "maria":    "mySqlConnector",
    "snowflake": "SnowflakeConnector",
    "redshift": "RedshiftConnector"
}

#########
# UTILS #
#########


def buildResponse(statusCode, message):
    responseObject = {}
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['statusCode'] = statusCode
    responseObject['body'] = json.dumps(
        {"result": message}, default=str)
    return responseObject


def serializer(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def sc(connectInfo, dataAsDict=False):
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
        print(e)
        return False, False


##########
# LOGGER #
##########


def adderrorsLogs(owner, payload, errorType, problem):
    try:
        # LOG ERRORS
        conn, cur = sc(castConnectInfo)

        sql = "INSERT INTO errorsLogs (owner, payload, errorType, problem) VALUES (%s,%s,%s,%s) RETURNING id;"
        data = (owner, json.dumps(payload), errorType, problem)
        cur.execute(sql, data)

        errorLogID = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return errorLogID
    except Exception as e:
        print('Psycopg2 INSERT errorsLogs error: ', e)
        return False


def addFailedLogins(email, payload):
    # FAILED LOGINS
    try:
        conn, cur = sc(castConnectInfo)

        sql = "INSERT INTO failedLogins (email, payload) VALUES (%s,%s) RETURNING id;"
        data = (email, json.dumps(payload, default=str))
        cur.execute(sql, data)

        SheetQueryID = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return SheetQueryID
    except Exception as e:
        print('Psycopg2 INSERT failedLogins error: ', e)
        return False


def addScheduleErrorsLogs(jobID, payload, errorType, problem):
    try:
        conn, cur = sc(castConnectInfo)

        sql = "INSERT INTO errorsSchedule (jobID, payload, errorType, problem) VALUES (%s,%s,%s,%s) RETURNING id;"
        data = (jobID, json.dumps(payload, default=str), errorType, problem)
        cur.execute(sql, data)

        errorLogID = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return errorLogID
    except Exception as e:
        print('Psycopg2 INSERT errorsSchedule error: ', e)
        return False


def logActionStartToDb(userId, actionName, data=None, payload=None, successfully_completed=False):
    try:
        conn, cur = sc(castConnectInfo)
        sql = "INSERT INTO actionlogs (owner, action_name, data, payload, successfully_completed) VALUES (%s,%s,%s,%s,%s) RETURNING id;"

        data = (userId, actionName, json.dumps(data, default=str),
                json.dumps(payload, default=str), successfully_completed)
        cur.execute(sql, data)

        actionLogID = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return actionLogID
    except Exception as e:
        print('Psycopg2 INSERT actionlogs error: ', e)
        return False


def logActionSuccessToDb(actionId, updatePayload=False, payload=None):
    try:
        conn, cur = sc(castConnectInfo)
        if (updatePayload):
            sql = "UPDATE actionlogs SET successfully_completed=TRUE, payload=%s WHERE id=%s RETURNING id;"
            data = (json.dumps(payload, default=str), actionId)
        else:
            sql = "UPDATE actionlogs SET successfully_completed=TRUE WHERE id=%s RETURNING id;"
            data = (actionId,)
        cur.execute(sql, data)
        actionLogID = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return actionLogID
    except Exception as e:
        print('Psycopg2 UPDATE actionlogs error:', e)
        return False

###########
# EMAILER #
###########


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
    import smtplib
    import ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

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

#####################
# USER VERIFICATION #
#####################


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
        conn, cur = sc(castConnectInfo, True)

        sql = "SELECT secret, id, active, deactivate_reason FROM owner WHERE email = %s;"
        data = (email,)
        cur.execute(sql, data)

        secretowner = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if secretowner:
            verified = verify_password(secretowner['secret'], secret)
            return verified, secretowner['id'], secretowner['active']
        else:
            return False, False, False
    except Exception as e:
        print('Psycopg2 SELECT owner error', e)
        return False, False, False


def getOwner(email):
    try:
        conn, cur = sc(castConnectInfo)
        sql = "SELECT id from owner where email = %s"
        data = (email, )
        cur.execute(sql, data)

        owner = cur.fetchone()
        conn.commit()
        cur.close()

        if owner:
            return owner[0]
        else:
            return False
    except Exception as e:
        print('Psycopg2 SELECT owner error', e)
        return False


def hash_password(password):
    """Hash a password for storing."""
    salt = hashlib.sha256(os.urandom(60)).hexdigest().encode('ascii')
    pwdhash = hashlib.pbkdf2_hmac('sha512', password.encode('utf-8'),
                                  salt, 100000)
    pwdhash = binascii.hexlify(pwdhash)
    return (salt + pwdhash).decode('ascii')

###################
# RESET KEY SECTION
###################


def verifyUserWithKey(email, secret):
    try:
        # GET SAVED RESTORE SECRET
        conn, cur = sc(castConnectInfo)
        sql = "SELECT secret FROM resetKey WHERE email = %s AND date > %s AND date < %s;"

        # CHECK IF DATE HAS EXPIRED. QUERY ONLY SPECIFIC TIME WINDOW
        timeNow = datetime.datetime.now()
        data = (email, timeNow-datetime.timedelta(days=1), timeNow)

        cur.execute(sql, data)
        secretKey = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        # IF RECORDS PRESENT
        if secretKey:
            # VERIFY TOCKEN
            verified = verify_password(secretKey[0], secret)
            return verified
        else:
            return False
    except:
        return False


def sendResetEmail(key, email):
    # Create the plain-text and HTML version of your message
    text = """Here is your key for Castodia addon. Plase paste it when saving your query. key: %s""" % (
        key)

    html = """\
    <html><body><p>Hello,<br />Here is your Castodia Key:<br />%s<br />Paste it when saving database credentials<br /><br />Regards,<br />Castodia Team</p></body></html>
    """ % (key)

    return sendEmail(email, "Here is your Castodia key", text, html)


###################
# USER MANAGEMENT #
###################


def getUserById(id):
    try:
        conn, cur = sc(castConnectInfo, True)

        sql = "SELECT * FROM owner WHERE id = %s;"
        data = (id,)
        cur.execute(sql, data)

        user = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return user
    except Exception as e:
        print('Psycopg2 SELECT owner error', e)
        return False


def getUser(email, secret):
    try:
        conn, cur = sc(castConnectInfo)

        sql = "SELECT id FROM owner WHERE email = %s AND secret = %s;"
        data = (email, secret)
        cur.execute(sql, data)

        user = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return user
    except Exception as e:
        print('Psycopg2 SELECT owner error', e)
        return False


def getUserEmail(id):
    try:
        conn, cur = sc(castConnectInfo, True)
        sql = "SELECT email FROM owner WHERE id = %s;"
        data = (id,)
        cur.execute(sql, data)

        user = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        if user:
            return user
        else:
            return None
    except Exception as e:
        print('Psycopg2 SELECT owner error: ', e)
        return None


def addNewUser(email):
    try:
        # CREATES NEW USER
        conn, cur = sc(castConnectInfo)

        # sql = "INSERT INTO newuser (email) VALUES (%s) RETURNING id"
        sql = "INSERT INTO newuser (email) VALUES (%s) RETURNING id"
        data = (email,)
        cur.execute(sql, data)
        userRec = cur.fetchone()
        conn.commit()

        sql_owner = "INSERT INTO owner (email, plan, plan_start, plan_end) VALUES (%s, %s, %s, %s) RETURNING id"

        plan_start = datetime.datetime.now()
        plan_end = plan_start + datetime.timedelta(14)
        data_owner = (email, "Trial", plan_start, plan_end)

        cur.execute(sql_owner, data_owner)
        ownerRec = cur.fetchone()
        conn.commit()

        cur.close()
        conn.close()
        if userRec and ownerRec:
            return userRec[0]
        else:
            return False
    except:
        return False


# TRIAL SHOULD START WHEN USER AUTHENTICATES WITH GOOGLE
def addUser(email, secret):
    createdUser = None
    try:
        # CREATES NEW USER
        conn, cur = sc(castConnectInfo)
        sql_owner = """INSERT INTO owner (email, secret, plan, plan_start, plan_end) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (email) DO UPDATE SET
        (secret, plan, plan_start, plan_end) = (EXCLUDED.secret, EXCLUDED.plan, EXCLUDED.plan_start, EXCLUDED.plan_end)
        RETURNING id"""

        plan_start = datetime.datetime.now()
        plan_end = plan_start + datetime.timedelta(14)
        data_owner = (email, secret, "Trial", plan_start, plan_end)
        cur.execute(sql_owner, data_owner)
        userRec = cur.fetchone()
        conn.commit()

        if userRec:
            # CREATE USAGE RECORD
            sql_usage = "INSERT INTO usage_total (owner) VALUES (%s) RETURNING id"
            data_usage = (userRec[0], )
            cur.execute(sql_usage, data_usage)
            usageRec = cur.fetchone()
            conn.commit()
            if usageRec:
                createdUser = userRec[0]
        else:
            createdUser = False
        cur.close()
        conn.close()

        return createdUser
    except:
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
        # TODO: HANDLE EXCEPTION

    # DELETE ALL THE SCHEDULES ASSOCAITED WITH THESE JOBS
    if result and len(result) > 0:
        session = requests.Session()
        for schedule in result:
            print("removing ", schedule['id'])
            session.delete('{}/{}'.format(schedulerEndpoint, schedule['id']))
    cur.close()
    conn.close()
    return result


def deleteUser(email):
    try:
        conn, cur = sc(castConnectInfo)
        # DELETE JOBS - setActive = False
        sql = "SELECT id FROM owner WHERE email = %s;"
        data = (email,)
        cur.execute(sql, data)
        ownerID = cur.fetchone()
        conn.commit()
        if ownerID == None:
            return True, "no user with this such email"

        owner = ownerID[0]

        # DELETE QUERY ROUTER
        sql_delete_query_router = "DELETE FROM queryRouter WHERE owner = %s RETURNING id;"
        data_delete_query_router = (owner, )
        cur.execute(sql_delete_query_router, data_delete_query_router)
        deleted_query = cur.fetchall()
        conn.commit()
        print('deleted_query', deleted_query)

        # DELETE DBROUTER
        sql_delete_DB_router = "DELETE FROM dbRouter WHERE owner = %s RETURNING id;"
        data_delete_DB_router = (owner, )
        cur.execute(sql_delete_DB_router, data_delete_DB_router)
        delete_DB_router = cur.fetchall()
        conn.commit()
        print('delete_DB_router', delete_DB_router)

        # DELETE ALL SCHEDULES
        # sql = "UPDATE jobscron SET active = FALSE, deactivate_reason = %s WHERE owner = %s and active = TRUE RETURNING id, original;"
        sql_user_schedules = "SELECT id FROM jobscron WHERE owner = %s;"
        data_user_schedules = (owner, )
        cur.execute(sql_user_schedules, data_user_schedules)
        user_schedules = cur.fetchall()
        conn.commit()
        print('user_schedules', user_schedules)

        # DELETE ALL THE SCHEDULES ASSOCAITED WITH THESE JOBS
        if user_schedules and len(user_schedules) > 0:
            session = requests.Session()
            for schedule in user_schedules:
                print("removing ", schedule[0])
                session.delete('{}/{}'.format(schedulerEndpoint, schedule[0]))

            # sql_delete_user_schedules = "SELECT id FROM jobscron WHERE owner = %s RETURNING id;"
            sql_delete_user_schedules = "DELETE FROM jobscron WHERE owner = %s RETURNING id;"
            data_delete_user_schedules = (owner, )
            cur.execute(sql_delete_user_schedules, data_delete_user_schedules)
            deleted_schedule = cur.fetchall()
            conn.commit()
            print('deleted_schedule', deleted_schedule)

        # DELETE USER - CHANGE NAME TO DELETED WITH DATE AND ACTIVE = FALSE
        sql2 = "UPDATE owner SET (active, email, secret) = (%s, %s, %s) WHERE id = %s RETURNING id"
        # sql_delete_user = "DELETE FROM jobscron WHERE owner = %s;"
        newqEmail = email+"-castodiaDeleted-"+str(datetime.datetime.now())
        data2 = (False, newqEmail, None, owner)
        cur.execute(sql2, data2)
        deletedOwner = cur.fetchone()

        print('deletedOwner', deletedOwner)

        conn.commit()
        cur.close()
        conn.close()

        if deletedOwner:
            return True, "Your account has been deleted"
        else:
            return False, "Could not delete your account, please contact us at support@castodia.com"
    except Exception as e:
        print('Psycopg2 error: ', e)
        return False, "Server error, please contact us at support@castodia.com"


#######################
# DATABASE MANAGEMENT #
#######################


def addUpdateDB(owner, nickname, creds, dbType):
    # GET CONNECT INFO
    try:
        conn, cur = sc(castConnectInfo, False)

        sql = "SELECT id FROM dbRouter WHERE owner = %s AND nickname = %s AND active = TRUE;"
        data = (owner, nickname)
        cur.execute(sql, data)
        dbID = cur.fetchone()
        conn.commit()
        # IF RECORD ALREADY EXISTS
        if dbID:
            # UPDATE RECORD
            sql = "UPDATE dbRouter SET (creds, dbtype) = (%s, %s) WHERE id = %s RETURNING id;"
            data = (creds, dbType, dbID[0])
            cur.execute(sql, data)
            updated = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()

            if updated:
                return updated[0], "updated"
            else:
                return False, False
        else:
            # ADD NEW DB CREDS
            sql = "INSERT INTO dbRouter (owner, nickname, creds, dbtype) VALUES (%s,%s,%s,%s) RETURNING id;"
            data = (owner, nickname, creds, dbType)
            cur.execute(sql, data)
            dbRouterID = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()

            if dbRouterID:
                return dbRouterID[0], "added"
            else:
                return False, False
    except Exception as e:
        print('Psycopg2 dbRouter  error', e)
        return False


def deleteDB(owner, nickname):
    # DELETE DATABASE - deleted creds
    try:
        conn, cur = sc(castConnectInfo, False)

        disableDBRoutersql = "UPDATE dbRouter SET (active, nickname, creds) = (%s, %s, %s) WHERE owner = %s ANd nickname = %s RETURNING id"
        newqNickname = nickname+"-castodiaDeleted-" + \
            str(datetime.datetime.now())
        disableDBRouterdata = (False, newqNickname, None, owner, nickname)
        cur.execute(disableDBRoutersql, disableDBRouterdata)
        updatedDRRouter = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if updatedDRRouter:
            return True, "Database has been deleted"
        else:
            return False, "Could not delete database"
    except Exception as e:
        print('Psycopg2 UPDATE dbRouter error', e)
        return False


def getListOfDBs(owner):
    # GET LIST OF DBS
    try:
        conn, cur = sc(castConnectInfo, False)

        sql = "SELECT nickname, dbtype, defaultdb FROM dbRouter WHERE owner = %s AND active = TRUE ORDER BY nickname ASC;"
        data = (owner,)
        cur.execute(sql, data)

        # GET LIST OF SAVED QUERIES
        results = cur.fetchall()
        defaultDB = None
        savedDBs = []
        dbtypes = {}
        if results is None:
            return False

        else:
            for i in results:
                savedDBs.append(i[0])
                dbtypes[i[0]] = i[1]
                if i[2] == True:
                    defaultDB = i[0]

            conn.commit()
            cur.close()
            conn.close()

            if len(savedDBs) > 0:
                return {"listOfDB": savedDBs, "dbtypes": dbtypes, "defaultDB": defaultDB}
            else:
                return False
    except Exception as e:
        print('Psycopg2 SELECT dbRouter error', e)
        return False


def setDefaultDB(owner, nickname):
    try:
        conn, cur = sc(castConnectInfo, False)

        sql1 = "UPDATE dbRouter SET defaultdb = FALSE WHERE owner = %s"
        data1 = (owner, )

        sql2 = "UPDATE dbRouter SET defaultdb = TRUE WHERE owner = %s AND nickname = %s RETURNING id"
        data2 = (owner, nickname)

        cur.execute(sql1, data1)
        cur.execute(sql2, data2)
        defaultDB = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if defaultDB:
            return defaultDB[0]
        else:
            return False
    except Exception as e:
        print('Psycopg2 UPDATE dbRouter error', e)
        return False


def testConn(host, port, database, user, password):
    try:
        db = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=5)
        if db:
            return True
        else:
            return False
    except Exception as e:
        print('Psycopg2 connection error: ', e)
        return False


def testConnection(dbConnectInfo, owner):
    if dbConnectInfo['dbType'] == "postgres":
        dbHost = dbConnectInfo['dbHost']
        dbPort = dbConnectInfo['dbPort']
        dbUser = dbConnectInfo['dbUser']
        dbPass = dbConnectInfo['dbPass']
        dbName = dbConnectInfo['dbName']
        return testConn(dbHost, dbPort, dbName, dbUser, dbPass)
    else:
        # SETUP LAMBDA INVOKING
        try:
            invokeLam = boto3.client("lambda", region_name="us-west-1")

            # STANDARD PAYLOAD
            payload = {
                "connectInfo": dbConnectInfo,
                "qText": "",
                "qVars": "",
                "query": "testDatabase",  # SERVES AS AN ACTION VARIABLE IN CONNECTOR LAMBDA
                "InvType": "RequestResponse"  # BY DEFAULT InvocationType IS RequestResponse
            }

            print('SENDING TO SERVER FOR TESTING')
            print(payload)
            results = invokeLam.invoke(
                FunctionName=lambdaNames[dbConnectInfo["dbType"]], InvocationType="RequestResponse", Payload=json.dumps(payload, default=str))
            print('GOT BACK FROM TESTING SERVER')
            print(results)
            # READ PAYLOAD
            readPayload = results["Payload"].read()
            # DECODE PAYLOAD
            decodePayload = readPayload.decode()

            testedConnection = json.loads(decodePayload)
            print('testedConnection')
            print(testedConnection)

            if testedConnection:
                return True  # buildResponse(200, "Connection is good!")
            else:
                # problem = json.dumps(
                #     {"dbHost": dbHost, "dbPort": dbPort, "dbName": dbName, "dbUser": dbUser, "dbPass": dbPass})
                # adderrorsLogs(owner, json.dumps(dbConnectInfo),
                #               "errorTestConnectInfo", problem)
                # buildResponse(501, "Could not connect to your database")
                return False

        except Exception as e:
            print('PostgresConnector error: ', e)
            return False  # buildResponse(500, "bad lambda")


############
# ENCRYPTION
############


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


def getCreds(owner, nickname):
    conn, cur = sc(castConnectInfo, False)
    sql = "SELECT creds FROM dbRouter WHERE owner = %s AND nickname = %s AND active = TRUE;"
    data = (owner, nickname)
    cur.execute(sql, data)
    creds = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if creds:
        f = createFernet(owner)
        creds2 = json.loads(f.decrypt(bytes(creds[0])).decode("utf-8"))
        return creds2
    else:
        return False


####################
# QUERY MANAGEMENT #
####################
def AddCustQueryV1(QID, qVersion, qText, qVars, qOptions):
    # INSERT CUSTOM QUERY
    conn, cur = sc(castConnectInfo)

    sql = "INSERT INTO custQueryV1 (QID, qVersion, qText, qVars, qOptions) VALUES (%s,%s,%s,%s,%s) RETURNING QID;"
    data = (QID, qVersion, qText, json.dumps(
        qVars, default=str), json.dumps(qOptions, default=str))
    cur.execute(sql, data)

    queryRecID = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return queryRecID


def addQueryRouter(owner, qTableName, QID, qVersion, qName, qDescr, nickname):
    # SAVE IN ROUTER
    conn, cur = sc(castConnectInfo)
    sql = "SELECT id FROM dbrouter where owner = %s and nickname = %s and active = TRUE;"
    data = (owner, nickname)
    cur.execute(sql, data)
    dbrouter = cur.fetchone()

    if dbrouter:
        sql2 = "INSERT INTO queryRouter (owner, qTableName, QID, qVersion, qName, qDescr, dbrouterid, active) VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE) RETURNING id;"
        data2 = (owner, qTableName, QID, qVersion, qName, qDescr, dbrouter[0])
        cur.execute(sql2, data2)
        queryRouterID = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if queryRouterID:
            return queryRouterID[0]
        else:
            return False
    else:
        conn.commit()
        cur.close()
        conn.close()
        return False


def disableQuery(qName, owner):
    conn, cur = sc(castConnectInfo)

    # newqName = qName + " (castodiaDeleted-" + \
    #     str(datetime.datetime.now()) + ")"

    # TODO: DELETE QUERIES
    # sql = "UPDATE queryRouter SET active = FALSE, qName = %s WHERE owner = %s AND qName = %s RETURNING id;"
    # data = (newqName, owner, qName)

    sql = "DELETE FROM queryRouter WHERE  owner = %s AND qName = %s RETURNING id"
    data = (owner, qName)
    cur.execute(sql, data)

    deletedQueryID = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()
    if deletedQueryID:
        return deletedQueryID[0]
    else:
        return None


def updateQueryRouter(qVersion, qDescr, qName, nickname, owner, QID):
    # UPDATE ROUTER
    conn, cur = sc(castConnectInfo)

    sql = "SELECT id FROM dbrouter where owner = %s and nickname = %s and active = TRUE;"
    data = (owner, nickname)
    cur.execute(sql, data)
    dbrouter = cur.fetchone()

    if dbrouter:
        sql2 = "UPDATE queryRouter SET qVersion = %s, qDescr = %s, qName = %s, dbrouterid = %s WHERE owner = %s AND QID = %s AND active = TRUE RETURNING id;"
        data2 = (qVersion, qDescr, qName, dbrouter[0], owner, QID)
        cur.execute(sql2, data2)
        updatedQueryRouterID = cur.fetchone()

        conn.commit()
        cur.close()
        conn.close()

        if updatedQueryRouterID:
            return updatedQueryRouterID[0]
        else:
            return False
    else:

        conn.commit()
        cur.close()
        conn.close()
        return False


def getQIDFromQueryRouter(qName, owner):
    # GET QID FROM ROUTER GIVEN QNAME AND USER
    conn, cur = sc(castConnectInfo)

    sql = "SELECT QID FROM queryRouter WHERE qName = %s AND owner = %s AND active = TRUE;"
    data = (qName, owner)
    cur.execute(sql, data)

    QID = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return QID


def getQueryByName(owner, qName):
    conn, cur = sc(castConnectInfo, True)

    sql = '''
    SELECT custQueryV1.*, queryRouter.*, dbrouter.nickname FROM queryRouter
    INNER JOIN custQueryV1 ON custQueryV1.QID = queryRouter.QID and custQueryV1.qVersion = queryRouter.qVersion
    INNER JOIN dbrouter ON dbrouter.id = queryrouter.dbrouterid
    WHERE queryRouter.owner = %s AND queryRouter.qName = %s AND queryRouter.active = TRUE;'''
    data = (owner, qName)
    cur.execute(sql, data)

    queryData = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if queryData:
        return dict(queryData)
    else:
        return None


def getQueryRouterByName(owner, qName):
    conn, cur = sc(castConnectInfo)

    sql = "SELECT id FROM queryRouter WHERE owner = %s AND qName = %s AND active = TRUE;"
    data = (owner, qName)
    cur.execute(sql, data)

    queryRouterID = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if queryRouterID:
        return queryRouterID[0]
    else:
        return None


def getSavedQueries(owner):
    try:
        # GET ALL SAVED QUERIES
        conn, cur = sc(castConnectInfo)
        sql = "SELECT qName, id FROM queryRouter WHERE owner = %s AND active = TRUE ORDER BY qname ASC;"
        data = (owner,)
        cur.execute(sql, data)

        savedQueries = [{
            "id": str(r[1]),
            "name": r[0]
        } for r in cur.fetchall()]

        conn.commit()
        cur.close()
        conn.close()
        return savedQueries
    except Exception as e:
        print(e)
        return False


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

####################
# SHEET MANAGEMENT #
####################


def addSheetInfo(SSID, sheetID, sheetName):
    # SHEET INFO
    conn, cur = sc(castConnectInfo)

    sql = "INSERT INTO sheetInfo (SSID, sheetID, sheetName) VALUES (%s,%s,%s) RETURNING id;"
    data = (SSID, sheetID, sheetName)
    cur.execute(sql, data)

    sheetInfoID = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return sheetInfoID


def getAddSheetInfo(SSID, SSName, sheetID, sheetName):
    # GET SHEET INFO OR ADD IF DOESN'T EXIST
    conn, cur = sc(castConnectInfo)
    # SELECT DATA
    sql = "SELECT id FROM sheetInfo WHERE SSID = %s AND sheetID = %s;"
    data = (SSID, sheetID)
    cur.execute(sql, data)

    sheetInfoID = cur.fetchone()
    if sheetInfoID:
        result = sheetInfoID[0]
    else:
        # INSERT NEW IF DOESN'T EXIST
        sql2 = "INSERT INTO sheetInfo (SSID, SSName, sheetID, sheetName) VALUES (%s,%s,%s,%s) RETURNING id;"
        data2 = (SSID, SSName, sheetID, sheetName)
        cur.execute(sql2, data2)
        sheetInfoID = cur.fetchone()

        if sheetInfoID:
            result = sheetInfoID[0]
        else:
            result = None

    conn.commit()
    cur.close()
    conn.close()
    return result


##########
# SCHEDULE
##########

# ADD JOB
def saveSchedulePg(owner, queryRouterID, sheetInfoID, schedules, frequency, repeating, timezone, humanReadableStr, original, qoptions, active):
    print('in saveSchedulePg')
    try:
        conn, cur = sc(castConnectInfo)

        # ADD NEW SCHEDULE
        sql = """INSERT INTO jobscron (owner, queryRouterID, sheetInfoID, schedules, repeating, timezone, humanReadableStr, original, qoptions, active)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"""
        data = (owner, queryRouterID, sheetInfoID, json.dumps(schedules),
                repeating, timezone, humanReadableStr, original, json.dumps(qoptions), active)
        cur.execute(sql, data)
        jobID = cur.fetchone()
        conn.commit()

        # ADD RECORDS FOR STATS
        sql_stat = f"INSERT INTO schedule_stats (jobid, success, fail, frequency) VALUES ('{jobID[0]}', 0, 0, '{frequency}') RETURNING id;"
        cur.execute(sql_stat, )
        inserted = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if jobID:
            return True, jobID[0]
        else:
            return False, None
    except Exception as e:
        print(e)
        return False, None

# SAVE SCHEDULE


def saveScheduleMG(schedules, repeating, timezone, humanReadableStr, jobID, owner):
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
        # result = requests.post(schedulerEndpoint, data=json.dumps(data))
        header = {'Content-Type': 'application/json; charset=UTF-8'}
        session = requests.Session()
        result = session.post(
            schedulerEndpoint, json.dumps(data), headers=header)
        print('result')
        print(result)
    except Exception as e:
        print('GOT AN ERROR!')
        print(e)
        return False, None

    if result.status_code == 201:  # 201 is created
        return True, result.text  # if successful, return the json contained created schedule
    else:
        print(result.status_code)
        print(result.text)
        return False, None  # return nothing otherwise

# GET ALL JOBS


def getAllJobs(owner):
    # SPECIAL CURSOR, RETURNS VALS AS A DICT
    conn, cur = sc(castConnectInfo, True)
    sql = "SELECT jobscron.*, sheetInfo.sheetName, sheetInfo.ssname, queryRouter.qName FROM jobscron \
      FULL OUTER JOIN sheetInfo ON jobscron.sheetInfoID = sheetInfo.id \
      FULL OUTER JOIN queryRouter ON jobscron.queryRouterID = queryRouter.id AND queryRouter.active = TRUE \
      WHERE jobscron.owner = %s;"
    data = (owner, )
    cur.execute(sql, data)

    # HOW TO ENSURE THRERE IS ONLY ONE?
    features = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    if features:
        return features
    else:
        return None


# GET SHEET JOBS
def getJobsForSheet(owner, SSID, sheetName, sheetID):
    # SPECIAL CURSOR, RETURNS VALS AS A DICT
    conn, cur = sc(castConnectInfo, True)
    sql = "SELECT jobscron.*, sheetInfo.sheetName, sheetInfo.ssname, queryRouter.qName FROM jobscron \
      FULL OUTER JOIN sheetInfo ON jobscron.sheetInfoID = sheetInfo.id \
      FULL OUTER JOIN queryRouter ON jobscron.queryRouterID = queryRouter.id AND queryRouter.active = TRUE \
      WHERE jobscron.owner = %s AND sheetInfo.SSID = %s AND sheetInfo.sheetName = %s;"
    data = (owner, SSID, sheetName)
    cur.execute(sql, data)

    # HOW TO ENSURE THERE IS ONLY ONE?
    features = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    if features:
        return features
    else:
        return None


def deleteSchedule(JobID):
    # FIRST, DELETE JOB IN SCHEDULER
    session = requests.Session()
    result = session.delete('{}/{}'.format(schedulerEndpoint, JobID))

    # IF SUCCESS, DELETE THE JOB IN POSTGRES
    if result.status_code == 200:

        conn, cur = sc(castConnectInfo)

        sql = "DELETE FROM jobscron WHERE id = %s RETURNING id;"
        data = (JobID, )
        cur.execute(sql, data)

        deletedJobID = cur.fetchone()

        conn.commit()
        cur.close()
        conn.close()

        if deletedJobID:
            print('deleting the job from agenda')
            return True, deletedJobID[0]

        else:
            return False, None
    else:
        return False, None


# GET LIMITS
def getUsageInfo(owner):
    try:
        conn, cur = sc(castConnectInfo, True)
        sql = """select p.*, ut.*, o.plan_end from owner as o
            inner join plan as p on o.plan = p.name
            inner join usage_total as ut on ut.owner = o.id
            where o.id = %s
        """
        data = (owner, )
        cur.execute(sql, data)
        creds = cur.fetchone()
        conn.commit()
        cur.close()
        return creds
    except Exception as e:
        print(e)
        return False


# ON START CALL
def getDataOnStart(owner):
    dbList = getListOfDBs(owner)
    savedQueries = getSavedQueries(owner)
    usageInfo = getUsageInfo(owner)
    return dbList, savedQueries, usageInfo


def getDBDataOnStart(owner):
    dbList = getListOfDBs(owner)
    usageInfo = getUsageInfo(owner)
    return dbList, usageInfo

# REACTIVATE SCHEDULE


def reactivateSchedule(owner, jobID):

    try:
        conn, cur = sc(castConnectInfo, True)

        sql_owner_active = "SELECT active from owner WHERE id = %s"
        data_owner_active = (owner,)
        cur.execute(sql_owner_active, data_owner_active)
        active_owner = cur.fetchone()

        if active_owner['active'] == False:
            return False, "Your account is inactive"

        sql1 = "SELECT * from jobscron WHERE id = %s"
        data1 = (jobID,)
        cur.execute(sql1, data1)
        originalSchedule = cur.fetchone()
        if originalSchedule:
            # ADD BACK TO MONGODB
            successMg, savedScheduleMg = saveScheduleMG(
                json.loads(originalSchedule["schedules"]),  # COULD BE JSON
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
                return reactivatedJob, ""
            else:
                return False, "Could not reactivate the job"
        else:
            return False, "Could not reactivate the job"
    except Exception as e:
        print('Error reactivating schedules: ', e)
        return False, "Internal server error"


def reactivateSystemFailedJobs(owner):
    try:
        conn, cur = sc(castConnectInfo, True)
        sql1 = "SELECT * from jobscron WHERE owner = %s AND deactivate_reason = 'backend' AND active = FALSE;"
        data1 = (owner,)
        cur.execute(sql1, data1)
        deactivatedSchedules = cur.fetchall()

        if deactivatedSchedules:
            succeeded = []
            failed = []
            for originalSchedule in deactivatedSchedules:
                # ADD BACK TO MONGODB
                successMg, savedScheduleMg = saveScheduleMG(
                    json.loads(originalSchedule["schedules"]),  # COULD BE JSON
                    originalSchedule["repeating"],
                    originalSchedule["timezone"],
                    originalSchedule["humanreadablestr"],
                    originalSchedule['id'],
                    owner
                )

                # REACTIVATE IN PG
                reactivatedJob = None
                if successMg and savedScheduleMg:
                    sql = "UPDATE jobscron SET active = TRUE, deactivate_reason = '' WHERE owner = %s AND id = %s RETURNING id"
                    data = (owner, originalSchedule['id'])
                    cur.execute(sql, data)
                    reactivatedJob = cur.fetchone()
                    conn.commit()

                if reactivatedJob:
                    succeeded.append(reactivatedJob)
                else:
                    failed.append(reactivatedJob)

            cur.close()
            conn.close()
            payload = {
                "succeeded": succeeded,
                "succeededCount": len(succeeded),
                "failed": failed,
                "failedCount": len(succeeded),
                "total": len(deactivatedSchedules)
            }
            return json.dumps(payload, default=str)
        else:
            return False
    except Exception as e:
        print(e)
        return False


##################
# /user Endpoint Handler
##################


def lambda_handler(event=None, context=None):
    # GET DATA
    data = json.loads(event['body'])
    
    # data = event['body']
    # NEW USER EMAIL FROM EVENT
    email = data["email"]
    action = data['action']

    # VERIFY USER
    owner = getOwner(email)

    if action == "testAction":
        print('inside test Action')
        return buildResponse(200, "Test Action Success!")

    elif action == "adminReactivateSchedules":
        owner = data['owner']
        reactivatedSchedules = reactivateSystemFailedJobs(owner)
        if reactivatedSchedules:
            buildResponse(200, reactivatedSchedules)
        else:
            buildResponse(500, "Failed to reactivate all inactive schedules")

    # DOES NOT NEED TO BE AUTHORIZED
    elif action == "newUser":
        actionId = logActionStartToDb(None, "newUser", data)
        addedNewuser = addNewUser(email)
        if addedNewuser:
            logActionSuccessToDb(actionId)
            return buildResponse(200, "New user has been created")
        else:
            return buildResponse(501, "Could not add a new user")

    # TODO: VERIFY LOGIC
    elif action == "addUpdateDB":
        actionId = logActionStartToDb(owner, "addUpdateDB", data)

        secret = data["secret"]
        dbConnectInfo = data['dbConnectInfo']

        if dbConnectInfo == None:
            return buildResponse(400, "Database credentials are missing")

        verified, owner, active = verifyUser(email, secret)

        if owner and verified:

            # TEST CONNECTION
            testedConnection = testConnection(dbConnectInfo, owner)
            if testedConnection == False:
                problem = "Could not connect to database with given credentials"
                adderrorsLogs(owner, data, "errorTestConnectInfo", problem)
                return buildResponse(400, "Could not connect to your database")

            # ENCRYPT DB CREDS
            f = createFernet(owner)
            message = json.dumps(dbConnectInfo)
            creds = f.encrypt(bytes(message, "utf-8"))

            # SAVE RECORDS
            dbType = dbConnectInfo["dbType"]
            nickname = dbConnectInfo['nickname']
            resltID, status = addUpdateDB(owner, nickname, creds, dbType)

            if resltID:
                # RETURNIN WITH STATUS, LIKE DATABASE ADDED/UPDATED
                logActionSuccessToDb(actionId, True, status)
                return buildResponse(200, "Database "+status)
            else:
                problem = json.dumps(
                    {"owner": str(owner), "nickname": nickname, "creds": creds}, default=str)
                adderrorsLogs(owner, data, "errorAddUpdateDB", problem)
                return buildResponse(501, "Could not add database")

        else:
            return buildResponse(401, "You are not authorized")

    elif action == "testConnection":
        # TODO: ADD AUTHORIZATION
        # GET DB CREDS
        actionId = logActionStartToDb(owner, 'testConnection', data)
        dbConnectInfo = data['dbConnectInfo']
        testedConnection = testConnection(dbConnectInfo, owner)

        if testedConnection == True:
            logActionSuccessToDb(actionId)
            return buildResponse(200, "Connection is good!")
        else:
            problem = "Failed to test connection"
            adderrorsLogs(owner, data, "errorTestConnectInfo", problem)
            return buildResponse(501, "Could not connect to your database")

    elif action == "deleteUser":
        actionId = logActionStartToDb(owner, "deleteUser", data)

        # DELETE ALL THE PREVIOUS RECORDS
        deletedUser, message = deleteUser(email)
        if deletedUser:
            logActionSuccessToDb(actionId)
            return buildResponse(200, message)
        else:
            return buildResponse(501, message)

    ##################
    # AUTHORIZE USER #
    secret = data["secret"]

    verified, owner, active = verifyUser(email, secret)

    if owner == False or verified == False:
        problem = json.dumps({"owner": str(owner)})
        adderrorsLogs(owner, data, "Failed to autorize user", problem)
        return buildResponse(500, "Failed to autorize user")

    if action == 'getStartData':
        dbList, savedQueries, usageInfo = getDataOnStart(owner)
        payload = {
            "dbList": dbList,
            "savedQueries": savedQueries,
            "usageInfo": usageInfo
        }
        return buildResponse(200, json.dumps(payload, default=str))

    elif action == 'getDBDataOnStart':
        dbList, usageInfo = getDBDataOnStart(owner)
        payload = {
            "dbList": dbList,
            "usageInfo": usageInfo
        }
        return buildResponse(200, json.dumps(payload, default=str))

    elif action == 'getUsageInfo':
        usageInfo = getUsageInfo(owner)
        payload = {
            "usageInfo": usageInfo
        }
        return buildResponse(200, json.dumps(payload, default=str))

    elif action == "deleteDB":
        # DELETE DATABASE
        nickname = data['nickname']
        logActionStartToDb(owner, "deleteDB", data)

        deletedDB = deleteDB(owner, nickname)
        if deletedDB:
            DBList = getListOfDBs(owner)
            return buildResponse(200, DBList)
        else:
            problem = json.dumps(
                {"email": email, "nickname": nickname}, default=str)
            adderrorsLogs(owner, data, "errorDeleteDB", problem)
            return buildResponse(501, "Could not delete database")

    elif action == "getListOfDatabases":
        actionId = logActionStartToDb(owner, "getListOfDatabases", data)

        DBList = getListOfDBs(owner)
        if DBList:
            logActionSuccessToDb(actionId, True, {"DBList": DBList})
            return buildResponse(200, DBList)
        else:
            return buildResponse(501, "No Databases found")

    elif action == "setDefaultDB":
        actionId = logActionStartToDb(owner, "setDefaultDB", data)
        nickname = data["nickname"]

        # VERIFY USER
        defaultDB = setDefaultDB(owner, nickname)
        if defaultDB:
            logActionSuccessToDb(actionId)
            return buildResponse(200, "Default database as been updated")
        else:
            problem = json.dumps({"owner": str(owner)})
            adderrorsLogs(
                owner, data, "errorUpdatingDefaultDatabase", problem)
            return buildResponse(501, "Could not update default database")

    elif action == "getConnectInfo":
        actionId = logActionStartToDb(owner, "getConnectInfo", data)
        nickname = data['nickname']

        creds = getCreds(owner, nickname)
        if creds:
            #logActionSuccessToDb(actionId, True, {"creds": creds})
            return buildResponse(200, creds)
        else:
            problem = json.dumps(
                {"owner": str(owner), "nickname": nickname})
            adderrorsLogs(owner, data, "errorGetConnectInfo", problem)
            return buildResponse(501, "Could not find credentials")

    elif action == "saveCustQuery":
        if active == False:
            return buildResponse(403, "Your account is inactive")

        # GET QUERY
        query = data['query']
        qText = query['qText']

        # MAKE SURE THERE ARE NO MALICIOUS WORDS IN THE QUERY
        readOnly = verifyCustomQuery(qText)

        if readOnly == False:
            problem = json.dumps({"qText": query})
            adderrorsLogs(owner, data, "badCustomQueryWrite", query)
            return buildResponse(501, "Only read queries allowed!")

        # GET ADDITIONAL DATA FOR SAVING
        qOptions = query['qOptions']
        # GET QUERY VERSION
        qVersion = query['qVersion']
        qName = query['qName']
        qDescr = query['qDescr']
        nickname = query["nickname"]
        # TODO: CHECK THAT THESE VARS ARE PRESENT
        qVars = ""

        # ADD QUERY FIRST
        if qVersion == 1:
            # GENERATE QID
            QID = str(uuid.uuid4())
            # ADD NEW RECORD
            AddCustQueryV1(QID, qVersion, qText, qVars, qOptions)
            queryRouterID = addQueryRouter(
                owner, qTableName, QID, qVersion, qName, qDescr, nickname)
        else:
            # IF SAVING NEW VESION, GET QID
            QID = getQIDFromQueryRouter(qName, owner)
            # ADD NEW QID RECORD
            AddCustQueryV1(QID, qVersion, qText, qVars, qOptions)
            queryRouterID = updateQueryRouter(
                qVersion, qDescr, qName, nickname, owner, QID)

        if queryRouterID:
            return buildResponse(200, "Your query has been saved!")
        else:
            # LOG ERROR
            problem = json.dumps(
                {
                    "owner": owner,
                    "QID": QID,
                    "qVersion": qVersion,
                    "qName": qName,
                    "qDescr": qDescr,
                    "nickname": nickname
                })
            adderrorsLogs(owner, data, "noQuerySaved", problem)
            return buildResponse(501, "Could not save your query.")

    elif action == "getCustQueryByName":
        # GET SHEET INFO
        qName = data["qName"]
        # GET RESULTS
        result = getQueryByName(owner, qName)
        if result:
            return buildResponse(200, json.loads(json.dumps(result, default=serializer)))
        else:
            # LOG ERROR
            problem = json.dumps({"qName": qName})
            adderrorsLogs(owner, data, "noGetCustQueryByName", problem)
            return buildResponse(501, "No custom query with this name")

    elif action == "getSavedQueries":
        result = getSavedQueries(owner)
        if result:
            return buildResponse(200, result)
        else:
            # LOG ERROR
            problem = json.dumps({"owner": str(owner)}, default='string')
            adderrorsLogs(owner, data, "noGetSavedQueries", problem)
            return buildResponse(501, "No saved queries")

    elif action == "deleteQuery":
        qName = data['qName']
        disabledQuery = disableQuery(qName, owner)

        if disabledQuery:
            return buildResponse(200, "Query has been deleted")
        else:
            # LOG ERROR
            problem = json.dumps({"qName": qName, "owner": owner})
            adderrorsLogs(owner, data, "noDeleteQuery", problem)
            return buildResponse(500, "Could not delete your query")

    elif action == "saveSchedule":
        if active == False:
            return buildResponse(403, "Your account is inactive or you have exhausted your quota")

        print("SAVING SCHEDULE!!!")
        #################
        # SAVE SHEET INFO
        sheetData = data["sheetData"]
        SSID = sheetData['SSID']
        SSName = sheetData['SSName']
        sheetID = sheetData['sheetID']
        sheetName = sheetData['sheetName']

        sheetInfoID = getAddSheetInfo(SSID, SSName, sheetID, sheetName)

        ##########
        # SCHEDULE
        scheduleData = data["scheduleData"]

        # DECODE VALUES
        # query = data["query"]
        qName = scheduleData['qName']
        schedules = scheduleData["schedules"]
        frequency = scheduleData["frequency"]
        repeating = scheduleData["repeating"]
        timezone = scheduleData["timezone"]
        humanReadableStr = scheduleData["humanReadableStr"]
        qoptions = scheduleData["qoptions"]

        # CHECK IF ACCOUNT IS ACTIVE

        original = json.dumps({
            "qName": qName,
            "sheetData": sheetData,
            "scheduleData": scheduleData
        })

        # GET QUERY ROUTER ID
        queryRouterID = getQueryRouterByName(owner, qName)
        if queryRouterID == None:
            return buildResponse(500, "Could not find database credentials")

        successPg, savedScheduleID = saveSchedulePg(
            owner, queryRouterID, sheetInfoID, schedules, frequency, repeating, timezone, humanReadableStr, original, qoptions, True)
        print('FROM POSTGTRS', successPg, savedScheduleID)

        if successPg == False:
            return buildResponse(500, "Could not save schedule")

        # CREATE SCHEDULES
        successMg, savedScheduleMg = saveScheduleMG(
            schedules, repeating, timezone, humanReadableStr, savedScheduleID, owner)
        print('FROM MONGO', successMg, savedScheduleMg)

        if successMg:
            return buildResponse(200, savedScheduleMg)
        else:
            # DELETE NEWLY ADDED JOBID
            conn, cur = sc(castConnectInfo)
            sql = "DELETE FROM jobscron WHERE id = %s RETURNING id"
            data = (savedScheduleID, )
            cur.execute(sql, data)
            conn.commit()
            cur.close()
            conn.close()
            # TODO: ADD ERROR LOG
            return buildResponse(500, "Could not save schedule")

    elif action == "getAllJobs":
        jobs = getAllJobs(owner)
        if jobs:
            return buildResponse(200, json.loads(json.dumps(jobs, default=serializer)))
        else:
            # TODO: ADD ERROR LOG
            return buildResponse(500, "No jobs schedules yet")

    elif action == "getSheetJobs":

        sheetData = data["sheet"]
        sheetID = sheetData['sheetID']
        SSID = sheetData['SSID']
        sheetName = sheetData['sheetName']

        jobs = getJobsForSheet(owner, SSID, sheetName, sheetID)

        if jobs:
            return buildResponse(200, json.loads(json.dumps(jobs, default=str)))
        else:
            # az add error here
            return buildResponse(500, "No jobs schedules for this sheet")

    elif action == "deleteJob":
        JobID = str(data["JobID"])
        success, deletedJob = deleteSchedule(JobID)

        if success and str(deletedJob) == JobID:
            return buildResponse(200, deletedJob)
        else:
            return buildResponse(500, "Could not delete job")

    elif action == "reactivateSchedule":
        JobID = data["JobID"]

        reactivatedJob, reactivate_message = reactivateSchedule(owner, JobID)
        if reactivatedJob:
            payload = {
                "JobID": JobID,
                "message": "Schedule is reactivated"
            }
            return buildResponse(200, json.dumps(payload, default=str))
        else:
            payload = {
                "JobID": JobID,
                "message": reactivate_message
            }

            return buildResponse(500, json.dumps(payload, default=str))

    else:
        actionId = logActionStartToDb(owner, "userActionNotFound", data, {
                                      "actionType": action}, False)
        return "Action type not found", 404
