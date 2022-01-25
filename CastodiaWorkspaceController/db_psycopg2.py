import psycopg2
from psycopg2.extras import RealDictCursor


castConnectInfo = {
    "dbHost": "172.17.0.1",
    "dbName": "postgres",
    "dbPort": 5432,
    "dbUser": "postgres",
    "dbPass": "password"
}

class DB_postgres:
    def __init__(self) -> None:
        self.conn = psycopg2.connect(
            host=castConnectInfo.get("dbHost"),
            port=int(castConnectInfo.get("dbPort")),
            database=castConnectInfo.get("dbName"),
            user=castConnectInfo.get("dbUser"),
            password=castConnectInfo.get("dbPass"),
        )
        self.conn.autocommit = True

    def reconnect(self):
        # close any open connections
        # TODO: what if i already closed it?
        self.conn.close()

        # start new connection
        self.conn = psycopg2.connect(
            host=castConnectInfo.get("dbHost"),
            port=int(castConnectInfo.get("dbPort")),
            database=castConnectInfo.get("dbName"),
            user=castConnectInfo.get("dbUser"),
            password=castConnectInfo.get("dbPass"),
        )
        self.conn.autocommit = True

    def close(self):
        self.conn.close()

    def query_get(self, sql, data, fetch_all=False, real_dict=True):
        try:
            return self._query_get(sql, data, fetch_all, real_dict)
        except psycopg2.ProgrammingError as e:
            self.conn.rollback()
            raise e
        except psycopg2.InterfaceError as e:
            # TODO: test if this actually reconnets on failure
            self.reconnect()
            return self._query_get(sql, data, fetch_all, real_dict)
        except Exception as e:
            raise e

    def _query_get(self, sql, data, fetch_all, real_dict):
        if real_dict:
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = self.conn.cursor()
        cur.execute(sql, data)
        result = cur.fetchall() if fetch_all else cur.fetchone()
        self.conn.commit()
        cur.close()
        return result

    def query_run(self, sql, data):
        try:
            return self._query_run(sql, data)
        except psycopg2.ProgrammingError as e:
            self.conn.rollback()
            raise e
        except psycopg2.InterfaceError as e:
            self.reconnect()
            return self._query_run(sql, data)
        except Exception as e:
            self.reconnect()
            raise ConnectionError("Failed to connect to database:" + str(e))

    def _query_run(self, sql, data):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, data)
        self.conn.commit()
        result = cur.rowcount
        cur.close()
        return result

    def copy(self, file, table, cols=None):
        try:
            return self._copy(file, table, cols)
        except psycopg2.ProgrammingError as e:
            self.conn.rollback()
            raise e
        except psycopg2.InterfaceError as e:
            self.reconnect()
            return self._copy(file, table)
        except Exception as e:
            self.reconnect()
            raise ConnectionError(
                "Failed to connect to database, please try again later"
            )

    def _copy(self, file, table, cols):
        # NOTE: columns of csv file must match type of table
        cur = self.conn.cursor()
        cur.copy_expert(f"COPY {table} {cols} FROM stdin CSV HEADER;", file)
        self.conn.commit()
        cur.close()


db_pg = DB_postgres()
