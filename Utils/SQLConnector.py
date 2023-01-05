import pyodbc
import pandas as pd

dbs = {
    "bi": {
        "server": "ls-bi-corona-an",
        "database": "BI_Corona"
    },
    "xrm": {
        "server": "lsxrm365extrnl",
        "database": "Korona_MSCRM"
    },
    "df": {
        "server": "ls-sql16-bi",
        "database": "DataFactoryMRR"
    }
}


class SQLConnector(object):
    def __init__(self, db='bi'):
        self.server = dbs[db]['server']
        self.database = dbs[db]['database']
        #connstring = "DRIVER={FreeTDS};SERVER=" + self.server + ";PORT=1433;DATABASE=" + self.database + ";UID=BRIUTNT\itay.hazan1;PWD=itay1^;TDS_Version=7.2"
        connstring = "DRIVER={FreeTDS};SERVER=" + self.server + ";PORT=1433;DATABASE=" + self.database + ";UID=BRIUTNT\\redash;PWD=HelloWorld8200Pakar!;TDS_Version=7.2"
        print(connstring)
        self.conn = pyodbc.connect(connstring)
        print('connected!')

    def close_connection(self):
        self.conn.close()

    def query_by_file(self, filename):
        with open(filename, 'r') as f:
            query_str = f.read()
        return self.query_by_str(query_str)

    def query_by_str(self, query_str):
        print('query from', self.server, self.database)
        df = pd.read_sql(query_str, self.conn, parse_dates=False)
        print('results:', df.shape, 'with columns:', df.columns)
        return df
