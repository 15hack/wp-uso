import os
import re
import sqlite3
from socket import gaierror, gethostbyname
from subprocess import DEVNULL, STDOUT, check_call
from urllib.parse import urlparse

import MySQLdb
import yaml
from bunch import Bunch
from sshtunnel import SSHTunnelForwarder

re_select = re.compile(r"^\s*select\b")
ip_dom = {}


def get_ip(dom):
    if dom in ip_dom:
        return ip_dom[dom]
    try:
        ip = gethostbyname(dom)
    except gaierror as e:
        ip = -1
    ip_dom[dom] = ip
    return ip


def get_yml(path):
    if not os.path.isfile(path):
        return []
    with open(path, "r") as f:
        return list(yaml.load_all(f))


def str_list(s):
    if s is None or len(s) == 0:
        return []
    if isinstance(s, str):
        return s.split()
    return s


def build_result(c, to_tuples=False, to_bunch=False):
    results = c.fetchall()
    if len(results) == 0:
        return results
    if isinstance(results[0], tuple) and len(results[0]) == 1:
        return [a[0] for a in results]
    if to_tuples:
        return results
    cols = [(i, col[0]) for i, col in enumerate(c.description)]
    n_results = []
    for r in results:
        d = {}
        for i, col in cols:
            d[col] = r[i]
        if to_bunch:
            d = Bunch(**d)
        n_results.append(d)
    return n_results


def flat(*args):
    arr = []
    for a in args:
        if isinstance(a, str):
            arr.append(a)
        else:
            for i in a:
                arr.append(i)
    return arr


class DB:
    def __init__(self, server, host, ssh_private_key_password, user, passwd, remote_bind_address='127.0.0.1', remote_bind_port=3306, **kargv):
        self.server = SSHTunnelForwarder(
            host,
            ssh_private_key_password=ssh_private_key_password,
            remote_bind_address=(remote_bind_address, remote_bind_port)
        )
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = None
        self.ip = get_ip(server)
        self.db_ban = str_list(kargv.get("db_ban"))
        self.url_ban = str_list(kargv.get("url_ban"))
        self.dom_ban = str_list(kargv.get("dom_ban"))

    def connect(self):
        self.server.start()
        self.db = MySQLdb.connect(
            host='127.0.0.1',
            port=self.server.local_bind_port,
            user=self.user,
            passwd=self.passwd,
            charset='utf8'
        )

    def close(self):
        self.db.close()
        self.server.stop()

    def isOk(self, url):
        for u in self.url_ban:
            if u in url:
                return False
        return True

    def isOkDom(self, dom):
        if dom.startswith("http"):
            dom = urlparse(dom).netloc
        if get_ip(dom) != self.ip:
            return False
        for d in self.dom_ban:
            if dom == d or dom.endswith("." + d):
                return False
        return True

    def execute(self, sql):
        cursor = self.db.cursor()
        if os.path.isfile(sql):
            with open(sql, 'r') as myfile:
                sql = myfile.read()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return flat(result)

    def multi_execute(self, vals, i_sql, where=None, order=None, debug=None, to_tuples=False):
        cursor = self.db.cursor()
        i_sql = i_sql.strip()

        if isinstance(vals, dict):
            vals = [flat(k, v) for k, v in vals.items()]
        if isinstance(vals[0], str):
            vals = [[v] for v in vals]

        if len(vals) > 1 or where or order:
            sql = "select distinct * from ("
            for v in sorted(vals):
                sql = sql+"(\n"+i_sql.format(*v)+"\n) UNION "

            sql = sql[:-7]
            sql = sql + "\n) T"
        else:
            sql = re_select.sub("select distinct", i_sql)
            sql = sql.format(*vals[0])
        if where:
            sql = sql + " where "+where
        if order:
            sql = sql + " order by "+order

        if debug:
            with open("debug/"+self.host+"_"+debug+".sql", "w") as f:
                f.write(sql)

        cursor.execute(sql)
        #results = cursor.fetchall()
        results = build_result(cursor, to_tuples=to_tuples)
        cursor.close()

        return results
        # return flat(results)


class DBLite:
    def __init__(self, file, total=None):
        self.file = file
        self.con = sqlite3.connect(file)
        self.cursor = self.con.cursor()
        self.count = 0
        self.total = total
        self.tables = {}
        self.load_tables()

    def execute(self, sql_file):
        with open(sql_file, 'r') as schema:
            qry = schema.read()
            self.cursor.executescript(qry)
            self.con.commit()
            if "CREATE TABLE" in qry.upper():
                self.load_tables()

    def load_tables(self):
        self.tables = {}
        for t in self.select("SELECT name FROM sqlite_master WHERE type='table'"):
            self.cursor.execute("select * from "+t+" limit 0")
            self.tables[t] = tuple(col[0] for col in self.cursor.description)

    def insert(self, table, **kargv):
        ok_keys = self.tables[table]
        keys = []
        vals = []
        for k, v in kargv.items():
            if k in ok_keys and v is not None and not(isinstance(v, str) and len(v) == 0):
                keys.append(k)
                vals.append(v)
        sql = "insert into %s (%s) values (%s)" % (
            table, ", ".join(keys), ("?," * len(vals))[:-1])
        self.cursor.execute(sql, vals)
        if self.total is not None:
            self.count = self.count + 1
            print("Creando sqlite {0:.0f}%".format(
                self.count*100/self.total), end="\r")

    def commit(self):
        self.con.commit()

    def close(self):
        self.con.commit()
        self.cursor.close()
        self.con.execute("VACUUM")
        self.con.commit()
        self.con.close()

    def select(self, sql, to_bunch=False, to_tuples=False):
        sql = sql.strip()
        if not sql.lower().startswith("select"):
            sql = "select * from "+sql
        self.cursor.execute(sql)
        r = build_result(self.cursor, to_bunch=to_bunch, to_tuples=to_tuples)
        return r

    def get_sql_table(self, table):
        sql = "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
        self.cursor.execute(sql, (table,))
        sql = self.cursor.fetchone()[0]
        return sql

    def size(self, file=None, suffix='B'):
        file = file or self.file
        num = os.path.getsize(file)
        for unit in ('', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'):
            if abs(num) < 1024.0:
                return ("%3.1f%s%s" % (num, unit, suffix))
            num /= 1024.0
        return ("%.1f%s%s" % (num, 'Yi', suffix))

    def zip(self):
        zip = os.path.splitext(self.file)[0]+".7z"
        if os.path.isfile(zip):
            os.remove(zip)
        cmd = "7z a %s %s" % (zip, self.file)
        check_call(cmd.split(), stdout=DEVNULL, stderr=STDOUT)
        return self.size(zip)


me = os.path.realpath(__file__)
dr = os.path.dirname(me)

DBs = (
    DB(**config) for config in get_yml(dr+"/config.yml")
)
