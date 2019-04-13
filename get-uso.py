#!/usr/bin/python3
import getpass
import os
import sys
import re
from urllib.parse import urlparse

import MySQLdb

if 'MYSQL_USER_READ' in os.environ:
    user, passwd = os.environ['MYSQL_USER_READ'].split()
else:
    user = input("Username: ")
    passwd = getpass.getpass("Password: ")

re_blog = re.compile(r".*_(\d+)_posts$")

def execute(cursor, file):
    _sql = None
    with open(file, 'r') as myfile:
        _sql = myfile.read()
    cursor.execute(_sql)
    return cursor.fetchall()

def get_site_url(cursor, scheme, wp_posts):
    pref=wp_posts[:-5]
    m = re_blog.match(wp_posts)
    if m:
        blog_id = int(m.group(1))
        sql = "select option_value URL from %s.%soptions where option_name='siteurl'" % (scheme, pref)
    else:
        sql = "select option_value URL from %s.%soptions where option_name='siteurl'" % (scheme, pref)
    cursor.execute(sql)
    siteurl = cursor.fetchone()[0]
    _,siteurl = siteurl.split("//",1)
    if siteurl.endswith("/"):
        siteurl=siteurl[:-1]
    return siteurl

def sort_dom(r):
    prs = urlparse("https://"+r[0])
    dom = prs.netloc
    path = prs.path
    dom = dom.split(".")
    dom.reverse()
    return [r[1]] + dom + [path] + list(r[2:])

db = MySQLdb.connect("localhost", user, passwd)

cursor = db.cursor()

results = execute(cursor, 'search-wp.sql')

sql = "select distinct * from ("

for row in results:
    siteurl = get_site_url(cursor,*row)
    sql = sql+'''
	(
		select
            '%s' site,
            count(*) num,
            max(post_date) fin,
            min(post_date) ini
		from %s.%s
		where
		post_status = 'publish' and
		post_type in ('post', 'page')
	)
	UNION
	'''.rstrip() % (siteurl, row[0], row[1])

sql = sql[:-7]

sql = sql + "\n) T"

cursor.execute(sql)

with open("README.md", "w") as f:
    f.write('''
| BLOG | POSTs | Último uso | 1º uso |
|:-----|------:|-----------:|-------:|
    '''.strip())
    results = sorted(cursor.fetchall(), key=sort_dom)
    for row in results:
        url, num, ini, fin = row
        f.write('''
| [{0}](https://{0}) | {1} | {2:%Y-%m-%d} | {3:%Y-%m-%d} |
        '''.rstrip().format(*row))
    f.write('''\n
Para reordenar la tabla puede usar las extensiones
[`Tampermonkey`](https://chrome.google.com/webstore/detail/tampermonkey/dhdgffkkebhmkfjojejmpbldmpobfkfo?hl=es)
o [`Greasemonkey`](https://addons.mozilla.org/es/firefox/addon/greasemonkey/)
con [`Github Sort Content`](https://greasyfork.org/en/scripts/21373-github-sort-content)
    '''.rstrip())
db.close()
