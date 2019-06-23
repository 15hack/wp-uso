#!/usr/bin/python3
import getpass
import os
import sys
import re
from urllib.parse import urlparse
from connect import DBs

re_blog = re.compile(r".*_(\d+)_posts$")

def title(s, c='=', l=10):
    s = "{1} {0} {1}".format(s, c*(l-len(s)))
    if len(s) % 2 == 1:
        s = c + s
    return s


def get_site_url(db, scheme, wp_posts):
    pref=wp_posts[:-5]
    m = re_blog.match(wp_posts)
    if m:
        blog_id = int(m.group(1))
        sql = "select option_value URL from %s.%soptions where option_name='siteurl'" % (scheme, pref)
    else:
        sql = "select option_value URL from %s.%soptions where option_name='siteurl'" % (scheme, pref)
    r = db.execute(sql)
    siteurl = r[0][0]
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
    return dom + [path] + list(r[1:])

results=[]
for db in DBs:
    print(title(db.host))
    db.connect()

    wps = db.execute('search-wp.sql')

    sql = "select distinct * from ("

    for row in wps:
        siteurl = get_site_url(db,*row)
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
    results.extend(db.execute(sql))
    db.close()

results = sorted(results, key=sort_dom)

with open("README.md", "w") as f:
    f.write('''
| BLOG | POSTs | Último uso | 1º uso |
|:-----|------:|-----------:|-------:|
    '''.strip())
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
