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

def clean_url(url):
    url = url.split("://", 1)[1]
    if url.endswith("/"):
        url = url[:-1]
    return url

def sort_dom(r):
    prs = urlparse("https://"+r[0])
    dom = prs.netloc
    path = prs.path
    dom = dom.split(".")
    dom.reverse()
    return dom + [path] + list(r[1:])

activity=[]
for db in DBs:
    print(title(db.host))
    db.connect()

    results = db.execute('search-wp.sql')

    prefixes = sorted([r for r in results if r[0]
                       not in db.db_ban and r[1] not in db.db_ban])

    print("%s wordpress encontrados" % len(prefixes))

    results = db.multi_execute(prefixes, '''
        select
            '{0}' prefix1,
            '{1}' prefix2,
            option_value siteurl
        from
            {1}options
        where
            option_name = 'siteurl'
	''', debug="sites", to_tuples=True)

    sites = {}
    for p1, p2, siteurl in results:
        site = clean_url(siteurl)
        if not db.isOkDom(siteurl) or not db.isOk(siteurl):
            print("%s (%s) sera descartado" % (p2, site))
            continue
        sites[site] = (p1, p2)


    results = db.multi_execute(sites, '''
        select
            '{0}' site,
            count(*) num,
            max(post_date) fin,
            min(post_date) ini
        from {2}posts
        where
        post_status = 'publish' and
        post_type in ('post', 'page')
	''', debug="activity", to_tuples=True)

    activity.extend(results)
    db.close()

activity = sorted(activity, key=sort_dom)

with open("README.md", "w") as f:
    f.write('''
| BLOG | POSTs | Último uso | 1º uso |
|:-----|------:|-----------:|-------:|
    '''.strip())
    for row in activity:
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
