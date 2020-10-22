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
    slp = url.split("://", 1)
    if len(slp)==2 and slp[0].lower() in ("http", "https"):
        url = slp[1]
    url = url.rstrip("/")
    return url

def sort_dom(r):
    prs = urlparse("https://"+r["site"])
    dom = prs.netloc
    path = prs.path
    dom = dom.split(".")
    dom.reverse()
    return tuple(dom + [path] + list(k for k, v in r.items() if k!="site"))

activity=[]
comments={}
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
            max(t1.post_date) fin,
            min(t1.post_date) ini
		from
            {2}posts t1
            left join {2}posts t3 on t1.post_parent = t3.ID
		where
    		(
                t1.post_status = 'publish' or
                (t1.post_status='inherit' and t3.post_status = 'publish')
            ) and
    		t1.post_type in ('post', 'page')
	''', debug="activity")
    activity.extend(results)

    for r in db.multi_execute(sites, '''
        select
            '{0}' site,
            count(*) comentarios,
            max(comment_date) ult
        from
            {2}comments
        where
            comment_type!='pingback' and
            comment_approved=1
    ''', debug="comments"):
        comments[r["site"]]=r
    db.close()

activity = sorted(activity, key=sort_dom)

with open("README.md", "w") as f:
    f.write('''
| BLOG | post/page | Comentarios | Último uso | 1º uso | Último comentario |
|:-----|----------:|------------:|-----------:|-------:|------------------:|
    '''.strip())
    for row in activity:
        row["admin"] = "https://{}/wp-admin/".format(row["site"])
        cmt = comments.get(row["site"], {})
        row["comentarios"] = cmt.get("comentarios", 0)
        row["ult_comentario"] = cmt.get("ult")
        if row["ult_comentario"]:
            row["ult_comentario"] = row["ult_comentario"].strftime("%Y-%m-%d")
        else:
            row["ult_comentario"] = ""
        f.write('''
| [{site}](https://{site}) | [{num}]({admin}edit.php?orderby=date&order=asc) | [{comentarios}]({admin}edit-comments.php?comment_type=comment&orderby=comment_date&order=desc) | {ini:%Y-%m-%d} | {fin:%Y-%m-%d} | {ult_comentario} |
        '''.rstrip().format(**row))
    f.write('''\n
Para reordenar la tabla puede usar las extensiones
[`Tampermonkey`](https://chrome.google.com/webstore/detail/tampermonkey/dhdgffkkebhmkfjojejmpbldmpobfkfo?hl=es)
o [`Greasemonkey`](https://addons.mozilla.org/es/firefox/addon/greasemonkey/)
con [`Github Sort Content`](https://greasyfork.org/en/scripts/21373-github-sort-content)
    '''.rstrip())
