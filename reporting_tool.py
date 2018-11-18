#!/usr/bin/env python3

import psycopg2


conn = psycopg2.connect("dbname=news")
cursor = conn.cursor()


def reporting_tool(cur, query, text, format):
    cur.execute(query)
    rows = cursor.fetchall()
    print(text)

    for row in rows:
        print(format % (row[0], row[1]))


reporting_tool(
    cur=cursor,
    query="""
  SELECT articles.title ,COUNT(log.ip) as hits
  FROM articles, log
  WHERE log.path = CONCAT('/article/', articles.slug )
  GROUP BY articles.title
  ORDER BY hits DESC
  LIMIT 3
  """,
    text='1. What are the most popular three articles of all time?',
    format=" '%s' - %s views"
)

# https://stackoverflow.com/a/15754763

reporting_tool(
    cur=cursor,
    query="""
    SELECT authors.name ,COUNT(log.ip) as hits
    FROM articles, log, authors
    WHERE authors.id = articles.author \
    and log.path = CONCAT('/article/', articles.slug )
    GROUP BY authors.name
    ORDER BY hits DESC
    LIMIT 4
    """,
    text="2. Who are the most popular article authors of all time?",
    format=" %s - %s views"
)

cursor.execute("""
        SELECT DATE(time) as thedate ,CAST(count(status) AS INT) as \
        errors,CAST(count(ip) AS INT) as hits
        FROM log
        WHERE  status =  '404 NOT FOUND'
        GROUP BY thedate
        HAVING \
        ROUND(CAST(count(status) AS INT) / CAST(count(ip) AS INT)  * 100) > 1.0
        ORDER BY errors DESC
        LIMIT 1
  """)
rows = cursor.fetchall()
print ('3. On which days did more than 1% of requests lead to errors?')
for row in rows:
    print (row[0].strftime('%B %d, %Y') + " — 1% errors")


conn.close()
