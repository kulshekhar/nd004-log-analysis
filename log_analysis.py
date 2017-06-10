#!/usr/bin/python2.7

import psycopg2

DBNAME = "news"

POP_ARTICLE_QUERY = '''
SELECT
    a.title, count(a.title) AS views
  FROM articles AS a
  LEFT JOIN
    log AS l
    ON
      CONCAT('/article/', a.slug) = l.path
  GROUP BY a.title
  ORDER BY views DESC
  LIMIT 3;
'''

POP_AUTHOR_QUERY = '''
SELECT
    au.name AS name, sum(au.id) AS total_views
  FROM authors AS au
  LEFT JOIN
    articles AS ar
    ON
      au.id=ar.author
  LEFT JOIN
    log AS l
    ON CONCAT('/article/', ar.slug) = l.path
  GROUP BY au.id
  ORDER BY total_views DESC;
'''

ERROR_QUERY = '''
SELECT
    t1.d, total_requests, error_requests
  FROM
    (SELECT DATE(time) AS d, COUNT(*) AS total_requests
       FROM log GROUP BY d) t1,
    (SELECT DATE(time) AS d, COUNT(*) AS error_requests
       FROM log WHERE status <> '200 OK' GROUP BY d) t2
  WHERE
    t1.d=t2.d AND error_requests > total_requests/100;
'''

with psycopg2.connect(database=DBNAME) as db:
    with db.cursor() as c:
        c.execute(POP_ARTICLE_QUERY)
        popular_articles = c.fetchall()

        c.execute(POP_AUTHOR_QUERY)
        popular_authors = c.fetchall()

        c.execute(ERROR_QUERY)
        days_with_high_error = c.fetchall()

print('The 3 most popular articles are:')
for article in popular_articles:
    print('  "{:s}" - {:d} views'.format(article[0], article[1]))

print('\nThe most popular authors are:')
for author in popular_authors:
    print('  {:25s} - {:d} views'.format(author[0], author[1]))

print('\nDays with high errors (>1%):')
for d in days_with_high_error:
    print('  {:s} - {:.2f}% errors'.format
          (d[0].strftime('%b %d, %Y'), d[2] * 100. / d[1]))
