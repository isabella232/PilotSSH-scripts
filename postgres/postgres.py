#!/usr/bin/env python

import json

try:
	import psycopg2
except ImportError:
	print json.dumps({
		'version': 1,
		'title': 'Postgres',
		'type': 'status',
		'status': 'error',
		'message': 'Please install the psycopg2 Python module to use this script'
	})
	exit()

from config import DSN

connection = psycopg2.connect(dsn=DSN)

def pg_connections():
	"""Postgres active connections"""

	c = connection.cursor()

	c.execute("SHOW max_connections")
	row = c.fetchone()

	return row[0]

def pg_locks():
	"""Show Postgres locks."""

	c = connection.cursor()

	c.execute("SELECT mode, COUNT(mode) FROM pg_locks GROUP BY mode ORDER BY mode")

	locks = 0
	exlocks = 0

	for row in c.fetchall():
		if 'exclusive' in row[0].lower():
			exlocks += row[1]
		locks += row[1]

	return dict(
		locks = locks,
		exlocks = exlocks
	)

def pg_databases_size():
	"""Postgres database sizes"""

	query = """SELECT d.datname AS Name,  pg_catalog.pg_get_userbyid(d.datdba) AS Owner,
			CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
				THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname))
				ELSE 'No Access'
			END AS Size
		FROM pg_catalog.pg_database d
			ORDER BY
			CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
				THEN pg_catalog.pg_database_size(d.datname)
				ELSE NULL
			END DESC -- nulls first
			LIMIT 20"""

	c = connection.cursor()

	c.execute(query)

	databases = {}

	for row in c.fetchall():
		databases[row[0]] = row[2]

	return databases

if __name__ == "__main__":
	data = {
		"version": 1,
		"title": "PostgresSQL",
		"type": "commands",
		"values": [
			{
				"name": pg_connections.__doc__,
				"value": pg_connections(),
				"command": ""
			},
			{
				"name": pg_locks.__doc__,
				"values": pg_locks(),
				"command": ""
			},
			{
				"name": pg_databases_size.__doc__,
				"values": pg_databases_size(),
				"command": ""
			}
		]
	}

	print json.dumps(data)