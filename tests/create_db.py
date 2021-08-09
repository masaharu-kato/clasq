""" Test of database table creation """
import argparse
import os
import sys
import subprocess
from dbconfig import ROOT_DIR, DBCFG
from libsql.cursor import MySQLCommandCursor
from libsql.connector import SQLExecutor

_DB_USERS_ = ['admin', 'loader', 'viewer']

def create_sample_db(*, debug_dump:bool=False) -> None:
    """ Create (or recreate) database itself and users """

    with SQLExecutor(MySQLCommandCursor(['mysql'], debug_dump=(sys.stderr if debug_dump else None))) as exr:
        exr.recreate_database(DBCFG['server']['databasename'])
        exr.use_database(DBCFG['server']['databasename'])

        for user in _DB_USERS_:
            exr.recreate_user(
                DBCFG['users'][user]['username'],
                DBCFG['users'][user]['password'],
                ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'EXECUTE']
            )

        with open(os.path.join(ROOT_DIR, 'conf/tables.sql')) as f:
            exr.execute_from(f)


def main():
    argp = argparse.ArgumentParser()
    argp.add_argument('-dd', '--debug-dump', action='store_true', help='Dump sqls to stderr instead of running')
    args = argp.parse_args()
    create_sample_db(debug_dump=args.debug_dump)


if __name__ == "__main__":
    main()
