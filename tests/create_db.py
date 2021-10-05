""" Test of database table creation """
import argparse
import os
import sys
from pathlib import Path
from libbc.config import LIB_ROOT_DIR, Configuration
from libsql.connector import MySQLConnection
from libsql.executor import QueryExecutor

_DB_USERS_ = ['admin', 'loader', 'viewer']

# def create_sample_db(db_config_path:Path, *, debug_dump:bool=False) -> None:
#     """ Create (or recreate) database itself and users """
#     # TODO: Fix

#     DBCFG = Configuration(db_config_path)
#     dbc = MySQLConnection()

#     with QueryExecutor(MySQLCommandCursor(['mysql'], debug_dump=(sys.stderr if debug_dump else None))) as exr:
#         exr.recreate_database(DBCFG['server']['databasename'])
#         exr.use_database(DBCFG['server']['databasename'])

#         for user in _DB_USERS_:
#             exr.recreate_user(
#                 DBCFG['users'][user]['username'],
#                 DBCFG['users'][user]['password'],
#                 ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'EXECUTE']
#             )

#         with open(os.path.join(LIB_ROOT_DIR, DBCFG['schema']['path'])) as f:
#             exr.execute_from(f)


def main():
    argp = argparse.ArgumentParser()
    argp.add_argument('-dd', '--debug-dump', action='store_true', help='Dump sqls to stderr instead of running')
    args = argp.parse_args()
    # create_sample_db(debug_dump=args.debug_dump)


if __name__ == "__main__":
    main()
