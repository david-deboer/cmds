#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Definitions to generate table initialization files.

Used in scripts cmds_init.py, cmds_pack.py
"""

import csv
import os.path
import subprocess
from math import floor

#-to cut?from astropy.time import Time
#-to cut?from sqlalchemy import BigInteger, Column, String

from . import cm_tables, cm_utils, cm

data_prefix = "initialization_data_"


def package_db_to_csv(session=None, tables="all"):
    """
    Get the configuration management tables and package them to csv files.

    The csv files are read by initialize_db_from_csv.

    Parameters
    ----------
    session : object or None
        session on current database. If session is None, a new session
        on the default database is created and used.
    tables: string
        comma-separated list of names of tables to initialize or 'all'.

    Returns
    -------
    list
        list of filenames written

    """
    import pandas

    tables_to_write = cm_tables.order_the_tables(tables)

    print("Writing packaged files to current directory.")
    print(
        "--> If packing from qmaster, be sure to use 'cm_pack.py --go' to "
        "copy, commit and log the change."
    )
    with cm.MCSessionWrapper(session=session) as session:
        files_written = []
        for table in tables_to_write:
            data_filename = data_prefix + table + ".csv"
            with session.get_bind().connect() as conn:
                table_data = pandas.read_sql_table(table, conn)
            print("\tPackaging:  " + data_filename)
            table_data.to_csv(data_filename, index=False)
            files_written.append(data_filename)

    return files_written


def pack_n_go(cm_csv_path):
    """
    Move the csv files to the distribution directory and commit.

    Parameters
    ----------
    cm_csv_path : str
        Path to csv distribution directory

    """
    # move files over to dist dir
    cmd = "mv -f *.csv {}".format(cm_csv_path)
    subprocess.call(cmd, shell=True)

    # commit new csv files
    cmd = "git -C {} commit -am 'updating csv to repo.'".format(cm_csv_path)
    subprocess.call(cmd, shell=True)


def initialize_db_from_csv(session=None, tables="all", maindb=False, cm_csv_path=None):
    """
    Read the csv files and repopulate the configuration management database.

    This entry module provides a double-check entry point to read the csv files and
    repopulate the configuration management database.  It destroys all current entries,
    hence the double-check

    Parameters
    ----------
    session: Session object
        session on current database. If session is None, a new session
        on the default database is created and used.
    tables: str
        comma-separated list of names of tables to initialize or 'all'.
    maindb: bool or str
        Either False or the password to change from main db.
    cm_csv_path : str or None
        Path where the csv files reside.  If None uses default.

    Returns
    -------
    bool
        Success, True or False

    """
    print("This will erase and rewrite the configuration management tables.")
    you_are_sure = input("Are you sure you want to do this (y/n)? ")
    if you_are_sure == "y":
        success = _initialization(
            session=session,
            cm_csv_path=cm_csv_path,
            tables=tables,
            maindb=maindb,
        )
    else:
        print("Exit with no rewrite.")
        success = False
    return success


def check_if_main(session, expected_main_hostname="obs-node1"):
    """
    Determine if the code is running on the site main computer or not.

    Parameters
    ----------
    session : object
        Session object required
    config_path : str or None
        Full path to location of config file.  Default is None, which goes to default path.
    expected_hostname : str
        Name of the expected main host.

    Returns
    -------
    bool
        True if main host, False if not.

    """
    import socket

    hostname = socket.gethostname()
    is_main_host = hostname == expected_main_hostname

    session_db_url = session.bind.engine.url.render_as_string(hide_password=False)

    if is_main_host:  # pragma: no cover
        print(f"Found main db at hostname {hostname} and DB url {session_db_url}")
    return is_main_host


def db_validation(maindb_pw, session):
    """
    Check if you are working on the main db and if so if you have the right password
    which is just hardcoded below.

    Parameters
    ----------
    maindb_pw : str
        password to allow access to main
    session : object
        Session object

    Returns
    -------
    bool
        True means you are allowed to modify main database.  False not.

    """
    hardcode_maindb_pw = "pw4maindb"
    is_maindb = check_if_main(session)

    if not is_maindb:
        return True

    if maindb_pw is False:
        raise ValueError("Error:  attempting access to main db without a password")
    if maindb_pw != hardcode_maindb_pw:
        raise ValueError("Error:  incorrect password for main db")

    return True


def _initialization(session=None, cm_csv_path=None, tables="all", maindb=False):
    """
    Initialize the database.

    This is an internal initialization method, it should be called via initialize_db_from_csv.

    Parameters
    ----------
    session : Session object
        session on current database. If session is None, a new session
             on the default database is created and used.
    tables : str
        comma-separated list of names of tables to initialize or 'all'.
    maindb : bool or str
        Either False or password to change from main db.
    testing : bool
        Flag to allow for testing.

    Returns
    -------
    bool
        Success, True or False

    """
    wrapper = cm.MCSessionWrapper(session=session)
    if cm_csv_path is None:
        cm_csv_path = cm.get_cm_csv_path(mc_config_file=None)

    if not db_validation(maindb, wrapper.session):
        print("cm_init not allowed.")
        return False

    if tables != "all":  # pragma: no cover
        print("You may encounter foreign_key issues by not using 'all' tables.")
        print("If it doesn't complain though you should be ok.")

    # Get tables to deal with in proper order
    tables_to_read = cm_tables.order_the_tables(tables)

    use_table = []
    for table in tables_to_read:
        csv_table_name = data_prefix + table + ".csv"
        use_table.append([table, os.path.join(cm_csv_path, csv_table_name)])


    # Delete tables in this order
    for table, data_filename in use_table:
        num_rows_deleted = wrapper.session.query(cm_tables[table][0]).delete()
        print("%d rows deleted in %s" % (num_rows_deleted, table))

    # Initialize tables in reversed order
    for table, data_filename in reversed(use_table):
        cm_utils.log("cm_initialization: " + data_filename)
        field_row = True  # This is the first row
        with open(data_filename, "rt") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                table_inst = cm_tables[table][0]()
                if field_row:
                    field_name = row
                    field_row = False
                else:
                    for i, r in enumerate(row):
                        if r == "":
                            r = None
                        elif "gpstime" in field_name[i]:
                            # Needed since pandas does not have an integer representation
                            #  of NaN, so it outputs a float, which the database won't allow
                            r = int(float(r))
                        setattr(table_inst, field_name[i], r)
                    wrapper.session.add(table_inst)
                    wrapper.session.commit()
    wrapper.wrapup(
        updated=False
    )  # Since we commited along the way to handle ForeignKey
