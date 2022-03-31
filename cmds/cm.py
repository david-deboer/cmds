# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Setup and manage the CM database.

See INSTALL.md in the Git repository for instructions on how to initialize
your database and configure CM to find it.

"""

import os.path as op
from abc import ABCMeta
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

from . import MCDeclarativeBase
from .cm_session import MCSession
from .data import DATA_PATH

default_config_file = op.join(DATA_PATH, "db_config.json")
cm_log_file = op.expanduser("~.cm_log.txt")


class DB(object, metaclass=ABCMeta):
    """
    Abstract base class for CM database object.

    This ABC is only instantiated through the AutomappedDB or DeclarativeDB
    subclasses.

    """

    engine = None
    sessionmaker = sessionmaker(class_=MCSession)
    sqlalchemy_base = None

    def __init__(self, sqlalchemy_base, db_url):  # noqa
        self.sqlalchemy_base = MCDeclarativeBase
        self.engine = create_engine(db_url)
        self.sessionmaker.configure(bind=self.engine)


class DeclarativeDB(DB):
    """
    Declarative CM database object -- to create CM database tables.

    Parameters
    ----------
    db_url : str
        Database location.

    """

    def __init__(self, db_url):
        super(DeclarativeDB, self).__init__(MCDeclarativeBase, db_url)

    def create_tables(self):
        """Create all CM tables."""
        self.sqlalchemy_base.metadata.create_all(self.engine)

    def drop_tables(self):
        """Drop all CM tables."""
        self.sqlalchemy_base.metadata.bind = self.engine
        self.sqlalchemy_base.metadata.drop_all(self.engine)


class AutomappedDB(DB):
    """Automapped CM database object -- attaches to an existing CM database.

    This is intended for use with the production CM database. __init__()
    raises an exception if the existing database does not match the schema
    defined in the SQLAlchemy initialization magic.

    Parameters
    ----------
    db_url : str
        Database location.

    """

    def __init__(self, db_url):
        super(AutomappedDB, self).__init__(automap_base(), db_url)

        from .db_check import is_valid_database

        with self.sessionmaker() as session:
            if not is_valid_database(MCDeclarativeBase, session):
                raise RuntimeError(
                    "database {0} does not match expected schema".format(db_url)
                )


def get_cm_argument_parser():
    """
    Get an CM specific `argparse.ArgumentParser` object.

    Includes some predefined arguments global to all scripts that interact with
    the CM system. Currently, these are the path to the CM config file, and
    the name of the CM database connection to use.

    Once you have parsed arguments, you can pass the resulting object to a
    function like `connect_to_cm_db()` to automatically use the settings it
    encodes.

    """
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument(
        "--config",
        dest="cm_config_path",
        type=str,
        default=default_config_file,
        help="Path to the cm_config.json configuration file.",
    )
    p.add_argument(
        "--db",
        dest="cm_db_name",
        type=str,
        help="Name of the database to connect to. The default is "
        "used if unspecified.",
    )
    return p


def connect_to_cm_db(args, forced_db_name=None, check_connect=True):
    """
    Get a DB object that is connected to the CM database.

    Parameters
    ----------
    args : arguments
        The result of calling `parse_args` on an `argparse.ArgumentParser`
        instance created by calling `get_cm_argument_parser()`. Alternatively,
        it can be None to use the full defaults.
    forced_db_name : str, optional
        Database name to use (forced). If not set, uses the default one from
        args.
    check_connect : bool
        Option to test the database connection.

    Returns
    -------
    DB object
        An instance of the `DB` class providing access to the CM database.

    """
    if args is None:
        config_path = default_config_file
        db_name = None
    else:
        config_path = args.cm_config_path
        db_name = args.cm_db_name

    if forced_db_name is not None:
        db_name = forced_db_name

    import json

    with open(config_path) as f:
        config_data = json.load(f)

    if db_name is None:
        db_name = config_data.get("default_db_name")
        if db_name is None:
            raise RuntimeError(
                "cannot connect to CM database: no DB name "
                "provided, and no default listed in {0!r}".format(config_path)
            )

    db_data = config_data.get("databases")
    if db_data is None:
        raise RuntimeError(
            'cannot connect to CM database: no "databases" '
            "section in {0!r}".format(config_path)
        )

    db_data = db_data.get(db_name)
    if db_data is None:
        raise RuntimeError(
            "cannot connect to CM database: no DB named {0!r} "
            'in the "databases" section of {1!r}'.format(db_name, config_path)
        )

    db_url = db_data.get("url")
    if db_url is None:
        raise RuntimeError(
            'cannot connect to CM database: no "url" item for '
            "the DB named {0!r} in {1!r}".format(db_name, config_path)
        )

    db_mode = db_data.get("mode")
    if db_mode is None:
        raise RuntimeError(
            'cannot connect to CM database: no "mode" item for '
            "the DB named {0!r} in {1!r}".format(db_name, config_path)
        )

    if db_mode == "testing":
        db = DeclarativeDB(db_url)
    elif db_mode == "production":
        db = AutomappedDB(db_url)
    else:
        raise RuntimeError(
            "cannot connect to CM database: unrecognized mode "
            "{0!r} for the DB named {1!r} in {2!r}".format(
                db_mode, db_name, config_path
            )
        )

    if check_connect:
        # Test database connection
        with db.sessionmaker() as session:
            from . import db_check

            if not db_check.check_connection(session):
                raise RuntimeError(
                    "Could not establish valid connection to " "database."
                )

    return db


def connect_to_cm_testing_db(forced_db_name="testing"):
    """
    Get a DB object that is connected to the testing CM database.

    Parameters
    ----------
    forced_db_name : str
        Database name to use.

    Returns
    -------
    DB object
        An instance of the `DB` class providing access to the testing CM
        database.

    """
    return connect_to_cm_db(None, forced_db_name=forced_db_name)
