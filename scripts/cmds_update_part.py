#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Script to handle adding a part.
"""

from cmds import cm, cm_utils, cm_table_util


def query_args(args):
    """
    Gets information from user
    """
    if args.pn is None:
        args.pn = input("System part number:  ")
    if args.ptype is None:
        args.ptype = input("System part type:  ")
    if args.mfg is None:
        args.mfg = input("Manufacturers number for part:  ")
    args.date = cm_utils.query_default("date", args)
    return args


if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument("-p", "--pn", help="part number", default=None)
    parser.add_argument("-t", "--ptype", help="part type", default=None)
    parser.add_argument(
        "-m", "--mfg", help="Manufacturers number for part", default=None
    )
    parser.add_argument(
        "--disallow_restart",
        dest="allow_restart",
        help="Flag to disallow restarting an " "existing and stopped part",
        action="store_false",
    )
    cm_utils.add_date_time_args(parser)
    cm_utils.add_verbosity_args(parser)
    args = parser.parse_args()

    if args.pn is None or args.ptype is None or args.mfg is None:
        args = query_args(args)

    # Pre-process some args
    at_date = cm_utils.get_astropytime(args.date, args.time, args.format)
    args.verbosity = cm_utils.parse_verbosity(args.verbosity)

    db = cm.connect_to_cm_db(args)
    session = db.sessionmaker()

    if args.verbosity > 1:
        print("Trying to add new part {}".format(args.pn))
    new_part = [[args.pn, args.ptype, args.mfg]]
    cm_table_util.add_new_parts(
        session, parts=new_part, start_dates=[at_date], allow_restart=args.allow_restart
    )
