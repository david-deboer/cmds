#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""
Script to add a general connection to the database.
"""

from cmds import cm, cm_utils, cm_tables, cm_table_util


def query_args(args):
    """
    Gets information from user
    """
    if args.uppart is None:
        args.uppart = input("Upstream part number:  ")
    if args.upport is None:
        args.upport = input("Upstream output port:  ")
    if args.dnpart is None:
        args.dnpart = input("Downstream part number:  ")
    if args.dnport is None:
        args.dnport = input("Downstream input port:  ")
    if args.date == "now":  # note that 'now' is the current default.
        args.date = cm_utils.query_default("date", args)
    return args


if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument("-u", "--uppart", help="Upstream part number", default=None)
    parser.add_argument("--upport", help="Upstream output port", default=None)
    parser.add_argument("-d", "--dnpart", help="Downstream part number", default=None)
    parser.add_argument("--dnport", help="Downstream input port", default=None)
    cm_utils.add_date_time_args(parser)
    cm_utils.add_verbosity_args(parser)
    args = parser.parse_args()
    args.verbosity = cm_utils.parse_verbosity(args.verbosity)
    args = query_args(args)

    # Pre-process some args
    at_date = cm_utils.get_astropytime(args.date, args.time, args.format)

    db = cm.connect_to_cm_db(args)
    session = db.sessionmaker()
    connect = cm_tables.Connections()

    print("Adding connection {}:{} <-> {}:{}"
          .format(args.uppart, args.upport, args.dnpart, args.dnport))
    # Connect parts
    npc = [[args.uppart, args.upport, args.dnpart, args.dnport]]
    cm_table_util.add_new_connections(session, npc, [at_date])
