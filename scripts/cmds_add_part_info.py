#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Script to handle adding a comment to the part_info table.
"""

from cmds import mc, cm_utils, cm_partconnect


def query_args(args):
    """
    Gets information from user
    """
    if args.pn is None:
        args.pn = input("System part number:  ")
    if args.comment is None:
        args.comment = input("Comment:  ")
    if args.reference is None:
        args.reference = input("reference:  ")
    args.date = cm_utils.query_default("date", args)
    return args


if __name__ == "__main__":
    parser = mc.get_mc_argument_parser()
    parser.add_argument("-p", "--pn", help="System part number", default=None)
    parser.add_argument("-c", "--comment", help="Comment on part", default=None)
    parser.add_argument("-l", "--reference", help="Library filename", default=None)
    parser.add_argument(
        "-q", "--query", help="Set flag if wished to be queried", action="store_true"
    )
    parser.add_argument("--verbose", help="Turn verbose mode on.", action="store_true")
    cm_utils.add_date_time_args(parser)
    args = parser.parse_args()

    if args.query:
        args = query_args(args)

    # Pre-process some args
    at_date = cm_utils.get_astropytime(args.date, args.time, args.format)
    if type(args.reference) == str and args.reference.lower() == "none":
        args.reference = None

    db = mc.connect_to_mc_db(args)
    session = db.sessionmaker()

    # Check for part
    if args.verbose:
        print("Adding info for part {}".format(args.pn))
    cm_partconnect.add_part_info(
        session,
        args.pn,
        args.comment,
        at_date=at_date,
        reference=args.reference,
    )
