#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Script to handle adding a comment to the part_info table.
"""

from cmds import mc, cm_utils, cm_tables


if __name__ == "__main__":
    parser = mc.get_mc_argument_parser()
    parser.add_argument("-p", "--pn", help="Part number", default=None)
    parser.add_argument("-c", "--comment", help="Comment on part", default=None)
    parser.add_argument("-l", "--reference", help="Library filename", default=None)
    cm_utils.add_date_time_args(parser)
    args = parser.parse_args()

    # Pre-process some args
    date = cm_utils.get_astropytime(args.date, args.time, args.format)

    db = mc.connect_to_mc_db(args)
    session = db.sessionmaker()

    cm_tables.update_info(
        [{'pn': args.pn, 'comment': args.comment, 'reference': args.reference}],
        dates=[date],
        session=session
    )
