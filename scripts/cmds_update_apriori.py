#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""
Script to handle adding or stopping apriori status.
"""

from cmds import cm, cm_utils, cm_tables
stat_choices = cm_tables.get_allowed_apriori_statuses() + [None]


if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument("action", help="add or stop part.", choices=['add', 'stop'])
    parser.add_argument("pn", help="part number")
    parser.add_argument("-s", "--status", help="apriori status", choices=stat_choices, default=None)
    parser.add_argument("-c", "--comment", help="Comment", default=None)
    cm_utils.add_date_time_args(parser)
    args = parser.parse_args()

    # Pre-process some args
    date = cm_utils.get_astropytime(args.date, args.time, args.format)
    update = {"action": args.action, "pn": args.pn.upper()}
    if args.action == 'add':
        if args.status is None:
            raise ValueError("status must have a value for add.")
        update['status'] = args.status
        update['comment'] = args.comment

    db = cm.connect_to_cm_db(args)
    with db.sessionmaker() as session:
        cm_tables.update_aprioris(parts=[update], dates=[date], session=session)
