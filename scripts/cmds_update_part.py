#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""
Script to handle adding or stopping a part.
"""

from cmds import cm, cm_utils, cm_table_util


if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument("action", help="add or stop part.", choices=['add', 'stop'])
    parser.add_argument("-p", "--pn", help="part number")
    parser.add_argument("-t", "--type", help="part type", default=None)
    parser.add_argument("-m", "--mfg", help="Manufacturers number for part", default=None)
    cm_utils.add_date_time_args(parser)
    args = parser.parse_args()

    # Pre-process some args
    date = cm_utils.get_astropytime(args.date, args.time, args.format)
    update = {"action": args.action, "pn": args.pn.upper()}
    if args.action == 'add':
        if args.type is None or args.mfg is None:
            raise ValueError("ptype and mfg must have values for add.")
        update['ptype'] = args.type
        update['manufacturer_id'] = args.mfg

    db = cm.connect_to_cm_db(args)
    session = db.sessionmaker()
    cm_table_util.update_parts(parts=[update], dates=[date], session=session)
    session.close()
