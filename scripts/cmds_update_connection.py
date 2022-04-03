#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""
Script to add/update connection to/in the database.
"""

from cmds import cm, cm_utils, cm_tables

if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument("action", help="add or stop connection.", choices=['add', 'stop'])
    parser.add_argument("-u", "--uppart", help="Upstream part number")
    parser.add_argument("--upport", help="Upstream output port")
    parser.add_argument("-d", "--dnpart", help="Downstream part number")
    parser.add_argument("--dnport", help="Downstream input port")
    parser.add_argument("--same-conn-sec", dest='same_conn_sec',
                        help="Threshhold for start being same connection (sec)", default=100)
    cm_utils.add_date_time_args(parser)
    args = parser.parse_args()

    date = cm_utils.get_astropytime(args.date, args.time, args.format)
    update = {"action": args.action,
              "upstream_part": args.uppart.upper(), "upstream_output_port": args.upport.lower(),
              "downstream_part": args.dnpart.upper(), "downstream_input_port": args.dnport.lower()
              }

    db = cm.connect_to_cm_db(args)
    session = db.sessionmaker()

    cm_tables.update_connections([update], [date], same_conn_sec=args.same_conn_sec, session=session)
    session.close()
