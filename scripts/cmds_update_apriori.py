#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""
Script to handle adding or stopping apriori status.
"""

from cmds import cm, cm_utils, cm_tables


if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument("pn", help="part number")
    parser.add_argument("status", help="apriori status")
    parser.add_argument("-c", "--comment", help="Optional (but encouraged) comment", default=None)
    cm_utils.add_date_time_args(parser)
    args = parser.parse_args()

    # Populate update dictionary
    update = [{"pn": args.pn.upper(),
               "status": args.status,
               "comment": args.comment,
               "date": cm_utils.get_astropytime(args.date, args.time, args.format)}]

    with cm.CMSessionWrapper() as session:
        cm_tables.update_aprioris(parts=update, session=session)
