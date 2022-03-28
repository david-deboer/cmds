#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Checks the database for:
    associativity of connections to active parts
    duplicated comments
    concurrent apriori states
"""
from hera_cm import cm_checks
import argparse

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-l', '--info_log', help="Check latest info log",
                    action='store_true')
    ap.add_argument('--look-back', dest='look_back', help="Days back for log.",
                    type=float, default=7.0)
    ap.add_argument('-m', '--comments', help="Check for duplicate comments",
                    action='store_true')
    ap.add_argument('-n', '--connections', help="Check connections for ...",
                    action='store_true')
    ap.add_argument('-a', '--apriori', help="Check for overlapping apriori states XXX",
                    action='store_true')
    ap.add_argument('-e', '--ethers', help="Check the hosts/ethers hera_mc vs redis",
                    action='store_true')
    ap.add_argument('-d', '--daemons', help="Check running daemons",
                    action='store_true')
    ap.add_argument('-c', '--crontabs', help="Check crontable.",
                    action='store_true')
    ap.add_argument('--output-format', dest='output_format', default='orgtbl',
                    help='Format of output table - uses cm_utils.general_table_handler')
    ap.add_argument('--use-case', dest='ignore_case', help="For serial and mac use case",
                    action='store_false')
    ap.add_argument('--ignore-no-data-level', dest='ignore_no_data', default=1,
                    help="Level at which to ignore no serial/hosts/ethers data", type=int,
                    choices=[0, 1, 2])
    args = ap.parse_args()

    check = cm_checks.Checks()

    if args.info_log:
        check.info_log(look_back=args.look_back)
    if args.comments:
        check.duplicate_comments()
    if args.connections:
        check.part_conn_assoc()
    if args.apriori:
        check.apriori()
    if args.ethers:
        check.hosts_ethers(table_fmt=args.output_format)
        check.for_same(ignore_case=args.ignore_case, ignore_no_data=args.ignore_no_data)
    if args.crontabs:
        check.crontab()
    if args.daemons:
        check.daemon()
