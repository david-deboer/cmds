#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Utility scripts to display dossier information.

Actions 'parts', 'connections', and 'notes' differ only by defining a different
set of columns, which may be overridden by instead using the args.columns parameter
(--list-all-columns)

"""
from cmds import cm, cm_dossier, cm_utils


parser = cm.get_cm_argument_parser()

# set values for 'action' to use
parser.add_argument(
    "-p",
    "--pn",
    help="Part number or portion thereof, csv list.",
    default='obs',
)
parser.add_argument(
    "-e",
    "--exact-match",
    help="Force exact matches on part numbers, not beginning " "N char. [False]",
    dest="exact_match",
    action="store_true",
)
parser.add_argument(
    "--columns",
    help="Custom columns as csv list. " "Use '--list-all-columns' for options.",
    default=None,
)
parser.add_argument(
    "--list-all-columns",
    dest="list_columns",
    help="Show a list of all available columns",
    action="store_true",
)
parser.add_argument(
    "--ports",
    help="Include only these ports, csv list",
    default=None
)
cm_utils.add_verbosity_args(parser)
cm_utils.add_date_time_args(parser, date_default='now')
parser.add_argument(
    "--window",
    help="If provided, it will return log between [this many days in the past (if number) or from get_astropytime str -> supplied date]",
    default=None
)
args = parser.parse_args()

try:
    args.window = float(args.window)
except (ValueError, TypeError):
    pass
args.verbosity = cm_utils.parse_verbosity(args.verbosity)
date_query = cm_utils.get_astropytime(args.date, args.time, args.format)

# Start session
with cm.CMSessionWrapper() as session:
    if args.list_columns:
        blank = cm_dossier.PartEntry(None, None)
        print("\t{:30s}\t{}".format("Use", "For"))
        print("\t{:30s}\t{}".format("--------------", "----------"))
        for col in blank.col_hdr.keys():
            print("\t{:30s}\t{}".format(col, blank.col_hdr[col]))
    else:
        args.pn = cm_utils.listify(args.pn)

        if args.verbosity == 1:
            columns = ["pn", "comment"]
        elif args.verbosity == 2:
            columns = ["pn", "posting_gpstime", "comment", "pol"]
        else:
            columns = ["pn", "posting_gpstime", "comment", "pol", "reference"]

        if args.columns is not None:
            columns = cm_utils.listify(args.columns)
        if args.ports is not None:
            args.ports = cm_utils.listify(args.ports)  # specify port names as list.
        dossier = cm_dossier.Dossier(
            pn=args.pn,
            exact_match=args.exact_match,
            active = [],
            at_date=date_query,
            session=session
        )
        dossier.load_dossier(window=args.window)
        print(dossier.show_dossier(columns))
    print()