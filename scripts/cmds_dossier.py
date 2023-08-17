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

all_views = {
    "p": "parts",
    "c": "connections",
}

parser = cm.get_cm_argument_parser()
parser.add_argument(
    "view",
    nargs="?",
    help="Views are:  {}.  Need first letter only.\
                    ".format(
        ", ".join(all_views.values())
    ),
    default="parts",
)
# set values for 'action' to use
parser.add_argument(
    "-p",
    "--pn",
    help="Part number or portion thereof, csv list.",
    default=None,
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
parser.add_argument("--ports", help="Include only these ports, csv list", default=None)
cm_utils.add_verbosity_args(parser)
cm_utils.add_date_time_args(parser)

args = parser.parse_args()

args.verbosity = cm_utils.parse_verbosity(args.verbosity)
view = all_views[args.view[0].lower()]
date_query = cm_utils.get_astropytime(args.date, args.time, args.format)

# Start session
with cm.CMSessionWrapper() as session:
    if args.list_columns:
        blank = cm_dossier.PartEntry(None, None)
        print("\t{:30s}\t{}".format("Use", "For"))
        print("\t{:30s}\t{}".format("--------------", "----------"))
        for col in blank.col_hdr.keys():
            print("\t{:30s}\t{}".format(col, blank.col_hdr[col]))
    else:  # view == 'parts' or view == 'connections'
        args.pn = cm_utils.listify(args.pn)
        if view == "parts":
            if args.verbosity == 1:
                columns = ["pn", "ptype", "input_ports", "output_ports"]
            elif args.verbosity == 2:
                columns = [
                    "pn",
                    "ptype",
                    "manufacturer_id",
                    "start_gpstime",
                    "stop_gpstime",
                    "input_ports",
                    "output_ports",
                    "station",
                ]
            else:
                columns = [
                    "pn",
                    "ptype",
                    "manufacturer_id",
                    "start_gpstime",
                    "stop_gpstime",
                    "input_ports",
                    "output_ports",
                    "station",
                    "comment",
                ]
        elif view == "connections":
            if args.verbosity == 1:
                columns = [
                    "up.upstream_part",
                    "up.upstream_output_port",
                    "up.downstream_input_port",
                    "pn",
                    "down.upstream_output_port",
                    "down.downstream_input_port",
                    "down.downstream_part",
                ]
            elif args.verbosity == 2:
                columns = [
                    "up.upstream_part",
                    "up.upstream_output_port",
                    "up.downstream_input_port",
                    "pn",
                    "down.upstream_output_port",
                    "down.downstream_input_port",
                    "down.downstream_part",
                ]
            else:
                columns = [
                    "up.start_gpstime",
                    "up.stop_gpstime",
                    "up.upstream_part",
                    "up.upstream_output_port",
                    "up.downstream_input_port",
                    "pn",
                    "down.upstream_output_port",
                    "down.downstream_input_port",
                    "down.downstream_part",
                    "down.start_gpstime",
                    "down.stop_gpstime",
                ]

        if args.columns is not None:
            columns = cm_utils.listify(args.columns)
        if args.ports is not None:
            args.ports = cm_utils.listify(args.ports)  # specify port names as list.
        dossier = cm_dossier.Dossier(
            dtype = args.view,
            pn=args.pn,
            at_date=date_query,
            exact_match=args.exact_match,
            session=session,
        )
        print(dossier.show_dossier(columns))
    print()
