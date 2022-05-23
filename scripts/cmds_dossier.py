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
    "i": "info",
    "n": "node",
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
    help="Part number or portion thereof, csv list. "
    "If view is 'node', this may be ints or a hyphen-range of ints (e.g. '0-3')"
    "or 'active'/'all'",
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
parser.add_argument(
    "--notes-start-date",
    dest="notes_start_date",
    help="<For notes> start_date for notes [<]",
    default="<",
)
parser.add_argument(
    "--notes-start-time",
    dest="notes_start_time",
    help="<For notes> start_time for notes",
    default=0.0,
)
parser.add_argument(
    "--notes-float-format",
    dest="notes_float_format",
    help="unix, gps, jd as appropriate for notes-start-date",
    default=None,
)
args = parser.parse_args()

args.verbosity = cm_utils.parse_verbosity(args.verbosity)
view = all_views[args.view[0].lower()]
date_query = cm_utils.get_astropytime(args.date, args.time, args.format)
notes_start_date = cm_utils.get_astropytime(
    args.notes_start_date, args.notes_start_time, args.notes_float_format
)

# Start session
db = cm.connect_to_cm_db(args)
with db.sessionmaker() as session:
    if args.list_columns:
        blank = cm_dossier.PartEntry(None, None)
        print("\t{:30s}\t{}".format("Use", "For"))
        print("\t{:30s}\t{}".format("--------------", "----------"))
        for col in blank.col_hdr.keys():
            print("\t{:30s}\t{}".format(col, blank.col_hdr[col]))
    else:  # view == 'parts' or view == 'connections' or view == 'info'
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
                    "stations",
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
                    "stations",
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
                    "up.up_part_rev",
                    "up.upstream_output_port",
                    "up.downstream_input_port",
                    "pn",
                    "down.upstream_output_port",
                    "down.downstream_input_port",
                    "down.downstream_part",
                    "down.down_part_rev",
                ]
            else:
                columns = [
                    "up.start_gpstime",
                    "up.stop_gpstime",
                    "up.upstream_part",
                    "up.up_part_rev",
                    "up.upstream_output_port",
                    "up.downstream_input_port",
                    "pn",
                    "down.upstream_output_port",
                    "down.downstream_input_port",
                    "down.downstream_part",
                    "down.down_part_rev",
                    "down.start_gpstime",
                    "down.stop_gpstime",
                ]
        elif view == "info":
            if args.verbosity == 1:
                columns = ["pn", "comment"]
            elif args.verbosity == 2:
                columns = ["pn", "posting_gpstime", "comment"]
            else:
                columns = ["pn", "posting_gpstime", "reference", "comment"]

        if args.columns is not None:
            columns = cm_utils.listify(args.columns)
        if args.ports is not None:
            args.ports = cm_utils.listify(args.ports)  # specify port names as list.
        dossier = cm_dossier.Dossier(
            pn=args.pn,
            at_date=date_query,
            active=None,
            notes_start_date=notes_start_date,
            exact_match=args.exact_match,
        )
        print(dossier.show_part_dossier(columns))
    print()
