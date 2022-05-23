#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""This is meant to hold utility scripts for geo_location (via cm_stations)

"""

from cmds import cm, cm_stations, cm_utils

if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument(
        "fg_action",
        nargs="*",
        default=["none"],
        help="Actions for foreground listing:  "
        "a[ctive], i[nstalled], p[osition] <csv-list>, c[ofa], n[one] ()",
    )
    parser.add_argument(
        "-b", "--background",
        help="Plot background of all stations.",
        action='store_true',
    )
    cm_utils.add_date_time_args(parser)
    parser.add_argument(
        "-x", "--xgraph",
        help="X-axis of graph. [E]",
        choices=["easting", "northing", "elevation", "lat", "lon", "x", "y", "z"],
        default="easting",
    )
    parser.add_argument(
        "-y", "--ygraph",
        help="Y-axis of graph. [N]",
        choices=["easting", "northing", "elevation", "lat", "lon", "x", "y", "z"],
        default="northing",
    )
    parser.add_argument(
        "-t", "--station-types",
        help="Station types searched (csv_list or 'all') "
        "Can use types or prefixes. (default)",
        dest="station_types",
        default="default",
    )
    parser.add_argument(
        "--label",
        choices=["name", "conn", "id", "none"],
        default="name",
        help="Label by station_name (name), connected (conn) "
        "mfg id of connected (id) or none (none) (num)",
    )
    parser.add_argument(
        "--hookup-type",
        dest="hookup_type",
        help="Hookup type to use for active antennas.",
        default=None,
    )
    args = parser.parse_args()
    if len(args.fg_action) > 1:
        position = cm_utils.listify(args.fg_action[1])
    args.fg_action = args.fg_action[0].lower()
    args.station_types = args.station_types.lower()
    args.label = args.label.lower()
    at_date = cm_utils.get_astropytime(args.date, args.time, args.format)
    if args.station_types not in ["default", "all"]:
        args.station_types = cm_utils.listify(args.station_types)
    if args.label == "false" or args.label == "none":
        args.label = False

    # start session and instances
    db = cm.connect_to_cm_db(args)
    with db.sessionmaker() as session:
        S = cm_stations.Stations(session=session, at_date=at_date)

        # Apply background
        if args.background:
            S.load_stations()
            S.plot_stations()

        # Process foreground action.
        fg_markersize = 10

        if args.fg_action.startswith("a"):
            located = S.get_active_stations(
                station_types_to_use=args.station_types,
                query_date=at_date,
                hookup_type=args.hookup_type,
            )
            S.plot_stations(
                located,
                xgraph=args.xgraph,
                ygraph=args.ygraph,
                label=args.label,
                marker_color="k",
                marker_shape="*",
                marker_size=fg_markersize,
            )
        elif args.fg_action.startswith("i"):
            S.plot_station_types(
                station_types_to_use=args.station_types,
                query_date=at_date,
                xgraph=args.xgraph,
                ygraph=args.ygraph,
                label=args.label,
            )
        elif args.fg_action.startswith("p"):
            located = S.get_location(position, at_date)
            S.print_loc_info(located)
            S.plot_stations(
                located,
                xgraph=args.xgraph,
                ygraph=args.ygraph,
                label=args.label,
                marker_color="k",
                marker_shape="*",
                marker_size=fg_markersize,
            )
        elif args.fg_action.startswith("c"):
            cofa = S.cofa()
            S.print_loc_info(cofa)
            S.plot_stations(
                cofa,
                xgraph=args.xgraph,
                ygraph=args.ygraph,
                label="name",
                marker_color="k",
                marker_shape="*",
                marker_size=fg_markersize,
            )
