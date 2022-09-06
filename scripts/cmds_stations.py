#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""This is meant to hold utility scripts for geo_location (via cm_stations)

"""

from cmds import cm, cm_stations, cm_utils
import matplotlib.pyplot as plt
fg_groups = ['active', 'all', 'cofa', 'none']


if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument(
        "foreground",
        nargs="?",
        default="all",
        help="Actions for foreground listing:  active, all, <csv-list>, cofa, none",
    )
    parser.add_argument(
        "-b", "--background",
        help="Plot background of all stations.",
        action='store_true',
    )
    cm_utils.add_date_time_args(parser)
    parser.add_argument(
        "-x", "--xgraph",
        help="X-axis of graph. [easting]",
        choices=["easting", "northing", "elevation", "lat", "lon", "x", "y", "z"],
        default="easting",
    )
    parser.add_argument(
        "-y", "--ygraph",
        help="Y-axis of graph. [northing]",
        choices=["easting", "northing", "elevation", "lat", "lon", "x", "y", "z"],
        default="northing",
    )
    parser.add_argument(
        "-t", "--station-types",
        help="Station types searched (csv_list or 'all') Can use types or prefixes - see sysdef.json [all]",
        dest="station_types",
        default="all",
    )
    parser.add_argument(
        "--label",
        choices=["name", "conn", "id", "none"],
        default="name",
        help="Label by station_name (name), connected (conn) mfg id of connected (id) or none (none) (num)",
    )
    parser.add_argument(
        "--hookup-type",
        dest="hookup_type",
        help="Hookup type to use for active antennas.",
        default=None,
    )
    args = parser.parse_args()
    at_date = cm_utils.get_astropytime(args.date, args.time, args.format)
    if args.foreground not in fg_groups:
        args.foreground = cm_utils.listify(args.foreground)
    if args.station_types not in ["all"]:
        args.station_types = cm_utils.listify(args.station_types)
    if args.label == "false" or args.label == "none":
        args.label = False

    # start session and instances
    with cm.CMSessionWrapper() as session:
        S = cm_stations.Stations(session=session, at_date=at_date)

        # Apply background
        if args.background:
            S.plot_stations(list(S.active.stations.keys()),
                            xgraph=args.xgraph,
                            ygraph=args.ygraph,
                            label=False,
                            color="0.7",
                            shape="s",
                            size=4
                            )

        # Process foreground action.
        fg_markersize = 5
        if isinstance(args.foreground, list):
            S.plot_stations(args.foreground,
                            xgraph=args.xgraph,
                            ygraph=args.ygraph,
                            label=args.label,
                            color="k",
                            shape="o",
                            size=fg_markersize,
                            )
        elif args.foreground in ['all', 'active']:
            S.get_stations(args.foreground, args.station_types)
            S.plot_stations(None,
                            xgraph=args.xgraph,
                            ygraph=args.ygraph,
                            label=args.label,
                            color="k",
                            shape="o",
                            size=fg_markersize,
                            )
        elif args.foreground == 'cofa':
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

    plt.show()
