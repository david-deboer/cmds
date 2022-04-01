#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Script to handle installing a new station into system.
"""

from cmds import cm, cm_table_util, cm_utils


def query_geo_information(args):
    """
    Gets geo_location information from user
    """
    if args.easting is None:
        args.easting = float(input("Easting:  "))
    if args.northing is None:
        args.northing = float(input("Northing:  "))
    if args.elevation is None:
        args.elevation = float(input("Elevation:  "))
    args.datum = cm_utils.query_default("datum", args)
    args.tile = cm_utils.query_default("tile", args)
    if args.station_type is None:
        args.station_type = input("Station type: ")
    args.date = cm_utils.query_default("date", args)
    return args


def add_entry_to_stations(session, args):
    # NotNull
    dt = cm_utils.get_astropytime(args.date, args.time, args.format)
    data = {"station_name", args.station_name.upper(),
            "station_type", args.station_type,
            "created_gpstime", dt.gps}
    # Other
    if args.datum:
        data["datum"] = args.datum
    if args.tile:
        data["tile"] = args.tile
    if args.northing:
        data["northing"] = args.northing
    if args.easting:
        data["easting"] = args.easting
    if args.elevation:
        data["elevation"] = args.elevation
    cm_table_util.update_station(session, [data], True)


def add_entry_to_parts(session, args):
    dt = cm_utils.get_astropytime(args.date, args.time, args.format)
    data = {"pn": args.station_name.upper(),
            "hptype": "station",
            "manufacturer_number": "{}:{}".format(int(args.northing), int(args.easting),
            "start_gpstime": dt.gps}
    cm_table_util.update_part(session, [data])


if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument(
        "station_name", help="Name of station (HH/A/B# for hera, ND# for node)."
    )
    parser.add_argument("-e", "--easting", help="Easting of new station.", default=None)
    parser.add_argument(
        "-n", "--northing", help="Northing of new station", default=None
    )
    parser.add_argument(
        "-z", "--elevation", help="Elevation of new station", default=None
    )
    cm_utils.add_date_time_args(parser)
    parser.add_argument(
        "--station_type", help="Station category name", default=None
    )
    parser.add_argument("--datum", help="Datum of UTM [WGS84]", default="WGS84")
    parser.add_argument("--tile", help="UTM tile [34J]", default="34J")
    parser.add_argument(
        "--add_new_geo",
        help="Flag to allow update to add a new " "record.  [True]",
        action="store_false",
    )

    args = parser.parse_args()

    db = cm.connect_to_cm_db(args)
    session = db.sessionmaker()
    add_entry_to_stations(session, args)
    add_entry_to_parts(session, args)
