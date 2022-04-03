#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""
Script to handle installing a new station into system.
"""

from cmds import cm, cm_table_util, cm_utils, cm_tables


def station_data(args):
    cpattr = ['station_name', 'station_type', 'easting', 'northing', 'elevation',
              'datum', 'tile']
    data = {}
    for cc in cpattr:
        data[cc] = getattr(args, cc)
    return data


def part_data(session, args):
    data = {"action": 'add',
            "pn": args.station_name.upper(),
            "ptype": "station",
            "manufacturer_id": "{}:{}".format(int(args.northing), int(args.easting))}
    return data


if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument("station_type", help="Type of station.")
    parser.add_argument("station_name", help="Name of station.")
    parser.add_argument("-e", "--easting", help="Easting of new station.",
                        type=float, default=0.0)
    parser.add_argument("-n", "--northing", help="Northing of new station",
                        type=float, default=0.0)
    parser.add_argument("-z", "--elevation", help="Elevation of new station",
                        type=float, default=0.0)
    cm_utils.add_date_time_args(parser)
    parser.add_argument("--datum", help="Datum of UTM", default="WGS84")
    parser.add_argument("--tile", help="UTM tile", default="-")
    args = parser.parse_args()
    date = cm_utils.get_astropytime(args.date, args.time, args.format)

    db = cm.connect_to_cm_db(args)
    session = db.sessionmaker()
    cm_table_util.update_stations([station_data(args)], [date], session)
    cm_tables.update_parts([part_data(args)], [date], session)
    session.close()
