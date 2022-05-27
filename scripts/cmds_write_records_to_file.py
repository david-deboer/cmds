#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""script to write M&C records to a CSV file

"""

from astropy.time import Time, TimeDelta

from hera_mc import cm, cm_utils

valid_tables = {
    "antenna_status": {
        "method": "get_antenna_status",
        "filter_column": "antenna_number",
        "arg_name": "antenna_number",
    },
}

# get commands without write_to_file options:
#   get_correlator_config_file, get_ant_metric, get_array_metric, get_metric_desc

if __name__ == "__main__":
    parser = cm.get_mc_argument_parser()
    parser.description = """Write M&C records to a CSV file"""
    parser.add_argument("table", help="table to get info from")

    list_of_filter_args = []
    for table, table_dict in valid_tables.items():
        if "arg_name" in table_dict:
            arg_name = table_dict["arg_name"]
            if arg_name not in list_of_filter_args:
                list_of_filter_args.append(arg_name)
                parser.add_argument(
                    "--" + arg_name,
                    help="only include the specified " + arg_name,
                    default=None,
                )

    parser.add_argument("--filename", help="filename to save data to")
    parser.add_argument(
        "--start-date", dest="start_date", help="Start date YYYY/MM/DD", default=None
    )
    parser.add_argument(
        "--start-time", dest="start_time", help="Start time in HH:MM", default="17:00"
    )
    parser.add_argument(
        "--stop-date", dest="stop_date", help="Stop date YYYY/MM/DD", default=None
    )
    parser.add_argument(
        "--stop-time", dest="stop_time", help="Stop time in HH:MM", default="7:00"
    )
    parser.add_argument(
        "-l",
        "--last-period",
        dest="last_period",
        default=None,
        help="Time period from present for data (in minutes). "
        "If present ignores start/stop.",
    )

    args = parser.parse_args()

    if args.last_period:
        stop_time = Time.now()
        start_time = stop_time - TimeDelta(
            float(args.last_period) / (60.0 * 24.0), format="jd"
        )
    else:
        start_time = cm_utils.get_astropytime(args.start_date, args.start_time)
        stop_time = cm_utils.get_astropytime(args.stop_date, args.stop_time)

    with cm.CMSessionWrapper() as session:
        relevant_arg_name = valid_tables[args.table]["arg_name"]
        for arg in list_of_filter_args:
            if getattr(args, arg) is not None and arg != relevant_arg_name:
                print(
                    "{arg} is specified but does not apply to table {table}, "
                    "so it will be ignored.".format(arg=arg, table=args.table)
                )

        method_kwargs = {
            "starttime": start_time,
            "stoptime": stop_time,
            valid_tables[args.table]["filter_column"]: getattr(
                args, valid_tables[args.table]["arg_name"]
            ),
            "write_to_file": True,
            "filename": args.filename,
        }
        getattr(session, valid_tables[args.table]["method"])(**method_kwargs)
