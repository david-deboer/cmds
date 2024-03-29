#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Allows various views on the antenna hookup.

"""

from cmds import cm, cm_hookup, cm_utils

if __name__ == "__main__":
    parser = cm.get_cm_argument_parser()
    parser.add_argument(
        "-p",
        "--pn",
        help="Part number, csv-list. (default)",
        default="default",
    )
    parser.add_argument(
        "-e",
        "--exact-match",
        help="Force exact matches on part numbers, not beginning N char.",
        dest="exact_match",
        action="store_true",
    )
    parser.add_argument(
        "--pol", help="Define desired pol(s) for hookup or 'all'", default="all"
    )
    parser.add_argument(
        "--all",
        help="Toggle to show 'all' hookups as opposed to 'full'",
        action="store_true",
    )
    parser.add_argument(
        "--notes", help="If set, this will also show hookup notes.", action="store_true"
    )
    parser.add_argument(
        "-c",
        "--hookup-cols",
        help="Specify a subset of parts to show in hookup, csv-list or all (all])",
        default="all",
    )
    parser.add_argument(
        "-t",
        "--hookup-type",
        help="Use specified hookup type - the default is in the sysdef json file.",
        default='ata-rfsoc',
    )
    parser.add_argument(
        "--ports", help="Show ports on hookup.", action="store_true"
    )
    parser.add_argument(
        "-f",
        "--file",
        help="output filename, if desired.  Tags are '.txt', '.html', '.csv' to set type.",
        default=None,
    )
    parser.add_argument(
        "-s",
        "--sortby",
        help="Part-type column order to sort display or None. (None)",
        default=None,
    )
    cm_utils.add_date_time_args(parser)

    args = parser.parse_args()
    # Pre-process the args
    at_date = cm_utils.get_astropytime(args.date, args.time, args.format)
    args.hookup_cols = cm_utils.listify(args.hookup_cols)
    state = "all" if args.all else "full"
    if args.file is None:
        output_format = "display"
    else:
        print("Writing data to {}".format(args.file))
        output_format = args.file.split(".")[-1]

    # Start session
    with cm.CMSessionWrapper() as session:
        hookup = cm_hookup.Hookup(session)

        hookup_dict = hookup.get_hookup(pn=args.pn, at_date=at_date,
                                        exact_match=args.exact_match, hookup_type=args.hookup_type)
        show = hookup.show_hookup(cols_to_show=args.hookup_cols, signal_paths_to_show=args.pol,
                                  ports=args.ports, sortby=args.sortby, state=state,
                                  filename=args.file, output_format=output_format)
        if output_format == "display":
            print(show)
        if args.notes:
            print("\nNotes:\n---------------------------------------------------------------")
            print(hookup.show_notes(state=state))
            print("-------------------------------------------------------------------------")
