#! /usr/bin/env python

from cmds import cm_sysdef
import argparse
from copy import copy


ap = argparse.ArgumentParser()
ap.add_argument('-s', '--sysdef', help="Sysdef json file name.", default="sysdef.json")
ap.add_argument('-k', '--hookup', help="Hookup to show.", default=None)
args = ap.parse_args()

sys = cm_sysdef.Sysdef(args.sysdef, None)

if args.hookup is None:
    args.hookup = copy(sys.sysdef_json['hookup_defs'])
else:
    args.hookup = args.hookup.split(',')

for hu in args.hookup:
    sys.get_hookup(hu)
    sys.print_sysdef()
