#! /usr/bin/env python

from cmds import cm_sysdef
import argparse

ap = argparse.ArgumentParser()
ap.add_argument('-s', '--sysdef', help="Sysdef json file name.", default="sysdef.json")
ap.add_argument('--hookup', help="Hookup to show.", default=None)
args = ap.parse_args()

sys = cm_sysdef.Sysdef(args.sysdef, None)

for hu in sys.sysdef_json['hookup_defs']:
    sys.get_hookup(hu)
    sys.print_sysdef()