# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""Defines the system architecture for the telescope array."""
import json

from .cm import default_config_file


def get_sysdef(args):
    """
    Return the sysdef json file.
    """
    sysdef_file = None
    if args is None:
        config_file = default_config_file
    elif hasattr(args, "sysdef_file"):
        sysdef_file = args.sysdef_file
    else:
        config_file = args.config_file
    if sysdef_file is None:
        with open(config_file) as fp:
            config = json.load(fp)
            sysdef_file = config["sysdef_files"][config['default_sysdef_name']]
    with open(sysdef_file) as fp:
        return json.load(fp)


class Sysdef:
    """
    Defines the system architecture for the telescope array for given architecture.

    """

    opposite_direction = {"up": "down", "down": "up"}

    def __init__(self, hookup_type=None, sysdef=None):
        if sysdef is None:
            sysdef = get_sysdef(None)
        if hookup_type is None:
            hookup_type = sysdef['default_type']
        self.type = hookup_type
        self.polarizations = sysdef['polarization_defs'][self.type]
        self.hookup = sysdef['hookup_defs'][self.type]
        self.components = sysdef['components']
