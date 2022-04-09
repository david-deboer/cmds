# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""Defines the system architecture for the telescope."""
import json
from .cm import default_config_file

opposite_direction = {"up": "down", "down": "up"}


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
        return sysdef_file, json.load(fp)


class Sysdef:
    """
    Defines the system architecture for the telescope array for given architecture.

    """

    def __init__(self, hookup_type=None, sysdef=None):
        if sysdef is None:
            self.sysdef_filename, sysdef = get_sysdef(None)
        if hookup_type is None:
            hookup_type = sysdef['default_type']
        self.type = hookup_type
        self.polarizations = sysdef['polarization_defs'][self.type]
        self.components = sysdef['components']
        self._set_hookup(sysdef['hookup_defs'][self.type])  # Set attribute hookup

    def _set_hookup(self, hdef):
        self.hookup = []
        for i, hd in enumerate(hdef):
            if isinstance(hd, dict):
                self.hookup.append(list(hd.keys())[0])
                self.components.update(hd)
            else:
                self.hookup.append(hd)

    def get_thru_port(self, port, side, pol, part_type):
        """
        Return the port on the other side, given other port etc.

        Parameters
        ----------
        port : str
            Name of port.
        side : str
            Side of part for that port
        pol : str
            Polarization used.
        part_type : str
            Type of part

        Returns
        -------
        str
            Name of port on the other side.

        """
        if port is None:
            return None
        otherside = opposite_direction[side]
        other_side_ports = self.components[part_type][otherside][pol]
        # If only 1
        if len(other_side_ports) == 1:
            return other_side_ports[0]
        this_side_ports = self.components[part_type][side][pol]
        if len(this_side_ports) == len(other_side_ports):
            # Matched input/output (?)
            return other_side_ports[this_side_ports.index(port)]
        try:
            # Have same name.
            return other_side_ports[other_side_ports.index(port)]
        except ValueError:
            for p in other_side_ports:
                if p[0] == pol[0]:
                    # First with same initial pol letter
                    return p
        print("Sysdef Warning: 'get_thru_port' criteria not met -- 'None' returned.")
        return None
