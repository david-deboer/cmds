# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R DeBoer
# Licensed under the 2-clause BSD license.

"""Defines the system architecture for the telescope."""
import json

from .cm import default_config_file

opposite_direction = {"up": "down", "down": "up"}


class Sysdef:
    """
    Defines the system architecture for the telescope array for given architecture.

    Parameters
    ----------
    hookup_type : str or None
    sysdef : str or None

    """
    def __init__(self, hookup_type=None, sysdef=None):
        self.get_sysdef(hookup_type, sysdef)

    def get_sysdef(self, hookup_type, sysdef=None):
        """
        Return the sysdef json file information as attributes.

        """
        if sysdef is None:
            with open(default_config_file) as fp:
                config = json.load(fp)
                self.sysdef_file = config["sysdef_files"][config['default_sysdef_name']]
        else:
            self.sysdef_file = sysdef
        with open(self.sysdef_file) as fp:
            self.sysdef_json = json.load(fp)

        if hookup_type is None:
            hookup_type = self.sysdef_json['default_type']
        self.type = hookup_type

        self.polarizations = self.sysdef_json['polarization_defs'][self.type]
        self.components = self.sysdef_json['components']
        self.station_types = self.sysdef_json['station_types']
        self.apriori_statuses = self.sysdef_json['apriori_statuses']
        self.hookup = []
        for i, hd in enumerate(self.sysdef_json['hookup_defs'][self.type]):
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
