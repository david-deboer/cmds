#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Contains the HookupEntry class for  hookup entries."""

from copy import copy


def _polport(pol, port, demarc='-'):
    if port == 'split':
        return pol.split(demarc)
    return f"{pol}{demarc}{port}"


class HookupEntry(object):
    """
    Holds the structure of the hookup entry.  All are keyed on polarization-port.

    Parameters
    ----------
    entry_key : str
        Entry key to use for the entry.  Must be None if input_dict is not None.
    input_dict : dict
        Dictionary with seed hookup.  If it is None, entry_key and sysdef must both be provided.

    """

    def __init__(self, entry_key=None, sysdef=None):
        self.hookup = {}  # actual hookup connection information
        self.fully_connected = {}  # flag if fully connected
        self.columns = {}  # list with the actual column headers in hookup
        self.timing = {}  # aggregate hookup start and stop
        self.part_type = {}
        self.sysdef = sysdef
        self.columns = copy(self.sysdef.hookup)
        self.columns.append("start")
        self.columns.append("stop")

    def __repr__(self):
        """Define representation."""
        s = str(self.hookup)
        return s

    def add_extras(self, polport, pt_cache):
        """
        Add the timing and fully_connected flag for the hookup.

        Parameters
        ----------
        polport : str
            Part port to get.

        """
        full_hookup_length = len(self.sysdef.hookup) - 1
        latest_start = 0
        earliest_stop = None
        self.part_type[polport] = []
        for c in self.hookup[polport]:
            self.part_type[polport].append(pt_cache[c.upstream_part])
            if c.start_gpstime > latest_start:
                latest_start = c.start_gpstime
            if c.stop_gpstime is None:
                pass
            elif earliest_stop is None:
                earliest_stop = c.stop_gpstime
            elif c.stop_gpstime < earliest_stop:
                earliest_stop = c.stop_gpstime
        self.part_type[polport].append(pt_cache[self.hookup[polport][-1].downstream_part])
        self.timing[polport] = [latest_start, earliest_stop]
        self.fully_connected[polport] = len(self.hookup[polport]) == full_hookup_length

    def table_entry_row(self, polport, cols, show):
        """
        Produce the hookup table row for given parameters.

        Parameters
        ----------
        polport : str
            Polarization type (specified in 'cm_sysdef')
        cols : list
            Columns to include
        show : dict
            Dictionary containing flags of what components to show.

        Returns
        -------
        list
            List containing the table entry.

        """
        timing = self.timing[polport]
        td = ["-"] * len(cols)

        # Get the first N-1 parts
        dip = ""
        for part_type, c in zip(self.part_type[polport], self.hookup[polport]):
            if part_type in cols:
                new_row_entry = self._build_new_row_entry(
                    dip, c.upstream_part, c.upstream_output_port, show
                )
                td[cols.index(part_type)] = new_row_entry
            dip = c.downstream_input_port + "> "
        # Get the last part in the hookup
        part_type = self.part_type[polport][-1]
        if part_type in cols:
            new_row_entry = self._build_new_row_entry(
                dip, c.downstream_part, None, show
            )
            td[cols.index(part_type)] = new_row_entry
        # Add timing
        if "start" in cols:
            td[cols.index("start")] = timing[0]
        if "stop" in cols:
            td[cols.index("stop")] = timing[1]
        return td

    def _build_new_row_entry(self, dip, part, port, show):
        """
        Format the hookup row entry.

        Parameters
        ----------
        dip : str
            Current entry display for the downstream_input_port
        part : str
            Current part name
        port : str
            Current port name
        show : dict
            Dictionary containing flags of what components to show.

        Returns
        -------
        str
            String containing that row entry.

        """
        new_row_entry = ""
        if show["ports"]:
            new_row_entry = dip
        new_row_entry += part
        if port is not None and show["ports"]:
            new_row_entry += " <" + port
        return new_row_entry