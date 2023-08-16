#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Contains the Dossier and XEntry classes which serves as a "dossier" for part and hookup entries."""

from argparse import Namespace
from itertools import zip_longest

from . import cm_utils


class Dossier:

    def __init__(self, **kwargs):
        self.dossier = {}
        self.get_dossier(**kwargs)

    def get_dossier(self, pn, at_date="now", at_time=None, float_format=None,
                    active=None, notes_start_date="<", notes_start_time=None,
                    notes_float_format=None, exact_match=True, session=None, **kwargs):
        """
        Get information on a part or parts.

        Parameters
        ----------
        pn : str, list
            Part number [string or list-of-strings] (whole or first part thereof)
        at_date : anything interpretable by cm_utils.get_astropytime
            Date for which to check.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to check, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps or unix seconds or jd day
        active : cm_active.ActiveData class or None
            Use supplied ActiveData.  If None, read in.
        notes_start_date : anything interpretable by cm_utils.get_astropytime
            Start_date for displaying notes
        notes_start_time : anything interpretable by cm_utils.get_astropytime
            Start time for displaying notes, ignored if notes_start_date is a float or
            contains time information
        notes_float_format : str
            Format if notes_start_date is a number denoting gps or unix seconds or jd day.
        exact_match : bool
            Flag to enforce full part number match, or "startswith"
        kwargs:
            skip_pn_list_gather:  skip redoing the pn_list step (use pn as is)

        Returns
        -------
        dict
            dictionary keyed on the part_number:rev containing DossierEntry
            dossier classes

        """

        at_date = cm_utils.get_astropytime(at_date, at_time, float_format)
        notes_start_date = cm_utils.get_astropytime(
            notes_start_date, notes_start_time, notes_float_format
        )
        if active is None:
            from . import cm_active
            active = cm_active.ActiveData(session, at_date=at_date)
        elif at_date is not None:
            date_diff = abs(at_date - active.at_date).sec
            if date_diff > 1.0:
                raise ValueError(
                    "Supplied date and active date do not agree "
                    "({}sec)".format(date_diff)
                )
        else:
            at_date = active.at_date
        if active.parts is None:
            active.load_parts(at_date=at_date)
        if active.connections is None:
            active.load_connections(at_date=at_date)
        if active.info is None:
            active.load_info(at_date=at_date)
        if active.stations is None:
            active.load_stations(at_date=at_date)

        if 'skip_pn_list_gather' in kwargs:
            pn_list = pn
        else:
            pn_list = cm_utils.get_pn_list(pn, list(active.parts.keys()), exact_match)

        for this_pn in pn_list:
            this_part = DossierEntry(
                pn=this_pn,
                at_date=at_date,
                notes_start_date=notes_start_date,
            )
            this_part.get_entry(active)
            self.dossier[this_pn] = this_part

    def show_dossier(self, columns=None):
        """
        Generate part information print string.  Uses tabulate package.

        Parameter
        ---------
        columns : list
            List of column headers to use.  If None, use all

        Returns
        -------
        str
            String containing the dossier table.
        """
        from tabulate import tabulate

        pd_keys = cm_utils.put_keys_in_order(list(self.dossier.keys()))
        if len(pd_keys) == 0:
            return "Part not found"
        table_data = []
        headers = self.dossier[pd_keys[0]].get_headers(columns=columns)
        for pn in pd_keys:
            new_rows = self.dossier[pn].table_row(columns=columns)
            for nr in new_rows:
                table_data.append(nr)
        return "\n" + tabulate(table_data, headers=headers, tablefmt="orgtbl") + "\n"


class DossierEntry:
    """
    Holds all of the information on a given part.

    It includes connections, part_info, and, if applicable, station location.

    It contains the modules to format the dossier for use in the parts display matrix.
    Full available list is in col_hdr dict below.

    Parameters
    ----------
    pn : str
        System part number - for a single part, not list.  Note: only looks for exact matches.
    at_date : astropy.Time
        Date after which the part is active.  If inactive, the part will still be included,
        but things like notes, station etc may exclude on that basis.
    notes_start_date : astropy.Time
        Start date on which to filter notes.  The stop date is at_date above.

    """

    col_hdr = {
        "pn": "System P/N",
        "ptype": "Part Type",
        "manufacturer_id": "Mfg #",
        "start_gpstime": "Start",
        "stop_gpstime": "Stop",
        "input_ports": "Input",
        "output_ports": "Output",
        "station": "Geo",
        "comment": "Note",
        "posting_gpstime": "Date",
        "reference": "File",
        "pol": "Polarization",
        "up.start_gpstime": "uStart",
        "up.stop_gpstime": "uStop",
        "up.upstream_part": "Upstream",
        "up.upstream_output_port": "uOut",
        "up.downstream_input_port": "uIn",
        "down.upstream_output_port": "dOut",
        "down.downstream_input_port": "dIn",
        "down.downstream_part": "Downstream",
        "down.start_gpstime": "dStart",
        "down.stop_gpstime": "dStop",
    }

    def __init__(self, pn, at_date="now", notes_start_date="<"):
        self.pn = pn
        self.entry_key = pn
        self.at_date = at_date
        self.notes_start_date = notes_start_date
        # Below are the database components of the dossier
        self.input_ports = []
        self.output_ports = []
        self.part = None
        self.part_info = Namespace(comment=[], posting_gpstime=[], reference=[])
        self.connections = Namespace(up=None, down=None)
        self.station = None

    def __repr__(self):
        """Define representation."""
        return "{} -- {}".format(self.pn, self.part)

    def get_entry(self, active):
        """
        Get the part dossier entry.

        Parameters
        ----------
        active : ActiveData class
            Contains the active database entries

        """
        self.part = active.parts[self.entry_key]
        self.part.gps2Time()
        self._get_connections(active=active)
        self._get_part_info(active=active)
        self._get_station(active=active)
        self._add_ports()

    def _add_ports(self):
        """Pull out the input_ports and output_ports to a class variable."""
        if self.connections.down is not None:
            self.input_ports = [x.lower() for x in self.connections.down.keys()]
        if self.connections.up is not None:
            self.output_ports = [x.lower() for x in self.connections.up.keys()]

    def _get_connections(self, active):
        """
        Retrieve the connection info for the part in self.hpn.

        Parameters
        ----------
        active : ActiveData class
            Contains the active database entries.

        """
        if self.entry_key in active.connections["up"].keys():
            self.connections.up = active.connections["up"][self.entry_key]
        if self.entry_key in active.connections["down"].keys():
            self.connections.down = active.connections["down"][self.entry_key]

    def _get_part_info(self, active):
        """
        Retrieve the part_info for the part in self.hpn.

        Parameters
        ----------
        active : ActiveData class
            Contains the active database entries.

        """
        if self.entry_key in active.info.keys():
            for pi_entry in active.info[self.entry_key]:
                if pi_entry.posting_gpstime > self.notes_start_date.gps:
                    self.part_info.comment.append(pi_entry.comment)
                    self.part_info.posting_gpstime.append(pi_entry.posting_gpstime)
                    self.part_info.reference.append(pi_entry.reference)

    def _get_station(self, active):
        """
        Retrieve the geographical information for the part in self.hpn.

        Parameter
        ---------
        active : ActiveData class
            Contains the active database entries.

        """
        if self.pn in active.stations.keys():
            self.station = active.stations[self.pn]

    def get_headers(self, columns):
        """
        Generate the header titles for the given columns.

        The returned headers are used in the tabulate display.

        Parameters
        ----------
        columns : list
            List of columns to show.

        Returns
        -------
        list
            The list of the associated headers
        """
        headers = []
        for c in columns:
            headers.append(self.col_hdr[c])
        return headers

    def table_row(self, columns, ports=None):
        """
        Convert the part_dossier column information to a row for the tabulate display.

        Parameters
        ----------
        columns : list
            List of the desired columns to use.
        ports : list
            Allowed ports to show.

        Returns
        -------
        list
            A row or rows for the tabulate display.
        """
        # This section generates the appropriate ports to use, if necessary.
        conns = [[None, None]]
        ports_included = False
        for col in columns:
            if "up." in col or "down." in col:
                ports_included = True
                conns = zip_longest(
                    [self.connections.down[x.lower()] for x in self.input_ports],
                    [self.connections.up[x.lower()] for x in self.output_ports],
                )
                break
        if ports_included and ports is not None:
            new_up = []
            new_dn = []
            for up, down in conns:
                if (
                    up is None
                    or up.upstream_output_port.lower() in ports
                    or up.downstream_input_port.lower() in ports
                ):
                    new_up.append(up)
                else:
                    new_up.append(None)
                if (
                    down is None
                    or down.upstream_output_port.lower() in ports
                    or down.downstream_input_port.lower() in ports
                ):
                    new_dn.append(down)
                else:
                    new_up.append(None)
            conns = zip(new_up, new_dn)

        # This section pulls the appropriate value for a given cell out of the data.
        tdata = []
        for up, down in conns:
            trow = []
            no_port_data = True
            number_entries = 0
            for col in columns:
                cbeg = col.split(".")[0]
                cend = col.split(".")[-1]
                try:
                    x = getattr(self, col)
                except AttributeError:
                    try:
                        x = getattr(self.part, col)
                    except AttributeError:
                        try:
                            x = getattr(self.part_info, col)
                        except AttributeError:
                            use = up if cbeg == "up" else down
                            try:
                                x = getattr(use, cend)
                            except AttributeError:
                                x = None
                if cbeg in ["up", "down"] and x is not None:
                    no_port_data = False
                if col == "comment" and x is not None and len(x):
                    x = "\n".join([y.strip() for y in x])
                elif col == "station" and x is not None:
                    x = "{:.1f}E, {:.1f}N, {:.1f}m".format(
                        x.easting, x.northing, x.elevation
                    )
                elif cend in ["start_gpstime", "stop_gpstime"]:
                    x = cm_utils.get_time_for_display(x, float_format="gps")
                elif cend == "posting_gpstime":
                    x = "\n".join(
                        [
                            cm_utils.get_time_for_display(y, float_format="gps")
                            for y in x
                        ]
                    )
                elif isinstance(x, (list, set)):
                    x = ", ".join(x)
                trow.append(x)
                if x is not None and len(x):
                    number_entries += 1
            if ports_included and no_port_data:
                trow = None
            if trow is not None and len(trow):
                if number_entries > 1 or number_entries == len(columns):
                    tdata.append(trow)
        return tdata
