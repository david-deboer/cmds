#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Contains the Dossier and DossierEntry classes which serves as a "dossier" for part and hookup entries."""

from argparse import Namespace
from itertools import zip_longest
from . import cm_utils, cm_active


class Dossier:

    def __init__(self, pn, exact_match=False, skip_pn_list_gather=False, active=['parts', 'connections', 'stations'],
                 at_date='now', at_time=None, float_format=None, session=None):
        """
        pn : str or list
            part number(s)
        exact_match : bool
            If True, will only use exact matches in pn
        skip_pn_list_gather : bool
            If True, it won't try to update the provided pn
        active : self.active.ActiveData or list of modules to load (except info, which always gets loaded later)
            If not list, use this ActiveData
        at_date : anything interpretable by cm_utils.get_astropytime
            Date for which to check.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to check, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps or unix seconds or jd day

        """
        self.dossier = {}
        if isinstance(active, list):
            self.active = cm_active.ActiveData(session, at_date=at_date, at_time=at_time, float_format=float_format)
            for param in active:
                getattr(self.active, f'load_{param}')()
        else:
            self.active = active
        self.at_date = self.active.at_date
        if skip_pn_list_gather:
            self.pn = pn
        else:
            self.pn = cm_utils.get_pn_list(pn, list(self.active.parts.keys()), exact_match)

    def load_dossier(self, window=None):
        """
        Load the DossierEntry for the supplied part numbers.

        Parameters
        ----------

        window : int or str or None
            History length in days to view.  Used to set active.info_time.
            If None, it is ignored
            If int, number of days (bracket [at_date-window, at_date])
            If str, it will pass through to cm_astropytime (bracket [window, at_date])

        Attribute
        -------
        dict
            dictionary keyed on the part_number containing DossierEntry
            dossier classes

        """

        if window is None:
            bracket = False
        else:
            bracket = True
            if not isinstance(window, str):
                from datetime import timedelta
                window = self.at_date.datetime - timedelta(days=window)
        self.active.load_info(at_date=window, bracket=bracket)

        for this_pn in self.pn:
            this_part = DossierEntry(pn=this_pn)
            this_part.get_entry(self.active)
            if this_part.use:
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

    def __init__(self, pn):
        self.pn = pn  # a single part number, not a list!
        # Below are the database components of the dossier
        self.input_ports = []
        self.output_ports = []
        self.part = None
        self.part_info = Namespace(comment=[], posting_gpstime=[], reference=[], pol=[])
        self.connections = Namespace(up=None, down=None)
        self.station = None

    def __repr__(self):
        """Define representation."""
        if self.part is not None:
            return "{} -- {}".format(self.pn, self.part)
        return str(self.pn)

    def get_entry(self, active):
        """
        Get the part dossier entry.

        Parameters
        ----------
        active : ActiveData class
            Contains the active database entries

        """
        if isinstance(active.parts, dict) and self.pn in active.parts:
            self.part = active.parts[self.pn]
        if isinstance(active.connections, dict):
            if self.pn in active.connections["up"]:
                self.connections.up = active.connections["up"][self.pn]
            if self.pn in active.connections["down"]:
                self.connections.down = active.connections["down"][self.pn]
        if isinstance(active.stations, dict) and self.pn in active.stations.keys():
                self.station = active.stations[self.pn]
        self._get_part_info(active=active)
        self._add_ports()

        self.use = self.part is not None or self.part_info is not None

    def _add_ports(self):
        """Pull out the input_ports and output_ports to a class variable."""
        if self.connections.down is not None:
            self.input_ports = [x.lower() for x in self.connections.down.keys()]
        if self.connections.up is not None:
            self.output_ports = [x.lower() for x in self.connections.up.keys()]

    def _get_part_info(self, active):
        """
        Retrieve the part_info for the part in self.hpn.

        Parameters
        ----------
        active : ActiveData class
            Contains the active database entries.

        """
        if isinstance(active.info, dict) and self.pn in active.info.keys():
            for pi_entry in active.info[self.pn]:
                self.part_info.comment.append(pi_entry.comment)
                self.part_info.posting_gpstime.append(pi_entry.posting_gpstime)
                self.part_info.reference.append(pi_entry.reference)
                self.part_info.pol.append(pi_entry.pol)
        else:
            self.part_info = None

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
                    x = ", ".join([str(tmp) for tmp in x])
                trow.append(x)
                if x is not None and len(x):
                    number_entries += 1
            if ports_included and no_port_data:
                trow = None
            if trow is not None and len(trow):
                if number_entries > 1 or number_entries == len(columns):
                    tdata.append(trow)
        return tdata
