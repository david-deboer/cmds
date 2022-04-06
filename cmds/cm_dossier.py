#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Contains the Entry classes which serves as a "dossier" for part and hookup entries."""

from argparse import Namespace
from itertools import zip_longest

from . import cm_sysdef, cm_utils, cm_tables


class Dossier:

    def __init__(self, **kwargs):
        self.dossier = {}
        self.get_part_dossier(**kwargs)

    def get_part_dossier(
        self,
        pn,
        at_date="now",
        at_time=None,
        float_format=None,
        active=None,
        notes_start_date="<",
        notes_start_time=None,
        notes_float_format=None,
        exact_match=True,
        session=None,
        **kwargs
    ):
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

        Returns
        -------
        dict
            dictionary keyed on the part_number:rev containing PartEntry
            dossier classes

        """
        from . import cm_active

        at_date = cm_utils.get_astropytime(at_date, at_time, float_format)
        notes_start_date = cm_utils.get_astropytime(
            notes_start_date, notes_start_time, notes_float_format
        )
        if active is None:
            active = cm_active.ActiveData(self.session, at_date=at_date)
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

        pn_list = cm_utils.get_pn_list(pn, list(active.parts.keys()), exact_match)

        for this_pn in pn_list:
            this_part = PartEntry(
                pn=this_pn,
                at_date=at_date,
                notes_start_date=notes_start_date,
            )
            this_part.get_entry(active)
            self.dossier[this_pn] = this_part

    def show_part_dossier(self, columns=None, ports=None):
        """
        Generate part information print string.  Uses tabulate package.

        Parameter
        ---------
        columns : list
            List of column headers to use.  If None, use all
        ports : list, str, None
            Ports to show.
            If None, counterintuitively, all are included
                (see cm_sysdef.all_port_types)
            If str, it assumes that types are provided
                (see cm_sysdef.all_port_types), specified as csv-list.
            If list, it only allows those.

        Returns
        -------
        str
            String containing the dossier table.
        """
        from tabulate import tabulate

        ports = [p.lower() for p in ports]
        pd_keys = cm_utils.put_keys_in_order(list(self.dossier.keys()))
        if len(pd_keys) == 0:
            return "Part not found"
        table_data = []
        headers = self.dossier[pd_keys[0]].get_headers(columns=columns)
        for pn in pd_keys:
            new_rows = self.dossier[pn].table_row(columns=columns, ports=ports)
            for nr in new_rows:
                table_data.append(nr)
        return "\n" + tabulate(table_data, headers=headers, tablefmt="orgtbl") + "\n"


class PartEntry:
    """
    Holds all of the information on a given part -- generally called from cm_handling.

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
        "up.start_gpstime": "uStart",
        "up.stop_gpstime": "uStop",
        "up.upstream_part": "Upstream",
        "up.up_part_rev": "uRev",
        "up.upstream_output_port": "uOut",
        "up.downstream_input_port": "uIn",
        "down.upstream_output_port": "dOut",
        "down.downstream_input_port": "dIn",
        "down.downstream_part": "Downstream",
        "down.down_part_rev": "dRev",
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
            self.input_ports = cm_utils.put_keys_in_order(
                [x.lower() for x in self.connections.down.keys()], "PN"
            )
        if self.connections.up is not None:
            self.output_ports = cm_utils.put_keys_in_order(
                [x.lower() for x in self.connections.up.keys()], "PN"
            )

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

    def table_row(self, columns, ports):
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
                    [self.connections.down[x.upper()] for x in self.input_ports],
                    [self.connections.up[x.upper()] for x in self.output_ports],
                )
                break
        if ports_included and ports is not None:
            new_up = []
            new_dn = []
            for up, down in conns:
                if (
                    up is None
                    or up.upstream_output_port.upper() in ports
                    or up.downstream_input_port.upper() in ports
                ):
                    new_up.append(up)
                else:
                    new_up.append(None)
                if (
                    down is None
                    or down.upstream_output_port.upper() in ports
                    or down.downstream_input_port.upper() in ports
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


class HookupEntry(object):
    """
    Holds the structure of the hookup entry.  All are keyed on polarization<port.

    Parameters
    ----------
    entry_key : str
        Entry key to use for the entry.  Must be None if input_dict is not None.
    sysdef : str
        Name of part type system for the hookup.  Must be None if input_dict is not None.
    input_dict : dict
        Dictionary with seed hookup.  If it is None, entry_key and sysdef must both be provided.

    """

    def __init__(self, entry_key=None, sysdef=None, input_dict=None):
        if input_dict is not None:
            if entry_key is not None:
                raise ValueError(
                    "cannot initialize HookupEntry with an " "entry_key and a dict"
                )
            if sysdef is not None:
                raise ValueError(
                    "cannot initialize HookupEntry with an " "sysdef and a dict"
                )
            self.entry_key = input_dict["entry_key"]
            hookup_connections_dict = {}
            for port, conn_list in input_dict["hookup"].items():
                new_conn_list = []
                for conn_dict in conn_list:
                    new_conn_list.append(cm_tables.get_connection(conn_dict))
                hookup_connections_dict[port] = new_conn_list
            self.hookup = hookup_connections_dict
            self.fully_connected = input_dict["fully_connected"]
            self.hookup_type = input_dict["hookup_type"]
            self.columns = input_dict["columns"]
            self.timing = input_dict["timing"]
            self.sysdef = cm_sysdef.Sysdef(input_dict=input_dict["sysdef"])
        else:
            if entry_key is None:
                raise ValueError(
                    "Must initialize HookupEntry with an " "entry_key and sysdef"
                )
            if sysdef is None:
                raise ValueError(
                    "Must initialize HookupEntry with an " "entry_key and sysdef"
                )
            self.entry_key = entry_key
            self.hookup = {}  # actual hookup connection information
            self.fully_connected = {}  # flag if fully connected
            self.hookup_type = {}  # name of hookup_type
            self.columns = {}  # list with the actual column headers in hookup
            self.timing = {}  # aggregate hookup start and stop
            self.sysdef = sysdef

    def __repr__(self):
        """Define representation."""
        s = "<{}:  {}>\n".format(self.entry_key, self.hookup_type)
        s += "{}\n".format(self.hookup)
        s += "{}\n".format(self.fully_connected)
        s += "{}\n".format(self.timing)
        return s

    def _to_dict(self):
        """Convert this object to a dict (so it can be written to json)."""
        hookup_connections_dict = {}
        for port, conn_list in self.hookup.items():
            new_conn_list = []
            for conn in conn_list:
                new_conn_list.append(conn._to_dict())
            hookup_connections_dict[port] = new_conn_list
        return {
            "entry_key": self.entry_key,
            "hookup": hookup_connections_dict,
            "fully_connected": self.fully_connected,
            "hookup_type": self.hookup_type,
            "columns": self.columns,
            "timing": self.timing,
            "sysdef": self.sysdef._to_dict(),
        }

    def get_hookup_type_and_column_headers(self, port, part_types_found):
        """
        Return the headers for the retrieved hookup.

        The columns in the hookup contain parts in the hookup chain and the
        column headers are the part types contained in that column.

        It just checks which hookup_type the parts are in and keeps however many
        parts are used.

        Parameters
        ----------
        port : str
            Part port to get, of the form 'POL<port', e.g. 'E<ground'
        part_types_found : list
            List of the part types that were found

        """
        self.hookup_type[port] = None
        self.columns[port] = []
        if len(part_types_found) == 0:
            return
        is_this_one = False
        for sp in self.sysdef.checking_order:
            for part_type in part_types_found:
                if part_type not in self.sysdef.full_connection_path[sp]:
                    break
            else:
                is_this_one = sp
                break
        if not is_this_one:
            print("Parts did not conform to any hookup_type")
            return
        else:
            self.hookup_type[port] = is_this_one
            for c in self.sysdef.full_connection_path[is_this_one]:
                if c in part_types_found:
                    self.columns[port].append(c)

    def add_timing_and_fully_connected(self, port):
        """
        Add the timing and fully_connected flag for the hookup.

        Parameters
        ----------
        port : str
            Part port to get, of the form 'POL<port', e.g. 'E<ground'

        """
        if self.hookup_type[port] is not None:
            full_hookup_length = (
                len(self.sysdef.full_connection_path[self.hookup_type[port]]) - 1
            )
        else:
            full_hookup_length = -1
        latest_start = 0
        earliest_stop = None
        for c in self.hookup[port]:
            if c.start_gpstime > latest_start:
                latest_start = c.start_gpstime
            if c.stop_gpstime is None:
                pass
            elif earliest_stop is None:
                earliest_stop = c.stop_gpstime
            elif c.stop_gpstime < earliest_stop:
                earliest_stop = c.stop_gpstime
        self.timing[port] = [latest_start, earliest_stop]
        self.fully_connected[port] = len(self.hookup[port]) == full_hookup_length
        self.columns[port].append("start")
        self.columns[port].append("stop")

    def get_part_from_type(self, part_type, include_revs=False, include_ports=False):
        """
        Retrieve the part name for a given part_type from a hookup.

        Parameters
        ----------
        part_type : str
            String of valid part type in hookup_dict (e.g. 'snap' or 'feed')
        include_revs : bool
            Flag to include revision number
        include_ports : bool
            Flag to include the associated ports to the part

        Returns
        -------
        dict
            Dictionary keyed on polarization for actual installed part number of
            specified type within hookup as a string per pol
                if include_revs part number is e.g. FDV1:A
                if include_ports they are included as e.g. 'input>FDV:A<terminals'

        """
        parts = {}
        extra_cols = ["start", "stop"]
        for port, names in self.columns.items():
            if part_type not in names:
                parts[port] = None
                continue
            iend = 1
            for ec in extra_cols:
                if ec in self.columns[port]:
                    iend += 1
            part_ind = names.index(part_type)
            is_first_one = part_ind == 0
            is_last_one = part_ind == len(names) - iend
            # Get part number
            if is_last_one:
                part_number = self.hookup[port][part_ind - 1].downstream_part
            else:
                part_number = self.hookup[port][part_ind].upstream_part
            # Get rev
            rev = ""
            if include_revs:
                if is_last_one:
                    rev = ":" + self.hookup[port][part_ind - 1].down_part_rev
                else:
                    rev = ":" + self.hookup[port][part_ind].up_part_rev
            # Get ports
            in_port = ""
            out_port = ""
            if include_ports:
                if is_first_one:
                    out_port = "<" + self.hookup[port][part_ind].upstream_output_port
                elif is_last_one:
                    in_port = (
                        self.hookup[port][part_ind - 1].downstream_input_port + ">"
                    )
                else:
                    out_port = "<" + self.hookup[port][part_ind].upstream_output_port
                    in_port = (
                        self.hookup[port][part_ind - 1].downstream_input_port + ">"
                    )
            # Finish
            parts[port] = "{}{}{}{}".format(in_port, part_number, rev, out_port)
        return parts

    def table_entry_row(self, port, columns, part_types, show):
        """
        Produce the hookup table row for given parameters.

        Parameters
        ----------
        port : str
            Polarization type, 'e' or 'n' for HERA (specified in 'cm_sysdef')
        columns : list
            Desired column headers to display
        part_types : dict
            Dictionary containing part_types
        show : dict
            Dictionary containing flags of what components to show.

        Returns
        -------
        list
            List containing the table entry.

        """
        timing = self.timing[port]
        td = ["-"] * len(columns)
        # Get the first N-1 parts
        dip = ""
        for d in self.hookup[port]:
            part_type = part_types[d.upstream_part]
            if part_type in columns:
                new_row_entry = self._build_new_row_entry(
                    dip, d.upstream_part, d.up_part_rev, d.upstream_output_port, show
                )
                td[columns.index(part_type)] = new_row_entry
            dip = d.downstream_input_port + "> "
        # Get the last part in the hookup
        part_type = part_types[d.downstream_part]
        if part_type in columns:
            new_row_entry = self._build_new_row_entry(
                dip, d.downstream_part, d.down_part_rev, None, show
            )
            td[columns.index(part_type)] = new_row_entry
        # Add timing
        if "start" in columns:
            td[columns.index("start")] = timing[0]
        if "stop" in columns:
            td[columns.index("stop")] = timing[1]
        return td

    def _build_new_row_entry(self, dip, part, rev, port, show):
        """
        Format the hookup row entry.

        Parameters
        ----------
        dip : str
            Current entry display for the downstream_input_port
        part : str
            Current part name
        rev : str
            Current part revision
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
        if show["revs"]:
            new_row_entry += ":" + rev
        if port is not None and show["ports"]:
            new_row_entry += " <" + port
        return new_row_entry
