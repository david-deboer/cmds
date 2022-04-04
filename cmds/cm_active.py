#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Methods to load all active data for a given date."""

from . import cm_utils, cm_tables


class ActiveData:
    """
    Object containing the active data (parts, connections, stations, etc) for a given date.

    Parameters
    ----------
    at_date : str, int, float, Time, datetime

    """

    def __init__(self, session=None, at_date="now", at_time=None, float_format=None):
        """
        Initialize ActiveData class attributes for at_date.

        It creates all attributes and sets them to None.  Another attribute
        'pytest_param' is set within to allow for fine-grained unit testing
        without the need for an init argument.  It allows for certain keys to
        be set in the method 'load_connections' to check edge cases.

        Parameters
        ----------
        session : session object or None
            If None, it will start a new session on the database.
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to initialize.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps or unix seconds or jd day.
        """
        if session is None:  # pragma: no cover
            from . import cm
            db = cm.connect_to_cm_db(None)
            session = db.sessionmaker()
        self.session = session
        self.at_date = cm_utils.get_astropytime(at_date, at_time, float_format)
        self.reset_all()
        self.pytest_param = False

    def reset_all(self):
        """Reset all active attributes to None."""
        self.parts = None
        self.connections = None
        self.info = None
        self.apriori = None
        self.stations = None

    def set_active_time(self, at_date, at_time=None, float_format=None):
        """
        Make sure that at_date and self.at_date are synced and supplies gps time.

        This utility function checks that the Class date hasn't changed.  If so,
        then it will reset all of the Attributes.

        Parameters
        ----------
        at_date : anything interpretable by cm_utils.get_astropytime
            Date for which to check.  If none, returns self.at_date.gps
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps, unix seconds, or jd

        Returns
        -------
        int
            gps seconds of at_date

        """
        if at_date is not None:
            this_date = cm_utils.get_astropytime(at_date, at_time, float_format)
            if abs(this_date.gps - self.at_date.gps) > 1:
                self.at_date = this_date
                self.reset_all()
        return self.at_date.gps

    def load_parts(self, at_date=None, at_time=None, float_format=None):
        """
        Retrieve all active parts for a given at_date.

        Loads the active parts onto the class and sets the class at_date.
        If at_date is None, the existing at_date on the object will be used.

        Writes class dictionary:
                self.parts - keyed on part:rev

        Parameters
        ----------
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to initialize.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps, unix seconds or jd

        """
        gps_time = self.set_active_time(at_date, at_time, float_format)
        self.parts = {}
        for prt in self.session.query(cm_tables.Parts).filter(
            (cm_tables.Parts.start_gpstime <= gps_time)
            & (
                (cm_tables.Parts.stop_gpstime > gps_time)
                | (cm_tables.Parts.stop_gpstime == None)  # noqa
            )
        ):
            key = prt.pn
            self.parts[key] = prt
            self.parts[key].logical_pn = None

    def load_connections(self, at_date=None, at_time=None, float_format=None):
        """
        Retrieve all active connections for a given at_date.

        Loads the active parts onto the class and sets the class at_date.
        If at_date is None, the existing at_date on the object will be used.

        Writes class dictionary:
                self.connections - has keys 'up' and 'down', each of which
                                   is a dictionary keyed on part:rev for
                                   upstream_part and downstream_part respectively.

        Parameters
        ----------
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to initialize.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps, unix seconds or jd

        Raises
        ------
        ValueError
            If a duplicate is found.

        """
        gps_time = self.set_active_time(at_date, at_time, float_format)
        self.connections = {"up": {}, "down": {}}
        check_keys = {"up": [], "down": []}
        for cnn in self.session.query(cm_tables.Connections).filter(
            (cm_tables.Connections.start_gpstime <= gps_time)
            & (
                (cm_tables.Connections.stop_gpstime > gps_time)
                | (cm_tables.Connections.stop_gpstime == None)  # noqa
            )
        ):
            chk = f"{cnn.upstream_part}-{cnn.upstream_output_port}"
            if self.pytest_param:
                check_keys[self.pytest_param].append(chk)
            if chk in check_keys["up"]:
                raise ValueError("Duplicate active port {}".format(chk))
            check_keys["up"].append(chk)
            chk = f"{cnn.downstream_part}-{cnn.downstream_input_port}"
            if chk in check_keys["down"]:
                raise ValueError("Duplicate active port {}".format(chk))
            check_keys["down"].append(chk)
            key = cnn.upstream_part
            self.connections["up"].setdefault(key, {})
            self.connections["up"][key][cnn.upstream_output_port.lower()] = cnn
            key = cnn.downstream_part
            self.connections["down"].setdefault(key, {})
            self.connections["down"][key][cnn.downstream_input_port.lower()] = cnn

    def load_info(self, at_date=None, at_time=None, float_format=None):
        """
        Retrieve all current part infomation (ie. before date).

        Loads the part information up to at_date onto the class and sets the class at_date
        If at_date is None, the existing at_date on the object will be used.

        Writes class dictionary:
                self.info - keyed on part:rev

        Parameters
        ----------
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to initialize.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps, unix seconds or jd

        """
        gps_time = self.set_active_time(at_date, at_time, float_format)
        self.info = {}
        for info in self.session.query(cm_tables.PartInfo).filter(
            (cm_tables.PartInfo.posting_gpstime <= gps_time)
        ):
            key = info.pn
            self.info.setdefault(key, [])
            self.info[key].append(info)

    def load_apriori(self, at_date=None, at_time=None, float_format=None, rev="A"):
        """
        Retrieve all active apriori status for a given at_date.

        Loads the active apriori data onto the class and sets the class at_date.
        If at_date is None, the existing at_date on the object will be used.

        Writes class dictionary:
                self.apriori - keyed on part:rev

        Parameters
        ----------
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to initialize.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps, unix seconds or jd
        rev : str
            Revision of antenna-station (always A)

        """
        gps_time = self.set_active_time(at_date, at_time, float_format)
        self.apriori = {}
        apriori_keys = []
        for astat in self.session.query(cm_tables.AprioriAntenna).filter(
            (cm_tables.AprioriAntenna.start_gpstime <= gps_time)
            & (
                (cm_tables.AprioriAntenna.stop_gpstime > gps_time)
                | (cm_tables.AprioriAntenna.stop_gpstime == None)  # noqa
            )
        ):
            key = astat.antenna
            if key in apriori_keys:
                raise ValueError("{} already has an active apriori state.".format(key))
            apriori_keys.append(key)
            self.apriori[key] = astat

    def load_stations(self, at_date=None, at_time=None, float_format=None):
        """
        Retrieve all current stations (ie. before date).

        Loads the station data at_date onto the class and sets the class at_date.
        If at_date is None, the existing at_date on the object will be used.

        Writes class dictionary:
                self.stations - keyed on part

        Parameters
        ----------
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to initialize.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps, unix seconds or jd

        """

        gps_time = self.set_active_time(at_date, at_time, float_format)
        self.stations = {}
        for asta in self.session.query(cm_tables.Stations).filter(
            cm_tables.Stations.created_gpstime <= gps_time
        ):
            key = cm_utils.make_part_key(asta.station_name, None)
            self.stations[key] = asta

    def get_ptype(self, ptype):
        """
        Return a list of all active parts of type ptype.

        Note that this assumes that self.load_parts() has been run and will error
        otherwise.  This is to keep the 'at_date' clearer.

        Parameters
        ----------
        hptype : str
            Valid HERA part type name (e.g. node, antenna, fem, ...)

        Returns
        -------
        list
            Contains all part number keys (hpn:rev) of that type.
        """
        ptype_list = []
        for key, partclass in self.parts.items():
            if partclass.ptype == ptype:
                ptype_list.append(key)
        return ptype_list
