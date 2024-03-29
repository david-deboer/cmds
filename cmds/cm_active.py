#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Methods to load all active data for a given date."""

from copy import copy
from . import cm_utils, cm_tables


def get_active(at_date="now", at_time=None, float_format=None, loading=["apriori"]):
    """
    Return an ActiveData object with specified loading.

    This allows for a simple way to get the active data, however if more
    transactions are needed, please use the session wrapper in cm to generate a
    context managed session and pass that session to the class.

    Parameters
    ----------
    at_date : anything interpretable by cm_utils.get_astropytime
        Date at which to initialize.
    at_time : anything interpretable by cm_utils.get_astropytime
        Time at which to initialize, ignored if at_date is a float or contains time information
    float_format : str
        Format if at_date is a number denoting gps or unix seconds or jd day.
    loading : list
        List of active components to load: parts, connections, apriori, info, geo

    Returns
    -------
    active : ActiveData object
        ActiveData objects with loading parameters as specified.
    """
    from . import cm

    with cm.CMSessionWrapper() as session:
        active = ActiveData(
            session=session, at_date=at_date, at_time=at_time, float_format=float_format
        )
        for param in loading:
            getattr(active, f"load_{param}")()
        return active


class ActiveData:
    """
    Object containing the active data (parts, connections, stations, etc) for a given date.

    Info is handled slightly differently in that the 'active' time may be different.

    Parameters
    ----------
    at_date : str, int, float, Time, datetime

    """

    def __init__(self, session, at_date="now", at_time=None, float_format=None):
        """
        Initialize ActiveData class attributes for at_date.


        Parameters
        ----------
        session : session object
            sqalchemy session to use for getting data
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to initialize.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps or unix seconds or jd day.
        """
        self.session = session
        self.at_date = cm_utils.get_astropytime(at_date, at_time, float_format)
        self.info_date = None
        self.reset_all()

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
                print("New date does not agree with class date - resetting attributes.")
                self.at_date = this_date
                self.reset_all()
        return self.at_date.gps

    def load_parts(self, at_date=None, at_time=None, float_format=None):
        """
        Retrieve all active parts for a given at_date.

        Loads the active parts onto the class and sets the class at_date.
        If at_date is None, the existing at_date on the object will be used.

        Writes class dictionary:
                self.parts - keyed on part

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
            self.parts[key] = copy(prt)
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
            if chk in check_keys["up"]:
                raise ValueError("Duplicate active port {}".format(chk))
            check_keys["up"].append(chk)
            chk = f"{cnn.downstream_part}-{cnn.downstream_input_port}"
            if chk in check_keys["down"]:
                raise ValueError("Duplicate active port {}".format(chk))
            check_keys["down"].append(chk)
            key = cnn.upstream_part
            self.connections["up"].setdefault(key, {})
            self.connections["up"][key][cnn.upstream_output_port.lower()] = copy(cnn)
            key = cnn.downstream_part
            self.connections["down"].setdefault(key, {})
            self.connections["down"][key][cnn.downstream_input_port.lower()] = copy(cnn)

    def load_info(self, at_date=None, at_time=None, float_format=None, bracket=False):
        """
        Retrieve all comments after date (can supply a different one) and if bracket before self.at_date

        If at_date is None, the existing at_date on the object will be used.

        Writes class dictionary:
                self.info - keyed on part

        Parameters
        ----------
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to use.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to use, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps, unix seconds or jd
        bracket : bool
            If True, and 'at_date' is provided, it will return dates within self.info_date - self.at_date
            If no new date is supplied, this is ignored.

        """
        
        self.info = {}
        if not bracket or at_date is None:
            gps_time = self.set_active_time(at_date, at_time, float_format)
            bracket = False
            self.info_date = None
        else:
            gps_time = self.at_date.gps
            self.info_date = cm_utils.get_astropytime(at_date, at_time, float_format)
            if self.info_date > self.at_date:
                print(f"{self.info_date} shouldn't be after {self.at_date}")
                print("But continuing anyway -- lower date is the Big Bang")
        for info in self.session.query(cm_tables.PartInfo).filter(
            (cm_tables.PartInfo.posting_gpstime <= gps_time) ):
            if bracket and info.posting_gpstime < self.info_date.gps:
                continue
            key = info.pn
            self.info.setdefault(key, [])
            self.info[key].append(copy(info))

    def load_apriori(self, at_date=None, at_time=None, float_format=None):
        """
        Retrieve all active apriori status for a given at_date.

        Loads the active apriori data onto the class and sets the class at_date.
        If at_date is None, the existing at_date on the object will be used.

        Writes class dictionary:
                self.apriori - keyed on part

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
        for astat in self.session.query(cm_tables.AprioriStatus).filter(
            (cm_tables.AprioriStatus.start_gpstime <= gps_time)
            & (
                (cm_tables.AprioriStatus.stop_gpstime > gps_time)
                | (cm_tables.AprioriStatus.stop_gpstime == None)  # noqa
            )
        ):
            key = astat.pn
            if key in apriori_keys:
                raise ValueError(f"{key} already has an active apriori state.")
            apriori_keys.append(key)
            self.apriori[key] = copy(astat)

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
            key = asta.station_name
            self.stations[key] = copy(asta)

    def get_ptypes(self, ptypes='all'):
        """
        Return a dict of all active parts of type ptype or 'all'.

        Note that this assumes that self.load_parts() has been run and will error
        otherwise.  This is to keep the 'at_date' clearer.

        Parameters
        ----------
        ptypes : list or 'all'
            Valid HERA part type name (e.g. node, antenna, fem, ...)

        Returns
        -------
        dict
            Contains all part number keys of that type keyed on ptype.
        """
        ptype_list = {}
        if isinstance(ptypes, str):
            ptypes = ptypes.split(',')
        for key, partclass in self.parts.items():
            for this_ptype in ptypes:
                if partclass.ptype == this_ptype or this_ptype == 'all':
                    ptype_list.setdefault(partclass.ptype, [])
                    ptype_list[partclass.ptype].append(partclass.pn)
                    break
        print(f"Part types ({self.at_date.datetime.isoformat(timespec='minutes')}): "
              "{', '.join(sorted(ptype_list.keys()))}")
        return ptype_list
