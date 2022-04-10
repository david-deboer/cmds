# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Keep track of geo-located stations.

Top modules are generally called by external (to CM) scripts.
Bottom part is the class that does the work.
"""

import copy
from pyuvdata import utils as uvutils
from numpy import radians

from . import cm, cm_utils, cm_active

default_plot_values = {'xgraph': 'easting',
                       'ygraph': 'northing',
                       'label': 'name',
                       'lblrng': '1:',
                       'marker_color': 'k',
                       'marker_shape': 'o',
                       'marker_size': 3
                       }


class Stations:
    """
    Class to allow various manipulations of stations and their properties etc.

    Parameters
    ----------
    session : Session object
        session on current database. If session is None, a new session
        on the default database is created and used.

    """

    lat_corr = {"J": 10000000, "T": 0}  # don't know about T

    def __init__(self, at_date='now', at_time=None, float_format=None, session=None):
        if session is None:  # pragma: no cover
            db = cm.connect_to_cm_db(None)
            self.session = db.sessionmaker()
        else:
            self.session = session
        self.date = cm_utils.get_astropytime(at_date, at_time, float_format)
        self.axes_set = False
        self.fp_out = None
        self.station_types_plotted = False
        self.active = cm_active.ActiveData(at_date=self.date)
        self.active.load_stations()

    def start_file(self, fname):
        """
        Open file for writing.

        Parameters
        ----------
        fname :  str
            File name to write to.

        """
        import os.path as op

        self.file_type = fname.split(".")[-1]
        write_header = False
        if op.isfile(fname):  # pragma: no cover
            print("{} exists so appending to it".format(fname))
        else:
            write_header = True
            print("Writing to new {}".format(fname))
        if not self.testing:  # pragma: no cover
            self.fp_out = open(fname, "a")
            if write_header:
                self.fp_out.write("{}\n".format(self._stn_line("header")))

    def _parse_tile(self, stn):
        tile = self.active.stations[stn].tile
        return int(tile[:2]), tile[-1]

    def get_stations(self, station_list='all'):
        """
        Get Station objects for a list of station_names, include uvdata.

        Parameters
        ----------
        to_find_list :  list of str
            station names to find

        Returns
        -------
        list of Station objects
            Station objects corresponding to station names.

        """
        import cartopy.crs as ccrs

        using_all = False
        if station_list == 'all':
            station_list = list(self.active.stations.keys())
            using_all = True
        latlon_p = ccrs.Geodetic()
        stations = []
        for station_name in station_list:
            if using_all or station_name in self.active.stations:
                tile = self._parse_tile(station_name)
                utm_p = ccrs.UTM(tile[0])
                lat_corr = self.lat_corr[tile[1]]
                stn = copy.copy(self.active.stations[station_name])
                # a.desc = self.station_types[a.station_type_name]["Description"] from Sysdef now
                stn.lon, stn.lat = latlon_p.transform_point(
                    stn.easting, stn.northing - lat_corr, utm_p
                )
                stn.X, stn.Y, stn.Z = uvutils.XYZ_from_LatLonAlt(
                    radians(stn.lat), radians(stn.lon), stn.elevation
                )
                stn.desc = "Get info from sysdef"
                stations.append(stn)
                if self.fp_out is not None:
                    self.fp_out.write("{}\n".format(self._stn_line(stn)))
        return stations

    def _stn_line(self, stn):
        """
        Return a list or str of the given stations, depending if stn is list or not.

        Parameters
        ----------
        stn : stations class, list of them, or 'header'
            List of stations class (or single)
        fmt : str
            Type of format; either 'csv' or not

        Return
        ------
        list-of-str or str : a single line containing the data
            if stn=='header' it returns the header line
        """
        if stn == "header":
            if self.file_type == "csv":
                return "name,easting,northing,longitude,latitude,elevation,X,Y,Z"
            else:
                return (
                    "name  easting   northing   longitude latitude  elevation"
                    "    X              Y               Z"
                )
        is_list = True
        if not isinstance(stn, list):
            stn = [stn]
            is_list = False
        ret = []
        for a in stn:
            if self.file_type == "csv":
                s = "{},{},{},{},{},{},{},{},{}".format(
                    a.station_name, a.easting, a.northing,
                    a.lon, a.lat, a.elevation,
                    a.X, a.Y, a.Z)
            else:
                s = "{:6s} {:.2f} {:.2f} {:.6f} {:.6f} {:.1f} {:.6f} {:.6f} {:.6f}".format(
                    a.station_name, a.easting, a.northing,
                    a.lon, a.lat, a.elevation,
                    a.X, a.Y, a.Z)
            ret.append(s)
        if not is_list:
            ret = ret[0]
        return ret

    def print_stn_info(self, stn_list):
        """
        Print out station information as returned from get_stations

        Parameters
        ----------
        stn_list : list of Stations classes
            List of station to print information for.

        """
        if stn_list is None or len(stn_list) == 0:
            print("No stnations found.")
            return
        for a in stn_list:
            print("station_name: ", a.station_name)
            print("\teasting: ", a.easting)
            print("\tnorthing: ", a.northing)
            print("\tlon/lat:  ", a.lon, a.lat)
            print("\televation: ", a.elevation)
            print("\tX, Y, Z: {}, {}, {}".format(a.X, a.Y, a.Z))
            print("\tstation description ({}):  {}".format(a.station_type, a.desc))
            print("\tcreated:  ", cm_utils.get_time_for_display(a.created_gpstime, float_format='gps'))

    def get_station_label(self, label_to_show, stn, show=':'):
        """
        Get a label for a station.

        name is the station name
        id is the manufacturer_id of the connected part
        conn is the connected part

        Parameters
        ----------
        label_to_show : str
            Specify label type, one of ["name", "id", "conn"]
        stn : Stations object
            station to get label for.
        show : str
            range of string to show, default is all (':')

        Returns
        -------
        str
            station label

        """
        if label_to_show == "name":
            lbl = stn.station_name
        else:
            self.active.load_connections()
            try:
                conn = self.active.connections['up']['ground'][stn.station_name].downstream_part
            except KeyError:
                return '-'
            if label_to_show == 'conn':
                lbl = conn
            elif label_to_show == 'id':
                self.active.load_parts()
                lbl = self.active.parts[conn].manufacturer_id
        return lbl[cm_utils.str2slice(show, lbl)]

    def plot_stations(self, stations_to_plot, **kwargs):  # pragma: no cover
        """
        Plot a list of stations.

        Parameters
        ----------
        stations_to_plot : list of Stations
            list containing stations classes to plot
        kwargs :  dict
            arguments for [xgraph, ygraph, label, lblrng, marker_color, marker_shape, marker_size]

        """
        if not len(stations_to_plot) or not self.graph:
            return
        for oarg in default_plot_values:
            if oarg not in kwargs.keys():
                kwargs[oarg] = default_plot_values[oarg]
        displaying_label = bool(kwargs["label"])
        if displaying_label:
            label_to_show = kwargs["label"].lower()
        fig_label = "{} vs {} stations".format(
            kwargs["xgraph"], kwargs["ygraph"]
        )

        import matplotlib.pyplot as plt
        for a in stations_to_plot:
            pt = {
                "easting": a.easting, "northing": a.northing, "elevation": a.elevation,
                "lat": a.lat, "lon": a.lon, "X": a.X, "Y": a.Y, "Z": a.Z}
            x_vals = pt[kwargs["xgraph"]]
            y_vals = pt[kwargs["ygraph"]]
            plt.plot(x_vals, y_vals, color=kwargs["marker_color"], label=a.station_name,
                     marker=kwargs["marker_shape"],
                     markersize=kwargs["marker_size"])
            if displaying_label:
                labeling = self.get_station_label(label_to_show, a, show=kwargs['lblrng'])
                if labeling:
                    plt.annotate(labeling, xy=(x_vals, y_vals), xytext=(x_vals, y_vals))
        if not self.axes_set:
            self.axes_set = True
            plt.xlabel(kwargs["xgraph"])
            plt.ylabel(kwargs["ygraph"])
            plt.title(fig_label)
        return

    def get_active_stations(self, station_types_to_use, query_date, query_time=None, float_format=None,
                            hookup_type=None):
        """
        Get active stations.

        Parameters
        ----------
        station_types_to_use : str or list of str
            Stations to use, can be a list of stations or "all" or "default".
        query_date :  Anything that `get_astropytime` can translate.
            Date for query.
        query_time : Anything that `get_astropytime` can translate.
            Time for query, ignored if at_date is a float or contains time information.
        float_format : str or None
            Format if query_date is unix or gps or jd day.
        hookup_type : str
            hookup_type to use

        Returns
        -------
        list of GeoLocation objects
            List of GeoLocation objects for all active stations.

        """
        from . import cm_hookup

        query_date = cm_utils.get_astropytime(query_date, query_time, float_format)
        hookup = cm_hookup.Hookup(self.session)
        hookup_dict = hookup.get_hookup(
            hookup.hookup_list_to_cache, at_date=query_date, hookup_type=hookup_type
        )
        self.station_types_to_use = self.parse_station_types_to_check(
            station_types_to_use
        )
        active_stations = []
        if len(active_stations):
            print("{}.....".format(12 * "."))
            print("{:12s}  {:3d}".format("active", len(active_stations)))
        return self.get_stations(active_stations)

    def plot_station_types(self, station_types_to_use, query_date, query_time=None, float_format=None,
                           **kwargs):
        """
        Plot the various sub-array types.

        Parameters
        ----------
        station_types_to_use : str or list of str
            station_types or prefixes to plot.
        query_date :  Anything that `get_astropytime` can translate.
            Date for query.
        query_time : Anything that `get_astropytime` can translate.
            Time for query, ignored if at_date is a float or contains time information.
        float_format : str or None
            Format if query_date is unix or gps or jd day.
        kwargs :  dict
            matplotlib arguments for marker_color, marker_shape, marker_size, label, xgraph, ygraph

        """
        if self.station_types_plotted:
            return
        self.station_types_plotted = True
        self.axes_set = False
        station_types_to_use = self.parse_station_types_to_check(station_types_to_use)
        total_plotted = 0
        for st in sorted(station_types_to_use):
            kwargs["marker_color"] = self.station_types[st]["Marker"][0]
            kwargs["marker_shape"] = self.station_types[st]["Marker"][1]
            kwargs["marker_size"] = 5
            stations_to_plot = self.get_stations(
                self.station_types[st]["Stations"], query_date, query_time, float_format
            )
            self.plot_stations(stations_to_plot, **kwargs)
            if len(stations_to_plot):
                print("{:12s}  {:3d}".format(st, len(stations_to_plot)))
                total_plotted += len(stations_to_plot)
        print("{:12s}  ---".format(" "))
        print("{:12s}  {:3d}".format("Total", total_plotted))
