# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Keep track of geo-located stations.

Top modules are generally called by external (to CM) scripts.
Bottom part is the class that does the work.
"""

from copy import copy
from pyuvdata import utils as uvutils
from numpy import radians

from . import cm_utils, cm_active, cm_sysdef

default_plot_values = {'xgraph': 'easting',
                       'ygraph': 'northing',
                       'label': 'name',
                       'lblrng': '1:',
                       'color': 'k',
                       'shape': 'o',
                       'size': 3
                       }


class Stations:
    """
    Class to allow various manipulations of stations and their properties etc.

    Parameters
    ----------
    session : Session object
        session on current database.
    at_date : see cm_utils.get_astropytime
        "
    at_time : "
    float_format :"

    """

    lat_corr = {"J": 10000000, "T": 0}  # don't know about T

    def __init__(self, session, at_date='now', at_time=None, float_format=None):
        self.session = session
        self.date = cm_utils.get_astropytime(at_date, at_time, float_format)
        self.axes_set = False
        self.fp_out = None
        self.stations = None
        self.station_types_plotted = False
        self.active = cm_active.ActiveData(session=session, at_date=self.date)
        self.active.load_stations()
        self.sysdef = cm_sysdef.Sysdef()

    def _parse_tile(self, stn):
        tile = self.active.stations[stn].tile
        return int(tile[:2]), tile[-1]

    def get_stations(self, station_list='all', station_types="all"):
        """
        Load Station objects into self.stations to use for plotting/listing/etc

        Parameters
        ----------
        station_list :  list of str
            station names to find or 'all'
        station_types : list or str
            station_types to find or 'all':

        """
        import cartopy.crs as ccrs
        allactive = list(self.active.stations.keys())
        if station_list == 'all':
            station_list = allactive
        elif station_list == 'active':
            from cmds import cm_hookup
            hookup = cm_hookup.Hookup(self.session)
            station_list = []
            hookup.get_hookup(allactive, at_date=self.active.at_date, exact_match=True, active=self.active)
            print("STATION HOOKUP NOT WORKING")
            print(hookup.hookup)
        if station_types == 'all':
            station_types = list(self.sysdef.station_types.keys())
        latlon_p = ccrs.Geodetic()
        self.stations = {}
        for station_name in station_list:
            try:
                this_station = self.active.stations[station_name]
            except KeyError:
                continue
            if this_station.station_type not in station_types:
                continue
            tile = self._parse_tile(station_name)
            utm_p = ccrs.UTM(tile[0])
            lat_corr = self.lat_corr[tile[1]]
            stn = copy(self.active.stations[station_name])
            # a.desc = self.station_types[a.station_type_name]["Description"] from Sysdef now
            stn.lon, stn.lat = latlon_p.transform_point(
                stn.easting, stn.northing - lat_corr, utm_p
            )
            stn.X, stn.Y, stn.Z = uvutils.XYZ_from_LatLonAlt(
                radians(stn.lat), radians(stn.lon), stn.elevation
            )
            stn.desc = self.sysdef.station_types[stn.station_type]
            self.stations[stn.station_name] = stn
            if self.fp_out is not None:
                self.fp_out.write("{}\n".format(self._stn_line(stn)))

    def _stn_line(self, header=False):
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
        if header:
            if self.file_type == "csv":
                return "name,easting,northing,longitude,latitude,elevation,X,Y,Z"
            else:
                return (
                    "name  easting   northing   longitude latitude  elevation"
                    "    X              Y               Z"
                )

        ret = []
        for a in self.stations:
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
        return ret

    def print_stn_info(self):
        """
        Print out station information as returned from load_stations

        Parameters
        ----------
        stn_list : list of Stations classes
            List of station to print information for.

        """

        for a in self.stations:
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

    def plot_stations(self, stations_to_plot=None, **kwargs):
        """
        Plot self.stations.

        Parameters
        ----------
        stations_to_plot : list of Stations
            list containing stations classes to plot.  If None, use self.stations.
        kwargs :  dict
            arguments for [xgraph, ygraph, label, lblrng, marker_color, marker_shape, marker_size]

        """
        for oarg in default_plot_values:
            if oarg not in kwargs.keys():
                kwargs[oarg] = default_plot_values[oarg]
        displaying_label = bool(kwargs["label"])
        if displaying_label:
            label_to_show = kwargs["label"].lower()
        fig_label = "{} vs {} stations".format(
            kwargs["xgraph"], kwargs["ygraph"]
        )
        if stations_to_plot is None:
            stations_to_plot = self.stations

        import matplotlib.pyplot as plt
        for sta in stations_to_plot:
            a = self.active.stations[sta]
            x_vals = getattr(a, kwargs["xgraph"])
            y_vals = getattr(a, kwargs["ygraph"])
            plt.plot(x_vals, y_vals, color=kwargs["color"], label=a.station_name,
                     marker=kwargs["shape"],
                     markersize=kwargs["size"])
            if displaying_label:
                labeling = self.get_station_label(label_to_show, a, show=kwargs['lblrng'])
                if labeling:
                    plt.annotate(labeling, xy=(x_vals, y_vals), xytext=(x_vals, y_vals))
        if not self.axes_set:
            self.axes_set = True
            plt.xlabel(kwargs["xgraph"])
            plt.ylabel(kwargs["ygraph"])
            plt.title(fig_label)
