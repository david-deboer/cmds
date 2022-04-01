# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R. DeBoer
# Licensed under the 2-clause BSD license.

"""All of the tables defined here."""

from astropy.time import Time
from sqlalchemy import BigInteger, Column, ForeignKeyConstraint, String, Text, Float
from . import MCDeclarativeBase, NotNull


class Stations(MCDeclarativeBase):
    """
    A table logging stations, which are geographical locations.

    Attributes
    ----------
    station_name : String Column
        Colloquial name of station (which is a unique location on the ground).
        This one shouldn't change. Primary_key
    station_type : String Column
        Type of station.
    datum : String Column
        Datum of the geoid.
    tile : String Column
        UTM tile
    northing : Float Column
        Northing coordinate in m
    easting : Float Column
        Easting coordinate in m
    elevation : Float Column
        Elevation in m
    created_gpstime : BigInteger Column
        The date when the station assigned by project.

    """

    __tablename__ = "stations"

    station_name = Column(String(64), primary_key=True)
    station_type = Column(String(64), nullable=False)
    datum = Column(String(64))
    tile = Column(String(64))
    northing = Column(Float(precision="53"))
    easting = Column(Float(precision="53"))
    elevation = Column(Float)
    created_gpstime = NotNull(BigInteger)

    def gps2Time(self):
        """Add a created_date attribute -- an astropy Time object based on created_gpstime."""
        self.created_date = Time(self.created_gpstime, format="gps")

    def station(self, **kwargs):
        """Allow specification of an arbitrary station value."""
        made_change = False
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key == 'station_name':
                    value = value.upper()
                setattr(self, key, value)
                made_change = True
            else:
                print("{} is not a valid station entry.".format(key))
                continue
        return made_change

    def __repr__(self):
        """Define representation."""
        return "<station_name={self.station_name} station_type={self.station_type} \
        northing={self.northing} easting={self.easting} \
        elevation={self.elevation}>".format(
            self=self
        )


class Parts(MCDeclarativeBase):
    """
    A table logging parts within the system.

    Attributes
    ----------
    pn : String Column
        System part number for each part; part of the primary key.
    ptype : String Column
        A part-dependent string, i.e. feed, frontend, ...
    manufacturer_id : String Column
        A part number/serial number as specified by manufacturer
    start_gpstime : BigInteger Column
        The date when the part was installed (or otherwise assigned by project).
    stop_gpstime : BigInteger Column
        The date when the part was removed (or otherwise de-assigned by project).

    """

    __tablename__ = "parts"

    pn = Column(String(64), primary_key=True)
    ptype = NotNull(String(64))
    manufacturer_id = Column(String(64))
    start_gpstime = Column(BigInteger, nullable=False)
    stop_gpstime = Column(BigInteger)

    def __repr__(self):
        """Define representation."""
        return (
            "<sysPartNumber id={self.pn} "
            "type={self.ptype} :: "
            "{self.start_gpstime} - {self.stop_gpstime}>".format(
                self=self
            )
        )

    def __eq__(self, other):
        """Define equality."""
        if (
            isinstance(other, self.__class__)
            and self.pn.upper() == other.pn.upper()
        ):
            return True
        return False

    def gps2Time(self):
        """Make astropy.Time object from gps."""
        self.start_date = Time(self.start_gpstime, format="gps")
        if self.stop_gpstime is None:
            self.stop_date = None
        else:
            self.stop_date = Time(self.stop_gpstime, format="gps")

    def part(self, **kwargs):
        """Allow specification of an arbitrary part value."""
        made_change = False
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key == 'pn':
                    value = value.upper()
                setattr(self, key, value)
                made_change = True
            else:
                print("{} is not a valid part entry.".format(key))
                continue
        return made_change


class AprioriAntenna(MCDeclarativeBase):
    """
    Table for a priori antenna status.

    Parameters
    ----------
    antenna :  str
        antenna designation
    start_gpstime : int
        start time for antenna status
    stop_gpstime : int
        stop time for antenna status
    status :  str
        status - TBD
    """

    __tablename__ = "apriori_antenna"

    antenna = Column(Text, primary_key=True)
    start_gpstime = Column(BigInteger, primary_key=True)
    stop_gpstime = Column(BigInteger)
    status = Column(Text, nullable=False)

    def __repr__(self):
        """Define representation."""
        return "<{}: {}  [{} - {}]>".format(
            self.antenna, self.status, self.start_gpstime, self.stop_gpstime
        )

    def valid_statuses(self):
        """Define current valid statuses GET FROM SYSDEF.JSON!."""
        return [
            "DEFINE IN SYSDEF.JSON!",
            "active",
            "maintenance",
            "etc"
        ]


class PartInfo(MCDeclarativeBase):
    """
    A table for logging test information etc for parts.

    Attributes
    ----------
    pn : String Column
        A system part number for each part.
    posting_gpstime : BigInteger Column
        Time that the data are posted. Part of the primary_key
    comment : String Column
        Comment associated with this data - or the data itself...
    reference : String Column
        Other reference associated with this entry.
    """

    __tablename__ = "part_info"

    pn = Column(String(64), nullable=False, primary_key=True)
    posting_gpstime = NotNull(BigInteger, primary_key=True)
    comment = NotNull(String(2048))
    reference = Column(String(256))

    def __repr__(self):
        """Define representation."""
        return (
            "<heraPartNumber id = {self.pn} "
            "comment = {self.comment}>".format(self=self)
        )

    def gps2Time(self):
        """Add a posting_date attribute (astropy Time object) based on posting_gpstime."""
        self.posting_date = Time(self.posting_gpstime, format="gps")

    def info(self, **kwargs):
        """Add arbitrary attributes passed in a dict to this object."""
        for key, value in kwargs.items():
            setattr(self, key, value)


class Connections(MCDeclarativeBase):
    """
    A table for logging connections between parts.

    Part and Port must be unique when combined

    Attributes
    ----------
    upstream_part : String Column
        up refers to the skyward part,
        Signal flows from A->B"
        Part of the primary_key, Foreign Key into parts
    upstream_output_port : String Column
        connected output port on upstream (skyward) part.
        Part of the primary_key
    downstream_part : String Column
        down refers to the part that is further from the sky, e.g.
        Part of the primary_key, Foreign Key into parts
    downstream_input_port : String Column
        connected input port on downstream (further from the sky) part
        Part of the primary_key
    start_gpstime : BigInteger Column
        start_time is the time that the connection is set
        Part of the primary_key
    stop_gpstime : BigInteger Column
        stop_time is the time that the connection is removed

    """

    __tablename__ = "connections"

    upstream_part = Column(String(64), nullable=False, primary_key=True)
    upstream_output_port = NotNull(String(64), primary_key=True)
    downstream_part = Column(String(64), nullable=False, primary_key=True)
    downstream_input_port = NotNull(String(64), primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["upstream_part"], [Parts.pn]
        ),
        ForeignKeyConstraint(
            ["downstream_part"], [Parts.pn]
        ),
    )

    start_gpstime = NotNull(BigInteger, primary_key=True)
    stop_gpstime = Column(BigInteger)

    def __repr__(self):
        """Define representation."""
        up = "{self.upstream_part}".format(self=self)
        down = "{self.downstream_part}".format(self=self)
        return (
            "<{}<{self.upstream_output_port}|{self.downstream_input_port}>{}>".format(
                up, down, self=self
            )
        )

    def __eq__(self, other):
        """Define equality."""
        if (
            isinstance(other, self.__class__)
            and self.upstream_part.upper() == other.upstream_part.upper()
            and self.upstream_output_port.lower() == other.upstream_output_port.lower()
            and self.downstream_part.upper() == other.downstream_part.upper()
            and self.downstream_input_port.lower() == other.downstream_input_port.lower()
        ):
            return True
        return False

    def gps2Time(self):
        """
        Add start_date and stop_date attributes (astropy Time objects).

        Based on start_gpstime and stop_gpstime.
        """
        self.start_date = Time(self.start_gpstime, format="gps")
        if self.stop_gpstime is None:
            self.stop_date = None
        else:
            self.stop_date = Time(self.stop_gpstime, format="gps")

    def connection(self, **kwargs):
        """Allow specification of arbitrary connection."""
        made_change = False
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key in ['upstream_part', 'downstream_part']:
                    value = value.upper()
                elif key in ['upstream_output_port', 'downstream_input_port']:
                    value = value.lower()
                setattr(self, key, value)
                made_change = True
            else:
                print("{} is not a valid connection entry.".format(key))
                continue
        return made_change

    def _to_dict(self):
        return {
            "upstream_part": self.upstream_part,
            "upstream_output_port": self.upstream_output_port,
            "downstream_part": self.downstream_part,
            "downstream_input_port": self.downstream_input_port,
        }
