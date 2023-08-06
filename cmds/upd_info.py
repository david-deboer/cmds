# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.
"""
This class sets up to update the part information database.
"""
from . import cm_utils, cm_active
from . import upd_util, upd_base, cm_gsheet_ata
import os.path
import json


class UpdateInfo(upd_base.Update):
    """Generates the script to update comments and "apriori" info from the configuration gsheet."""

    def __init__(self, script_type='infoupd', script_path='default', verbose=True):
        """Init of base."""
        super(UpdateInfo, self).__init__(script_type=script_type,
                                         script_path=script_path,
                                         verbose=verbose)
        self.new_apriori = {}
        self.load_active(['info', 'apriori'])
    
    def update_workflow(self):
        """See cmds_auto_update_info.py"""
        self.load_gsheet()
        self.gsheet.split_apriori()
        self.add_apriori()
        self.gsheet.split_comments()
        self.add_comments()

    def add_part_info(self, pn, note, cdate, ctime, ref=None):
        """
        Add a note/comment for a part to the database.

        Parameters
        ----------
        pn : str
              HERA part number for comment
        note : str
               The desired note.
        cdate : str
                YYYY/MM/DD format
        ctime : str
                HH:MM format
        ref : str
              Reference note.
        """
        if not len(note.strip()):
            return
        if ref is None:
            ref = ''
        else:
            ref = f'-l "{ref}" '
        self.printit("cmds_update_info.py {} -c '{}' {}--date {} --time {}"
                      .format(pn, note, ref, cdate, ctime))


    def update_apriori(self, antenna, status, cdate, ctime='12:00', comment=None):
        """
        Update the antenna a priori status.

        Parameters
        ----------
        antenna : str
            Antenna part number, e.g. HH24
        status : str
            Antenna apriori enum string.
        cdate : str
            YYYY/MM/DD
        ctime : str, optional
            HH:MM, default is 12:00
        """
        if comment is None:
            commententry = ''
        else:
            commententry = f'-c "{comment}" '
        self.printit('cmds_update_apriori.py {} {} {}--date {} --time {}'
                      .format(antenna, status, commententry, cdate, ctime))

    def add_apriori(self, comment=None):
        """Write out for apriori differences."""
        self.new_apriori = {}
        stmt_hdr = "apriori_antenna status change:"
        for key, value in self.gsheet.apriori.items():
            print(key, value)
            try:
                old_status = self.active.apriori[key].status
            except KeyError:
                old_status = 'Not defined.'
            print(f"old apriori status: {old_status}")
            if value != old_status:
                self.new_apriori[key] = {'info': []}
                self.new_apriori[key]['ant'] = key
                self.new_apriori[key]['old_status'] = old_status
                self.new_apriori[key]['new_status'] = value
                self.new_apriori[key]['cdate'] = self.cdate2
                self.new_apriori[key]['ctime'] = self.ctime2
                s = f"{self.new_apriori[key]['old_status']} > {self.new_apriori[key]['new_status']}"
                if self.verbose:
                    print(f"Updating {key}:  {s}")
                self.update_apriori(key, value, self.new_apriori[key]['cdate'], self.new_apriori[key]['ctime'], comment=comment)
                self.add_part_info(key, f"{stmt_hdr} {s}",
                                   self.new_apriori[key]['cdate'],
                                   self.new_apriori[key]['ctime'],
                                   ref='apa-refout')
                self.update_counter += 1

    def add_comments(self, duplication_window=90.0, view_duplicate=0.0):
        """
        Search the relevant fields in the googlesheets and generate add note commands.

        Parameters
        ----------
        duplication_window : float
            time-frame in days over which to check for duplicate comments.
        """
        self.new_notes = {}
        for key, entries in self.gsheet.comments.items():
            # Process sheet data
            pdate = self.cdate + ''
            ptime = self.ctime + ''
            for comment in entries:
                if not self.is_duplicate(key, comment, duplication_window, view_duplicate):
                    refout = 'info update from gsheet'
                    self.new_notes.setdefault(key, [])
                    self.new_notes[key].append(comment)
                    if self.verbose:
                        print(f"Adding comment: {key} - {comment}")
                    self.add_part_info(key, comment, pdate, ptime, ref=refout)
                    self.update_counter += 1


    def is_duplicate(self, key, statement, duplication_window, view_duplicate=0.0):
        """Check if duplicate."""
        if key in self.active.info.keys():
            for note in self.active.info[key]:
                note_time = cm_utils.get_astropytime(note.posting_gpstime, float_format='gps').datetime  # noqa
                dt = self.now - note_time
                ddays = dt.days + dt.seconds / (3600.0 * 24)
                if ddays < duplication_window and statement == note.comment:
                    if self.verbose and ddays > view_duplicate:
                        print(f"Duplicate for {key:8s}  '{statement}' ({ddays:.1f} days)")
                    return True
        return False

    def view_info(self):
        """View it."""
        if len(self.new_apriori.keys()):
            print("New Apriori")
            for x, info in sorted(self.new_apriori.items()):
                print("{}:  {}".format(x, info))
        else:
            print("No new apriori")
        if len(self.new_notes.keys()):
            print("New Notes")
            for x, info in sorted(self.new_notes.items()):
                print("{}:  {}".format(x, info))
        else:
            print("No new notes")
