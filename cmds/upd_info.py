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


def _dict2msg(data, warning=False):
    if warning:
        return f"*************{data['ant']}:  {data['new_status']}*************"
    msg = f"-------------{data['ant']}: {data['cdate']}  {data['ctime']}\n"
    msg += f"\t{data['old_status']} --> {data['new_status']}\n"
    for info in data['info']:
        msg += f"\t{info}\n"
    msg += '\n'
    return msg


class UpdateInfo(upd_base.Update):
    """Generates the script to update comments and "apriori" info from the configuration gsheet."""

    def __init__(self, script_type='infoupd', script_path='default', verbose=True):
        """Init of base."""
        super(UpdateInfo, self).__init__(script_type=script_type,
                                         script_path=script_path,
                                         verbose=verbose)
        self.new_apriori = {}
        self.apriori_notify_file = None if self.script is None else os.path.join(os.path.dirname(self.script), 'apriori_notify.txt')
    
    def update_workflow(self):
        self.active.load_info()
        self.active.load_apriori()
        self.load_gsheet()
        self.gsheet.split_apriori()
        # self.add_apriori()
        self.gsheet.split_comments()
        self.add_sheet_notes()

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


    def update_apriori(self, antenna, status, cdate, ctime='12:00'):
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
        self.printit('cmds_update_apriori.py {} -s {} --date {} --time {}'
                      .format(antenna, status, cdate, ctime))

    def process_apriori_notification(self, notify_type='new'):
        """
        Processes the log apriori updates and send email digest.

        Parameters
        ----------
        notify_type : str
            one of 'either', 'old', 'new':  notify if status in old, new, either
        """
        print("L98:  For now, apriori notification process.")
        return
        from cmds import watch_dog
        from os import remove
        anotify = {}
        if os.path.isfile(self.apriori_notify_file):
            with open(self.apriori_notify_file, 'r') as fp:
                anotify = json.load(fp)
            remove(self.apriori_notify_file)
        else:
            return
        self.load_gworkflow()
        from_addr = 'hera@lists.berkeley.edu'
        msg_header = 'Apriori system changes.\n------------------------------\n'
        for email, n in self.gsheet.apriori_email.items():
            msg = "{}".format(msg_header)
            used_antdt = []
            for antdt, data in anotify.items():
                if notify_type == 'old':
                    using = [data['old_status']]
                elif notify_type == 'new':
                    using = [data['new_status']]
                else:
                    using = [data['old_status'], data['new_status']]
                for this_status in n.notify:
                    if '!Warning!' in data['new_status']:
                        msg += _dict2msg(data, warning=True)
                        used_antdt.append(antdt)
                    elif this_status in using and antdt not in used_antdt:
                        msg += _dict2msg(data, warning=False)
                        used_antdt.append(antdt)
            if msg != msg_header:
                to_addr = [email]
                watch_dog.send_email(msg_header.splitlines()[0], msg, to_addr,
                                     from_addr, skip_send=False)

    def log_apriori_notifications(self):
        """
        Log the found apriori updates to a file with update_info script.

        Gets processed and distributed per "process_apriori_notification".
        """
        print("L140:  For now not logging apriori stuff.")
        return
        if not len(self.new_apriori):
            return
        try:
            with open(self.apriori_notify_file, 'r') as fp:
                full_notify = json.load(fp)
        except FileNotFoundError:
            full_notify = {}
        for k, v in self.new_apriori.items():
            new_key = f"{v['ant']}|{v['cdate']}|{v['ctime']}"
            full_notify[new_key] = v
        with open(self.apriori_notify_file, 'w') as fp:
            json.dump(full_notify, fp, indent=4)

    def add_apriori(self):
        """Write out for apriori differences."""
        self.new_apriori = {}
        stmt_hdr = "apriori_antenna status change:"
        refout = 'apa-infoupd'
        for key, value in self.gsheet.apriori.items():
            print(key, value)
            print("old  ", self.active.apriori[key].status)
            if value != self.active.apriori[key].status:
                self.new_apriori[key] = {'info': []}
                self.new_apriori[key]['ant'] = key
                self.new_apriori[key]['old_status'] = self.active.apriori[key].status
                self.new_apriori[key]['new_status'] = value
                self.new_apriori[key]['cdate'] = self.cdate2
                self.new_apriori[key]['ctime'] = self.ctime2
                s = f"{self.new_apriori[key]['old_status']} > {self.new_apriori[key]['new_status']}"
                if self.verbose:
                    print(f"Updating {key}:  {s}")
                self.update_apriori(key, value, self.new_apriori[key]['cdate'], self.new_apriori[key]['ctime'])
                self.add_part_info(key, f"{stmt_hdr} {s}",
                                   self.new_apriori[key]['cdate'],
                                   self.new_apriori[key]['ctime'], ref=refout)
                self.update_counter += 1

    def add_sheet_notes(self, duplication_window=90.0, view_duplicate=0.0):
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
                    refout = 'infoupd'
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
                    if key.startswith('N'):
                        node = key
                    else:
                        node = self.gsheet.ant_to_node[key]
                    if self.verbose and ddays > view_duplicate:
                        print("Duplicate for {:8s}  ({}) - {}  ({:.1f} days)"
                              .format(key, node, statement, ddays))
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
