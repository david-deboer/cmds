# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""This is the base class for the script generators."""
import datetime
import os
from . import signal_chain, cm_gsheet


class Update():
    """Base update class."""

    def __init__(self, script_type, script_path='default', verbose=True):
        """
        Initialize.

        Parameters
        ----------
        script_type : str
            Type of the script to generate (becomes component of dated name)
        script_path : str
            Path to the script.
        verbose : bool
            Verbose or not.
        """
        if script_path == 'default':
            from hera_mc import mc
            self.script_path = mc.get_cm_csv_path()
        else:
            self.script_path = script_path
        self.verbose = verbose
        self.now = datetime.datetime.now()
        self.cdate = self.now.strftime('%Y/%m/%d')
        self.ctime = self.now.strftime('%H:%M')
        time_offset = self.now + datetime.timedelta(seconds=100)
        self.cdate2 = time_offset.strftime('%Y/%m/%d')
        self.ctime2 = time_offset.strftime('%H:%M')
        if script_type is None:
            self.script = None
        else:
            self.script = '{}_{}_{}'.format(self.cdate.replace('/', '')[2:], script_type,
                                            self.ctime.replace(':', ''))
            self.script = os.path.join(self.script_path, self.script)
        self.hera = signal_chain.Update(script_to_run=self.script,
                                        chmod=True, verbose=verbose,
                                        cdate=self.cdate, ctime=self.ctime)
        self.update_counter = 0
        self.gsheet = None

    def load_gsheet(self, node_csv='none', tabs=None, path='', time_tag='_%y%m%d'):
        """Get the googlesheet information from the internet."""
        if self.gsheet is None:
            self.gsheet = cm_gsheet.SheetData()
        self.gsheet.load_sheet(node_csv=node_csv, tabs=None, path=path, time_tag=time_tag)

    def load_gworkflow(self):
        if self.gsheet is None:
            self.gsheet = cm_gsheet.SheetData()
        self.gsheet.load_workflow()

    def finish(self, cron_script=None, archive_to=None):
        """
        Close out process.  If no updates, it deletes the script file.
        If both parameters are None, it just leaves things alone.

        Parameters
        ----------
        cron_script : str or None
            If str, copies the script file to that.  Assumes in same directory as script.
            If no updates in script, then makes empty one.
        archive_to : str or None
            If str, moves the script file to that directory.  If not, deletes.
        """
        self.hera.done()
        if self.script is None or (cron_script is None and archive_to is None):
            return
        if cron_script is not None:
            cron_script = os.path.join(self.script_path, cron_script)
            if os.path.exists(cron_script):
                os.remove(cron_script)

        if self.update_counter == 0:
            os.remove(self.script)
            if self.verbose:
                print("No updates found.  Removing {}.".format(self.script))
            if cron_script is not None:
                with open(cron_script, 'w') as fp:
                    fp.write('\n')
                if self.verbose:
                    print("Writing empty {}.".format(cron_script))
        else:
            if archive_to is not None:
                os.system('cp {} {}'.format(self.script, archive_to))
                if self.verbose:
                    print("Copying {}  -->  {}".format(self.script, archive_to))
            if cron_script is not None:
                os.rename(self.script, cron_script)
                if self.verbose:
                    print("Moving {}  -->  {}".format(self.script, cron_script))
            else:
                os.remove(self.script)

        if os.path.exists(cron_script):
            os.chmod(cron_script, 0o755)
