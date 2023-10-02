"""Class for config gsheet."""
import csv
import requests
import os.path


gsheet_prefix = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR40OGLUdmYOnIWogF1SxeP2FgSmmsNOuYR3vGR2n0XmjJq_Tv0MBViGt6fPTQkhtaXVmAyG2FuzzA5/pub?'
gsheet = {}
gsheet['Antenna'] = 'gid=0&single=true&output=csv'
gsheet['Tuning'] = 'gid=616087514&single=true&output=csv'

no_prefix = ['Comments']


class SheetData:
    """Class for googlesheet."""

    def __init__(self):
        """Initialize dictionaries/lists."""
        self.tabs = []
        for key in gsheet.keys():
            gsheet[key] = gsheet_prefix + gsheet[key]
            self.tabs.append(key)
        # It reads into the variables below
        self.header = {}
        self.ants = {}
        self.tunings = {}
        self.rfsocs = {}
        self.rfcbs = {}
        self.snaps = {}
        self.obs = set()

    def load_sheet(self, arc_csv='none', tabs=None, path='.', time_tag='%y%m%d'):
        """
        Get the googlesheet information from the internet (or locally for testing etc).

        Parameters
        ----------
        arc_csv : str
            node csv file status:  one of 'read', 'write', 'none' (only need first letter)
            'read' uses a local version as opposed to internet version
            'write' writes a local version
            'none' does neither of the above
        tabs : none, str, list
            List of tabs to use.  None == all of them.
        path : str
            Path to use if reading/writing csv files.
        time_tag : str
            If non-zero length string use as time_tag format for the output files (if arc_csv=w).
        """
        arc_csv = arc_csv[0].lower()
        if tabs is None or str(tabs) == 'all':
            tabs = self.tabs
        elif isinstance(tabs, str):
            tabs = tabs.split(',')
        if arc_csv != 'n' and isinstance(time_tag, str) and len(time_tag):
            from datetime import datetime
            ttag = f"_{datetime.strftime(datetime.now(), time_tag)}"
        else:
            ttag = ""
        check_rfcb_part_port = []
        check_rfsoc_part_port = []
        check_snap_part_port = []
        for tab in tabs:
            obs_line_reached = False
            ofnc = os.path.join(path, f"{tab}{ttag}.csv")
            if arc_csv == 'r':
                csv_data = []
                with open(ofnc, 'r') as fp:
                    for line in fp:
                        csv_data.append(line)
            else:
                try:
                    xxx = requests.get(gsheet[tab])
                except:  # noqa
                    import sys
                    e = sys.exc_info()[0]
                    print(f"Error reading {gsheet[tab]}: {e}")
                    return
                csv_tab = b''
                for line in xxx:
                    csv_tab += line
                csv_data = csv_tab.decode('utf-8').splitlines()
            csv_tab = csv.reader(csv_data)
            if arc_csv == 'w':
                with open(ofnc, 'w') as fp:
                    fp.write('\n'.join(csv_data))
            for data in csv_tab:
                if data[0].startswith('$'):
                    self.header[tab] = [_x.strip('$') for _x in data]
                    continue
                elif data[0].startswith('#'):
                    if data[0].startswith('#Obs'):
                        obs_line_reached = True
                    continue
                if obs_line_reached:  # Everything relates to observatory overall
                    for cell in data:
                        if len(cell):
                            self.obs.add(cell)
                elif tab == 'Antenna':
                    this_ant = data[0].upper()
                    if this_ant in self.ants:
                        raise ValueError(f"{this_ant} is already present.")
                    self.ants[this_ant] = [_x.strip() for _x in data]
                elif tab == 'Tuning':
                    # Tunings
                    rpoltune = data[0].split(':')
                    this_rfcb = int(rpoltune[0])
                    this_pol = rpoltune[1][0]
                    this_tuning = rpoltune[1][1]
                    self.tunings.setdefault(this_pol, {})
                    self.tunings[this_pol].setdefault(this_tuning, {})
                    self.tunings[this_pol][this_tuning].setdefault(this_rfcb, [])
                    self.tunings[this_pol][this_tuning][this_rfcb].append([_x.strip() for _x in data])
                    self.rfcbs.setdefault(this_rfcb, [])
                    this_part_port = f"{this_rfcb}:{rpoltune[1].strip()}"
                    if this_part_port in check_rfcb_part_port:
                        raise ValueError(f"{this_part_port} already present.")
                    self.rfcbs[this_rfcb].append(data)
                    # RFSoCs
                    tmp = data[2].split(':')
                    if len(tmp) == 2:
                        this_rfsoc = int(tmp[0])
                        self.rfsocs.setdefault(this_rfsoc, [])
                        this_port = int(tmp[1])
                        this_part_port = f"{this_rfsoc}:{this_port}"
                        if this_part_port in check_rfsoc_part_port:
                            raise ValueError(f"{this_part_port} is already present.")
                        self.rfsocs[this_rfsoc].append(data)
                    # SNAPs
                    tmp = data[3].split(':')
                    if len(tmp) == 2:
                        this_snap = int(tmp[0])
                        self.snaps.setdefault(this_snap, [])
                        this_port = tmp[1].lower()
                        this_part_port = f"{this_snap}:{this_port}"
                        if this_part_port in check_snap_part_port:
                            raise ValueError(f"{this_part_port} is already present.")
                        self.snaps[this_snap].append(data)

    def split_apriori(self, tab='Antenna', hdr='A Priori Status', prepend='A'):
        self.apriori = {}
        apind = self.header[tab].index(hdr)
        for ant, entry in self.ants.items():
            aant = prepend + ant
            apval = entry[apind]
            if len(apval):
                self.apriori[aant] = apval

    def split_comments(self, tab='Antenna', comment_col='Comments', prepend='A'):
        self.comments = {}
        comments_start_at = self.header[tab].index(comment_col)
        for ant, entry in self.ants.items():
            aant = prepend + ant
            for i, this_entry in enumerate(entry[comments_start_at:]):
                try:
                    header = self.header[tab][i + comments_start_at]
                except IndexError:
                    header = False
                if len(this_entry):
                    self.comments.setdefault(aant, [])
                    if header == comment_col or not header:
                        entry_to_append = this_entry + ''
                    elif '|' in this_entry:
                        entry_to_append = f"{header}:|{this_entry}"
                    else:
                        entry_to_append = f"{header}:{this_entry}"
                    self.comments[aant].append(entry_to_append)
        if len(self.obs):
            self.comments['OBS'] = list(self.obs)
