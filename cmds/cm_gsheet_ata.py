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


    def load_sheet(self, node_csv='none', tabs=None, path='.', time_tag='%y%m%d'):
        """
        Get the googlesheet information from the internet (or locally for testing etc).

        Parameters
        ----------
        node_csv : str
            node csv file status:  one of 'read', 'write', 'none' (only need first letter)
            'read' uses a local version as opposed to internet version
            'write' writes a local version
            'none' does neither of the above
        tabs : none, str, list
            List of tabs to use.  None == all of them.
        path : str
            Path to use if reading/writing csv files.
        time_tag : str
            If non-zero length string use as time_tag format for the output files (if node_csv=w).
        """
        node_csv = node_csv[0].lower()
        if tabs is None or str(tabs) == 'all':
            tabs = self.tabs
        elif isinstance(tabs, str):
            tabs = tabs.split(',')
        if node_csv != 'n' and isinstance(time_tag, str) and len(time_tag):
            from datetime import datetime
            ttag = datetime.strftime(datetime.now(), time_tag)
        else:
            ttag = ""
        for tab in tabs:
            ofnc = os.path.join(path, f"{tab}_{ttag}.csv")
            if node_csv == 'r':
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
            if node_csv == 'w':
                with open(ofnc, 'w') as fp:
                    fp.write('\n'.join(csv_data))
            for data in csv_tab:
                if data[0].startswith('$'):
                    self.header[tab] = [_x.strip('$') for _x in data]
                    continue
                elif data[0].startswith('#'):
                    continue
                if tab == 'Antenna':
                    this_ant = data[0].upper()
                    if this_ant in self.ants:
                        raise ValueError(f"{this_ant} is already present.")
                    self.ants[this_ant] = [_x.strip() for _x in data]
                elif tab == 'Tuning':
                    # Tunings
                    tmp = data[0].split(':')
                    this_rfcb = int(tmp[0])
                    this_pol = tmp[1][0]
                    this_tuning = tmp[1][1]
                    self.tunings.setdefault(this_pol, {})
                    self.tunings[this_pol].setdefault(this_tuning, {})
                    self.tunings[this_pol][this_tuning].setdefault(this_rfcb, [])
                    self.tunings[this_pol][this_tuning][this_rfcb].append([_x.strip() for _x in data])
                    # RFSoCs
                    tmp = data[2].split(':')
                    if len(tmp) == 2:
                        this_rfsoc = tmp[0].upper()
                        self.rfsocs.setdefault(this_rfsoc, {})
                        this_port = int(tmp[1])
                        if this_port in self.rfsocs[this_rfsoc]:
                            raise ValueError(f"{this_rfsoc} port {this_port} is already present.")
                        self.rfsocs[this_rfsoc][this_port] = [_x.strip() for _x in data]


