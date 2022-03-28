"""Class for config gsheet."""
import csv
import requests
from . import util
from hera_mc import cm_utils
from argparse import Namespace

apriori_enum_header = 'Current apriori enum'
hu_col = {'Ant': 0, 'Pol': 4, 'Feed': 1, 'FEM': 2, 'PAM': 4, 'NBP/PAMloc': 3,
          'SNAP': 5, 'Port': 5, 'SNAPloc': 6, 'Node': 6}
sheet_headers = ['Ant', 'Pol', 'Feed', 'FEM', 'NBP/PAMloc', 'PAM', 'SNAP', 'Port',
                 'SNAPloc', 'APriori', 'History', 'Comments']

gsheet_prefix = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRrwdnbP2yBXDUvUZ0AXQ--Rqpt7jCkiv89cVyDgtWGHPeMXfNWymohaEtXi_-t7di7POGlg8qwhBlt/pub?"  # noqa

gsheet = {}
gsheet['node0'] = "gid=0&single=true&output=csv"
gsheet['node1'] = "gid=6391145&single=true&output=csv"
gsheet['node2'] = "gid=1387042544&single=true&output=csv"
gsheet['node3'] = "gid=1451443110&single=true&output=csv"
gsheet['node4'] = "gid=1237822868&single=true&output=csv"
gsheet['node5'] = "gid=1836116919&single=true&output=csv"
gsheet['node6'] = "gid=1913506139&single=true&output=csv"
gsheet['node7'] = "gid=780596546&single=true&output=csv"
gsheet['node8'] = "gid=1174361876&single=true&output=csv"
gsheet['node9'] = "gid=59309582&single=true&output=csv"
gsheet['node10'] = "gid=298497018&single=true&output=csv"
gsheet['node11'] = "gid=660944848&single=true&output=csv"
gsheet['node12'] = "gid=1465370847&single=true&output=csv"
gsheet['node13'] = "gid=954070149&single=true&output=csv"
gsheet['node14'] = "gid=1888985402&single=true&output=csv"
gsheet['node15'] = "gid=1947163734&single=true&output=csv"
gsheet['node16'] = "gid=1543199929&single=true&output=csv"
# gsheet['node17'] = "gid=523815291&single=true&output=csv"
gsheet['node18'] = "gid=2120802515&single=true&output=csv"
gsheet['node19'] = "gid=488699377&single=true&output=csv"
gsheet['node20'] = "gid=319083641&single=true&output=csv"
gsheet['node21'] = "gid=535819481&single=true&output=csv"
# gsheet['node22'] = "gid=196866991&single=true&output=csv"
# gsheet['node23'] = "gid=78305433&single=true&output=csv"
# gsheet['node24'] = "gid=1348236988&single=true&output=csv"
# gsheet['node25'] = "gid=2134158656&single=true&output=csv"
# gsheet['node26'] = "gid=1376433001&single=true&output=csv"
# gsheet['node27'] = "gid=1902916331&single=true&output=csv"
# gsheet['node28'] = "gid=1379228027&single=true&output=csv"
# gsheet['node29'] = "gid=1386400309&single=true&output=csv"
no_prefix = ['Comments']

gsheet['NodeNotes'] = "gid=906207981&single=true&output=csv"
gsheet['README'] = "gid=1630961076&single=true&output=csv"
gsheet['AprioriWorkflow'] = "gid=2096134790&single=true&output=csv"


class SheetData:
    """Class for googlesheet."""

    def __init__(self):
        """Initialize dictionaries/lists."""
        self.tabs = []
        for key in gsheet.keys():
            gsheet[key] = gsheet_prefix + gsheet[key]
            if key.startswith('node'):
                self.tabs.append(key)
        self.tabs = sorted(self.tabs)
        # It reads into the variables below
        self.data = {}
        self.ant_to_node = {}
        self.node_to_ant = {}
        self.header = {}
        self.date = {}
        self.notes = {}
        self.ants = []

    def load_workflow(self):
        """
        Load relevant data out of AprioriWorkflow tab.

        Currently, this is the apriori enums and emails.
        """
        self.workflow = {}
        self.apriori_enum = []
        self.apriori_email = {}
        try:
            xxx = requests.get(gsheet['AprioriWorkflow'])
        except:  # noqa
            import sys
            e = sys.exc_info()[0]
            print(f"Error reading {gsheet['AprioriWorkflow']}:  {e}")
            return
        csv_tab = b''
        for line in xxx:
            csv_tab += line
        csv_data = csv_tab.decode('utf-8').splitlines()
        capture_enums = False
        for data in csv.reader(csv_data):
            self.workflow[data[0]] = data[1:]
            if data[0] == apriori_enum_header:
                capture_enums = True
                for _i, email_addr in enumerate(data[1:]):
                    if len(email_addr):
                        self.apriori_email[email_addr] = Namespace(eno=_i+1, notify=[])
            elif capture_enums and not len(data[0]):
                capture_enums = False
            if capture_enums:
                if data[0] != apriori_enum_header:
                    self.apriori_enum.append(data[0])
                    for notifyee, nent in self.apriori_email.items():
                        if data[nent.eno].lower() == 'y':
                            nent.notify.append(data[0])

    def load_node_notes(self):
        self.node_notes = []
        try:
            xxx = requests.get(gsheet['NodeNotes'])
        except:  # noqa
            import sys
            e = sys.exc_info()[0]
            print(f"Error reading {gsheet['NodeNotes']}:  {e}")
            return
        csv_tab = b''
        for line in xxx:
            csv_tab += line
        _nodenotes = csv_tab.decode('utf-8').splitlines()
        for nn in csv.reader(_nodenotes):
            self.node_notes.append(nn)

    def load_sheet(self, node_csv='none', tabs=None, check_headers=False,
                   path='', time_tag='_%y%m%d'):
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
        check_headers : bool
            If True, it will make sure all of the headers agree with sheet_headers
        path : str
            Path to use if reading/writing csv files.
        time_tag : str
            If non-zero length string use as time_tag format for the output files (if node_csv=w).
        """
        ant_set = set()
        node_csv = node_csv[0].lower()
        if tabs is None or str(tabs) == 'all':
            tabs = self.tabs
        elif isinstance(tabs, str):
            tabs = tabs.split(',')
        if node_csv == 'w' and isinstance(time_tag, str) and len(time_tag):
            from datetime import datetime
            ttag = datetime.strftime(datetime.now(), time_tag)
        else:
            ttag = ""
        for tab in tabs:
            if node_csv == 'r':
                csv_data = []
                ofnc = f"{path}/{tab}.csv"
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
                ofnc = f"{path}/{tab}{ttag}.csv"
                with open(ofnc, 'w') as fp:
                    fp.write('\n'.join(csv_data))
            self.node_to_ant[tab] = []
            for data in csv_tab:
                if data[0].startswith('Ant'):  # This is the header line
                    self.header[tab] = ['Node'] + data
                    if check_headers:
                        util.compare_lists(sheet_headers, data, info=tab)
                    continue
                elif data[0].startswith('Date:'):  # This is the overall date line
                    self.date[tab] = data[1]
                    break
                try:
                    antnum = int(data[0])
                except ValueError:
                    continue
                hpn = util.gen_hpn('HH', antnum)
                hkey = cm_utils.make_part_key(hpn, 'A')
                ant_set.add(hkey)
                self.ant_to_node[hkey] = tab
                self.node_to_ant[tab].append(hpn)
                dkey = '{}-{}'.format(hkey, data[1].upper())
                self.data[dkey] = [util.get_num(tab)] + data
            # Get the notes below the hookup table.
            node_pn = 'N{:02d}'.format(int(util.get_num(tab)))
            for data in csv_tab:
                if data[0].startswith("Note"):
                    note_part = data[0].split()
                    if len(note_part) > 1:
                        npkey = note_part[1]
                    else:
                        npkey = node_pn
                    self.notes.setdefault(npkey, [])
                    self.notes[npkey].append('-'.join([y for y in data[1:] if len(y) > 0]))
        self.ants = cm_utils.put_keys_in_order(list(ant_set), sort_order='NPR')
