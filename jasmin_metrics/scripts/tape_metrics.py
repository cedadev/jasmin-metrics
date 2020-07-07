from lxml import etree
import requests, xmltodict
from .html_table_parser import HTMLTableParser


class TapeMetrics:
    def __init__(self):
        self.gws_dict = self.get_gws_dict()
        self.et_clock_watch_url = 'http://et-monitor.fds.rl.ac.uk/et_admin/ClockWatching.php#last_day_throughput'
        self.sd_clock_watch_url = 'http://storaged-monitor.fds.rl.ac.uk/storaged_ceda/ClockWatching.php#last_day_throughput'
        self.et_tape_url = 'http://et-monitor.fds.rl.ac.uk/et_admin/tape_library_status.php'
        self.sd_tape_url = 'http://storaged-monitor.fds.rl.ac.uk/storaged_ceda/tape_library_status.php'

    def get_gws_dict(self):
        # get xml content
        xml_page = requests.get('http://et-monitor.fds.rl.ac.uk/et_admin/ET_WkspaceOverview-XML.php')
        data = xml_page.content
        # fix the xml
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(data, parser)
        # convert xml into a dict
        data_dict = xmltodict.parse(etree.tostring(root))

        return data_dict

    def get_gws_list(self):
        gws_list = []
        for i in self.gws_dict['et_wkspaces']['et_wkspace']:
            gws_list.append(i['workspace_id'][0])

        return gws_list

    def get_tape_gws_quota(self, gws):
        for i in self.gws_dict['et_wkspaces']['et_wkspace']:
            if i['workspace_id'][0] == gws:
                return i['workspace_quota']

    def get_tape_gws_used(self, gws):
        for i in self.gws_dict['et_wkspaces']['et_wkspace']:
            if i['workspace_id'][0] == gws:
                return i['workspace_quota_used']

    def get_tape_gws_file_count(self, gws):
        for i in self.gws_dict['et_wkspaces']['et_wkspace']:
            if i['workspace_id'][0] == gws:
                return i['workspace_file_count']

    def get_et_summary_table(self):
        r = requests.get(self.et_clock_watch_url)
        p = HTMLTableParser()
        p.feed(r.text)
        tab = p.tables[0]
        assert tab[0] == ['Metric', 'Topic', 'Value'], "ET summary table doesn't look right"
        return tab

    def get_tape_et_datain_volume_day(self):
        table = self.get_et_summary_table()
        for line in table:
            if line[0] == 'Throughput: files in last 24 hrs':
                return float(line[2].split(':')[-1])
	
	raise ValueError("ET table results not being selected properly")

    def get_tape_et_datain_count_day(self):
        table = self.get_et_summary_table()
        for line in table:
            if line[0] == 'Throughput: files in last 24 hrs':
                return float(line[1].split(':')[-1])
	
	raise ValueError("ET table results not being selected properly")

    def get_tape_et_data_total_volume(self):
        table = self.get_et_summary_table()
        for line in table:
            if line[0] == 'Transfer state = SYNCED:DEFAULT':
                return float(line[-1][:-3])
	
	raise ValueError("ET table results not being selected properly")

    def get_tape_et_data_total_count(self):
        table = self.get_et_summary_table()
        for line in table:
            if line[0] == 'Transfer state = SYNCED:DEFAULT':
                return float(line[1].split(';')[-1].split(' ')[-2])
	
	raise ValueError("ET table results not being selected properly")

    def get_sd_summary_table(self):
        r = requests.get(self.sd_clock_watch_url)
        p = HTMLTableParser()
        p.feed(r.text)
        tab = p.tables[0]
        assert tab[0] == ['Metric', 'Topic', 'Value'], "ET summary table doesn't look right"
        return tab

    def get_tape_sd_datain_volume_day(self):
        table = self.get_sd_summary_table()
        for line in table:
            if line[0] == 'Throughput: files in last 24 hrs':
                return float(line[2].split(':')[-1])
	
	raise ValueError("SD table results not being selected properly")

    def get_tape_sd_datain_count_day(self):
        table = self.get_sd_summary_table()
        for line in table:
            if line[0] == 'Throughput: files in last 24 hrs':
                return float(line[1].split(':')[-1])
	
	raise ValueError("SD table results not being selected properly")

    def get_tape_sd_data_total_volume(self):
        table = self.get_sd_summary_table()
        for line in table:
            if line[0] == 'Transfer state = SYNCED:DEFAULT':
                return float(line[-1][:-3])
	
	raise ValueError("SD table results not being selected properly")

    def get_tape_sd_data_total_count(self):
        table = self.get_sd_summary_table()
        for line in table:
            if line[0] == 'Transfer state = SYNCED:DEFAULT':
                return float(line[1].split(';')[-1].split(' ')[-2])
	
	raise ValueError("SD table results not being selected properly")

    def get_sd_tape_table(self):
        r = requests.get(self.sd_tape_url)
        p = HTMLTableParser()
        p.feed(r.text)
        tab = p.tables[0]
        assert tab[0] == ['Tape Status', 'Tape Count'], "SD tape table doesn't look right"
        return tab

    def get_tape_in_library(self):
        r = requests.get(self.sd_tape_url)
        text = r.text
        para = text.split('<p>')[1].split('</p>')[0]
        return int(para.split(' ')[5])

    def get_tape_at_culham(self):
        table = self.get_sd_tape_table()
        return table[1][1]

    def get_tape_ready_for_culham(self):
        table = self.get_sd_tape_table()
        return table[2][1]

    def get_tape_in_double_copy(self):
        table = self.get_sd_tape_table()
        return table[4][1]

    def get_et_tape_table(self):
        r = requests.get(self.et_tape_url)
        p = HTMLTableParser()
        p.feed(r.text)
        tab = p.tables[0]
        assert tab[0] == ['Tape Type', 'Tape Count'], "SD tape table doesn't look right"
        return tab

    def get_tape_et_copy_1(self):
        table = self.get_et_tape_table()
        return table[1][1]

    def get_tape_et_copy_2(self):
        table = self.get_et_tape_table()
        return table[2][1]
