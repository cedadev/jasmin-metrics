from lxml import etree
import requests, xmltodict


class TapeMetrics:
    def __init__(self):
        self.gws_dict = self.get_gws_dict()

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
