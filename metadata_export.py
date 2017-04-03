import os
import re
import json
import arcpy
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from time import strftime


date_time_run = strftime("%Y%m%d_%H%M%S")


EMPTY_TEMPLATE_TREE = r'templates/GISI-metadata-empty-machine.xml'


DEFUALT_DISCLAIMER = '''There are no constraints or warranties with regard to the use of this dataset. Users are encouraged to attribute content to: State of Utah, SGID.This product is for informational purposes and may not have been prepared for, or be suitable for legal, engineering, or surveying purposes. Users of this information should review or consult the primary data and information sources to ascertain the usability of the information. AGRC provides these data in good faith and shall in no event be liable for any incorrect results, any lost profits and special, indirect or consequential damages to any party, arising out of or in connection with the use or the inability to use the data hereon or the services provided. AGRC provides these data and services as a convenience to the public. Further more, AGRC reserves the right to change or revise published data and/or these services at any time.'''


DIGFORM_STRING = '''<digform><digtinfo><formname></formname></digtinfo><digtopt><onlinopt><computer><networka><networkr></networkr></networka></computer></onlinopt></digtopt></digform>'''


class Current(object):
    GROUND_CONDITION = 'ground condition'
    PUBLICATION_DATE = 'publication date'


class Progress(object):
    COMPLETE = 'Complete'
    IN_WORK = 'In work'
    PLANNED = 'Planned'


class Update(object):
    CONTINUALLY = 'Continually'
    DAILY = 'Daily'
    WEEKLY = 'Weekly'
    MONTHLY = 'Monthly'
    ANNUALLY = 'Annually'
    AS_NEEDED = 'As needed'
    IRREGULAR = 'Irregular'
    NONE = 'None planned'


class FormName(object):
    DOWNLOADABLE_RESOURCE = 'Downloadable Resource'
    DOWNLOADABLE_SHAPEFILE = 'Downloadable Shapefile'
    DOWNLOADABLE_GDB = 'Downloadable File Geodatabase'
    WEB_MAP_SERVICE = 'Web Map Service (WMS)'
    WEB_FEATURE_SERVICE = 'Web Feature Service (WFS)'
    WEB_COVERAGE_SERVICE = 'Web Coverage Service (WCS)'
    ESRI_REST = 'ESRI REST'
    WEB_MAP_VIEWER = 'Web Map Viewer'


class GisiXml(object):
    '''Store the important elements of the GISI metadata document'''

    def __init__(self, template_tree):
        self.straight_writes = []
        self.output_xml = None
        self.template_tree = template_tree
        # citation
        self.origin = 'Utah Automated Geographic Reference Center (AGRC)'
        self.straight_writes.append('origin')
        self.pubdate = None
        self.straight_writes.append('pubdate')
        self.title = None
        self.straight_writes.append('title')
        self.onlink = 'https://gis.utah.gov/'
        self.straight_writes.append('onlink')
        # descript
        self.abstract = None
        self.straight_writes.append('abstract')
        self.purpose = None
        self.straight_writes.append('purpose')
        # timeperd
        self.caldate = None
        self.straight_writes.append('caldate')
        self.current = None
        self.straight_writes.append('current')
        # status
        self.progress = Progress.COMPLETE
        self.straight_writes.append('progress')
        self.update = Update.AS_NEEDED
        self.straight_writes.append('update')
        # spdom
        self.westbc = None
        self.straight_writes.append('westbc')
        self.eastbc = None
        self.straight_writes.append('eastbc')
        self.northbc = None
        self.straight_writes.append('northbc')
        self.southbc = None
        self.straight_writes.append('southbc')
        # keywords
        self.themekt = None
        self.straight_writes.append('themekt')
        self.keywords = []
        self.placekt = None
        self.straight_writes.append('placekt')
        self.placekeys = ['Utah']
        # accconst
        self.accconst = None
        self.straight_writes.append('accconst')
        # useconst
        self.useconst = DEFUALT_DISCLAIMER
        self.straight_writes.append('useconst')
        # ptcontact, distinfo, metainfo
        self.cntorg = 'Utah AGRC'
        self.straight_writes.append('cntorg')
        self.cntper = None
        self.straight_writes.append('cntper')
        self.addrtype = 'mailing and physical address'
        self.straight_writes.append('addrtype')
        self.address = 'Utah Automated Geographic Reference 1 State Office Building, Room 5130'
        self.straight_writes.append('address')
        self.city = 'Salt Lake City'
        self.straight_writes.append('city')
        self.state = 'UT'
        self.straight_writes.append('state')
        self.postal = '84114'
        self.straight_writes.append('postal')
        self.cntvoice = '801-538-3665'
        self.straight_writes.append('cntvoice')
        # digform
        self.resource_locations = None

    def _add_digform_elements(self):
        stdorder = self.template_tree.getroot().find('distinfo').find('stdorder')
        insert_i = 0
        for resource in self.resource_locations:
            formname, networkr = resource
            digform = ET.fromstring(DIGFORM_STRING)
            digform.find('digtinfo').find('formname').text = formname
            for e in digform.iter('networkr'):
                e.text = networkr
            stdorder.insert(insert_i, digform)
            insert_i += 1

    def _add_keyword_elements(self):
        theme = self.template_tree.getroot().find('idinfo').find('keywords').find('theme')
        for themekey in self.keywords:
            tk = ET.SubElement(theme, 'themekey')
            tk.text = themekey

    def _add_placekey_elements(self):
        place = self.template_tree.getroot().find('idinfo').find('keywords').find('place')
        for placekey in self.placekeys:
            tk = ET.SubElement(place, 'placekey')
            tk.text = placekey

    def write_fields_to_xml(self):
        self._add_keyword_elements()
        self._add_placekey_elements()
        self._add_digform_elements()

        root = self.template_tree.getroot()
        for xml_element_name in self.straight_writes:
            text = getattr(self, xml_element_name)
            if text is not None:
                for e in root.iter(xml_element_name):
                        e.text = text

        # self.template_tree.write(output_xml_path, method='html')
        with open(self.output_xml, 'wb') as pxml:
            for line in self.prettify(self.template_tree.getroot()):
                pxml.write(line.encode("UTF-8"))

    def prettify(self, root_element):
        '''Return a pretty-printed XML string for the Element.
        '''
        rough_string = ET.tostring(root_element, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="    ")


class BaseTranslator(GisiXml):
    '''Translation functions that set fields of GisiXml document'''

    all_translators = []

    def __init__(self,
                 sgid_xml,
                 resources=(
                     (FormName.DOWNLOADABLE_GDB,
                      'empty'),
                     (FormName.DOWNLOADABLE_SHAPEFILE,
                      'empty')
                 ),
                 empty_template_tree=None):
        if empty_template_tree is None:
            empty_template_tree = ET.parse(r'templates/GISI-metadata-empty-machine.xml')

        super(BaseTranslator, self).__init__(empty_template_tree)
        self.sgid_xml = sgid_xml
        self.resource_locations = resources
        self.name = None
        self.output_xml = None
        self.root = None
        self.direct_reads = [
            'abstract',
            'purpose',
            'accconst',
            'useconst',
            'westbc',
            'eastbc',
            'northbc',
            'southbc',
            'caldate',
            'themekt'
        ]
        self.setup()

    def setup(self):
        self.set_name()
        self.output_xml = r'data/outputs/{}.xml'.format(self.name)
        self.root = ET.parse(self.sgid_xml).getroot()

        self.set_direct_reads()
        self.set_citation_elements()
        self.set_time_period()
        self.set_keywords()

    def set_name(self):
        self.name = self.sgid_xml.split('\\')[-1].replace('.xml', '')

    def set_direct_reads(self):
        for element_name in self.direct_reads:
            for e in self.root.iter(element_name):
                setattr(self, element_name, e.text)

    def set_citation_elements(self):
        try:
            for e in self.root.iter('title'):
                self.title = e.text.split('.')[2]
        except IndexError:
            self.title = os.path.basename(self.sgid_xml).split('.')[-2]

        self.pubdate = strftime('%Y%m%d')

    def set_time_period(self):
        if self.caldate is None:
            self.caldate = strftime('%Y')

    def set_keywords(self):
        for e in self.root.iter('themekey'):
            self.keywords.append(e.text)


def export_sgid_metadata(output_directory,
                         workspace=r'Database Connections\Connection to sgid.agrc.utah.gov.sde',
                         feature_classes=None):
    '''Export metadata from feature class in feature_classes
    '''
    import arcpy
    workspace = r'Database Connections\Connection to sgid.agrc.utah.gov.sde'
    for feature in feature_classes:
        print 'exporting {}'.format(feature)
        arcpy.ExportMetadata_conversion(os.path.join(workspace, feature),
                                        'C:\Program Files (x86)\ArcGIS\Desktop10.3\Metadata\Translator\ARCGIS2FGDC.xml',
                                        os.path.join(output_directory, feature + '.xml'))


def get_features_in_workspace(workspace=r'Database Connections\Connection to sgid.agrc.utah.gov.sde'):
    arcpy.env.workspace = workspace
    fcs = arcpy.ListFeatureClasses()
    save_json(r'data/outputs/temp/fcs.json', {'featureclasses': fcs})


def get_features_in_category(categories, json_featureclasses=r'data/outputs/temp/fcs.json'):
    fcs = load_json(json_featureclasses)['featureclasses']
    featurenames = []
    for fc in fcs:
        if fc.split('.')[1] in categories:
            featurenames.append(fc)
    return featurenames


def load_json(json_path):
    with open(json_path, 'r') as json_file:
        properties = json.load(json_file)

    return properties


def save_json(json_path, properties):
    with open(json_path, 'w') as f_out:
        properties['upload_time_local'] = strftime("%Y_%m_%d %H:%M:%S")
        f_out.write(json.dumps(properties, sort_keys=True, indent=4))


def create_gisi_metadata(metadata_xml_paths):
    # # Setup translators and write out new xml
    translators = []
    output_xml_files = []
    for xml in metadata_xml_paths:
        translators.append(BaseTranslator(xml))
    for translator in translators:
        translator.write_fields_to_xml()
        output_xml_files.append(translator.output_xml)
    save_json('data/outputs/temp/lastgisi_output.json', {'output_files': output_xml_files})


if __name__ == '__main__':
    # featurenames = get_features_in_category(
    #     [
    #         'HEALTH',
    #         'GEOSCIENCE',
    #         'HISTORY',
    #         'FARMING',
    #         'PLANNING'
    #     ]
    # )
    # featurenames.remove('SGID10.HEALTH.HealthCareFacilities')
    featurenames = ['SGID10.GEOSCIENCE.Units_MoabSanRafael']
    #export_sgid_metadata('data', feature_classes=featurenames)
    create_gisi_metadata([os.path.join('data', f + '.xml') for f in featurenames])
