'''Translate metadata from arcgis to GISI
'''
import os
import re
import json
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from time import strftime
import drive_loader
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials


date_time_run = strftime("%Y%m%d_%H%M%S")


EMPTY_TEMPLATE_TREE = r'templates/GISI-metadata-empty-machine.xml'
LAST_GISI_OUTPUT = 'data/outputs/temp/lastgisi_output.json'


ABSTRACTS_DRIVE_FOLDER = '0B3wvsjTJuTRQbV9hd1lXSGpTWUE'
CATEGORIES_FOLDER = '0B3wvsjTJuTRQVHdZaWdNZ19rdUk'
ALL_FOLDER_ID = '0B3wvsjTJuTRQa1NaV2hLTnZkQm8'


SRC_FILE_NAME_PROPERTY = 'metaSrcName'
GISI_UPDATED_PROPERTY = 'metaGisiUpdated'


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

    def __init__(self, empty_template_tree=None):
        if empty_template_tree is None:
            empty_template_tree = ET.parse(r'templates/GISI-metadata-empty-machine.xml')

        super(BaseTranslator, self).__init__(empty_template_tree)
        self.sgid_xml = None
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

    def setup(self):
        self.set_name()
        self.output_xml = r'data/outputs/{}.xml'.format(self.name)
        self.root = ET.parse(self.sgid_xml).getroot()

        self.set_direct_reads()
        self.set_citation_elements()
        self.set_time_period()
        self.set_keywords()

    def set_name(self):
        self.name = self.sgid_xml.split('/')[-1].strip('.xml')

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


class RoadsTranslator(GisiXml):
    '''Translation functions that set fields of GisiXml document'''
    def __init__(self, empty_template_tree):
        super(RoadsTranslator, self).__init__(empty_template_tree)
        self.sgid_xml = r'data/SGID10.TRANSPORTATION.Roads.xml'
        self.name = 'SGID10.TRANSPORTATION.Roads'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/Roads/_Statewide/Roads_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/Roads/_Statewide/Roads_shp.zip')
        )
        self.output_xml = r'data/outputs/SGID10.TRANSPORTATION.Roads.xml'
        self.root = ET.parse(self.sgid_xml).getroot()

        self.direct_reads = [
            'purpose',
            'accconst',
            'useconst',
            'westbc',
            'eastbc',
            'northbc',
            'southbc',
        ]
        self.set_direct_reads()
        # self.set_resource_locations(resource_locations)
        self.set_citation_elements()
        self.set_descript_elements()
        self.set_keywords()
        self.set_time_period()

    def set_direct_reads(self):
        for element_name in self.direct_reads:
            for e in self.root.iter(element_name):
                setattr(self, element_name, e.text)

    # def set_resource_locations(self, resource_locations):
    #     self.resource_locations = resource_locations

    def set_citation_elements(self):
        for e in self.root.iter('title'):
            self.title = e.text.split('.')[2]

        self.pubdate = strftime('%Y%m%d')

    def set_descript_elements(self):
        for e in self.root.iter('abstract'):
            self.abstract = e.text[56:]  # Remove some junk at the beginning

    def set_keywords(self):
        for e in self.root.iter('themekey'):
            self.keywords.append(e.text)

    def set_time_period(self):
        self.caldate = strftime('%Y')


class CountiesTranslator(GisiXml):
    '''Translation functions that set fields of GisiXml document'''
    def __init__(self, empty_template_tree):
        super(CountiesTranslator, self).__init__(empty_template_tree)
        self.sgid_xml = r'data/SGID10.BOUNDARIES.Counties.xml'
        self.name = 'SGID10.BOUNDARIES.Counties'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/BOUNDARIES/UnpackagedData/Counties/_Statewide/Counties_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/BOUNDARIES/UnpackagedData/Counties/_Statewide/Counties_shp.zip')
        )
        self.output_xml = r'data/outputs/SGID10.BOUNDARIES.Counties.xml'
        self.root = ET.parse(self.sgid_xml).getroot()

        self.direct_reads = [
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
        self.set_direct_reads()
        self.set_citation_elements()
        self.set_descript_elements()
        self.set_keywords()

    def set_direct_reads(self):
        for element_name in self.direct_reads:
            for e in self.root.iter(element_name):
                setattr(self, element_name, e.text)

    def set_citation_elements(self):
        for e in self.root.iter('title'):
            self.title = e.text.split('.')[2]

        self.pubdate = strftime('%Y%m%d')

    def set_descript_elements(self):
        for e in self.root.iter('abstract'):
            self.abstract = e.text

    def set_keywords(self):
        for e in self.root.iter('themekey'):
            self.keywords.append(e.text)


class MunicipalitiesTranslator(GisiXml):
    '''Translation functions that set fields of GisiXml document'''
    def __init__(self, empty_template_tree):
        super(MunicipalitiesTranslator, self).__init__(empty_template_tree)
        self.sgid_xml = r'data/SGID10.BOUNDARIES.Municipalities.xml'
        self.name = 'SGID10.BOUNDARIES.Municipalities'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/BOUNDARIES/UnpackagedData/Municipalities/_Statewide/Municipalities_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/BOUNDARIES/UnpackagedData/Municipalities/_Statewide/Municipalities_shp.zip')
        )
        self.output_xml = r'data/outputs/SGID10.BOUNDARIES.Municipalities.xml'
        self.root = ET.parse(self.sgid_xml).getroot()

        self.direct_reads = [
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
        self.set_direct_reads()
        self.set_citation_elements()
        self.set_descript_elements()
        self.set_keywords()

    def set_direct_reads(self):
        for element_name in self.direct_reads:
            for e in self.root.iter(element_name):
                setattr(self, element_name, e.text)

    def set_citation_elements(self):
        for e in self.root.iter('title'):
            self.title = e.text.split('.')[2]

        self.pubdate = strftime('%Y%m%d')

    def set_descript_elements(self):
        for e in self.root.iter('abstract'):
            self.abstract = e.text

    def set_keywords(self):
        for e in self.root.iter('themekey'):
            self.keywords.append(e.text)


class AddressPointsTranslator(GisiXml):
    '''Translation functions that set fields of GisiXml document'''
    def __init__(self, empty_template_tree=EMPTY_TEMPLATE_TREE):
        super(AddressPointsTranslator, self).__init__(empty_template_tree)
        self.sgid_xml = r'data/SGID10.LOCATION.AddressPoints.xml'
        self.name = 'SGID10.LOCATION.AddressPoints'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/LOCATION/UnpackagedData/AddressPoints/_Statewide/AddressPoints_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/LOCATION/UnpackagedData/AddressPoints/_Statewide/AddressPoints_shp.zip'),
            (FormName.WEB_MAP_SERVICE,
             'http://utah.maps.arcgis.com/home/item.html?id=5b92021338f64f5ba77765d6fc47cbc9')
        )
        self.output_xml = r'data/outputs/SGID10.LOCATION.AddressPoints.xml'
        self.root = ET.parse(self.sgid_xml).getroot()

        self.direct_reads = [
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
        self.set_direct_reads()
        self.set_citation_elements()
        self.set_descript_elements()
        self.set_keywords()

    def set_direct_reads(self):
        for element_name in self.direct_reads:
            for e in self.root.iter(element_name):
                setattr(self, element_name, e.text)

    def set_citation_elements(self):
        for e in self.root.iter('title'):
            self.title = e.text.split('.')[2]

        self.pubdate = strftime('%Y%m%d')

    def set_descript_elements(self):
        for e in self.root.iter('abstract'):
            self.abstract = e.text

    def set_keywords(self):
        for e in self.root.iter('themekey'):
            self.keywords.append(e.text)


class PlssTranslator(GisiXml):
    '''Translation functions that set fields of GisiXml document'''

    all_translators = []

    def __init__(self, empty_template_tree=EMPTY_TEMPLATE_TREE):
        super(PlssTranslator, self).__init__(empty_template_tree)
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

    def setup(self):
        self.output_xml = r'data/outputs/{}.xml'.format(self.name)
        self.root = ET.parse(self.sgid_xml).getroot()

        self.set_direct_reads()
        self.set_citation_elements()
        self.set_time_period()
        self.set_keywords()

    def set_direct_reads(self):
        for element_name in self.direct_reads:
            for e in self.root.iter(element_name):
                setattr(self, element_name, e.text)

    def set_citation_elements(self):
        for e in self.root.iter('title'):
            self.title = e.text.split('.')[2]

        self.pubdate = strftime('%Y%m%d')

    def set_time_period(self):
        self.caldate = strftime('%Y')

    def set_keywords(self):
        for e in self.root.iter('themekey'):
            self.keywords.append(e.text)


class PlssPointsTranslator(PlssTranslator):

    def __init__(self):
        super(PlssPointsTranslator, self).__init__()

        PlssTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.CADASTRE.PLSSPoint_GCDB.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSPoint_GCDB/_Statewide/PLSSPoint_GCDB_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSPoint_GCDB/_Statewide/PLSSPoint_GCDB_shp.zip')
        )
        self.name = 'SGID10.CADASTRE.PLSSPoint_GCDB'
        self.setup()


class PlssQuarterQuarterSectionsTranslator(PlssTranslator):

    def __init__(self):
        super(PlssQuarterQuarterSectionsTranslator, self).__init__()

        PlssTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.CADASTRE.PLSSQuarterQuarterSections_GCDB.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSQuarterQuarterSections_GCDB/_Statewide/PLSSQuarterQuarterSections_GCDB_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSQuarterQuarterSections_GCDB/_Statewide/PLSSQuarterQuarterSections_GCDB_shp.zip')
        )
        self.name = 'SGID10.CADASTRE.PLSSQuarterQuarterSections_GCDB'
        self.setup()


class PlssQuarterSectionsTranslator(PlssTranslator):

    def __init__(self):
        super(PlssQuarterSectionsTranslator, self).__init__()

        PlssTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.CADASTRE.PLSSQuarterSections_GCDB.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSQuarterSections_GCDB/_Statewide/PLSSQuarterSections_GCDB_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSQuarterSections_GCDB/_Statewide/PLSSQuarterSections_GCDB_shp.zip')
        )
        self.name = 'SGID10.CADASTRE.PLSSQuarterSections_GCDB'

        self.direct_reads.append('title')

        self.setup()

    def set_citation_elements(self):
        self.pubdate = strftime('%Y%m%d')


class PlssSectionsTranslator(PlssTranslator):

    def __init__(self):
        super(PlssSectionsTranslator, self).__init__()

        PlssTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.CADASTRE.PLSSSections_GCDB.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSSections_GCDB/_Statewide/PLSSSections_GCDB_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSSections_GCDB/_Statewide/PLSSSections_GCDB_shp.zip')
        )
        self.name = 'SGID10.CADASTRE.PLSSSections_GCDB'
        self.setup()


class PlssTownshipsTranslator(PlssTranslator):

    def __init__(self):
        super(PlssTownshipsTranslator, self).__init__()

        PlssTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.CADASTRE.PLSSTownships_GCDB.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSTownships_GCDB/_Statewide/PLSSTownships_GCDB_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/PLSSTownships_GCDB/_Statewide/PLSSTownships_GCDB_shp.zip')
        )
        self.name = 'SGID10.CADASTRE.PLSSTownships_GCDB'
        self.setup()


class SchoolDistrictsTranslator(BaseTranslator):

    def __init__(self):
        super(SchoolDistrictsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.BOUNDARIES.SchoolDistricts.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/BOUNDARIES/UnpackagedData/SchoolDistricts/_Statewide/SchoolDistricts_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/BOUNDARIES/UnpackagedData/SchoolDistricts/_Statewide/SchoolDistricts_shp.zip')
        )
        self.setup()


class ZipCodesTranslator(BaseTranslator):

    def __init__(self):
        super(ZipCodesTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.BOUNDARIES.ZipCodes.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/BOUNDARIES/UnpackagedData/ZipCodes/_Statewide/ZipCodes_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/BOUNDARIES/UnpackagedData/ZipCodes/_Statewide/ZipCodes_shp.zip')
        )
        self.setup()


class LandOwnershipTranslator(BaseTranslator):

    def __init__(self):
        super(LandOwnershipTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.CADASTRE.LandOwnership.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/LandOwnership/_Statewide/LandOwnership_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/LandOwnership/_Statewide/LandOwnership_shp.zip')
        )
        self.setup()


class TurnBaselineTranslator(BaseTranslator):

    def __init__(self):
        super(TurnBaselineTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.CADASTRE.TURN_GPS_BaseLines.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/TURN_GPS_BaseLines/_Statewide/TURN_GPS_BaseLines_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/TURN_GPS_BaseLines/_Statewide/TURN_GPS_BaseLines_shp.zip')
        )
        self.setup()


class TurnStationsTranslator(BaseTranslator):

    def __init__(self):
        super(TurnStationsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.CADASTRE.TURN_GPS_Stations.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/TURN_GPS_Stations/_Statewide/TURN_GPS_Stations_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/CADASTRE/UnpackagedData/TURN_GPS_Stations/_Statewide/TURN_GPS_Stations_shp.zip')
        )
        self.setup()


class SkiAreaBoundariesTranslator(BaseTranslator):

    def __init__(self):
        super(SkiAreaBoundariesTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.RECREATION.SkiAreaBoundaries.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/SkiAreaBoundaries/_Statewide/SkiAreaBoundaries_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/SkiAreaBoundaries/_Statewide/SkiAreaBoundaries_shp.zip')
        )
        self.setup()


class SkiLiftsTranslator(BaseTranslator):

    def __init__(self):
        super(SkiLiftsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.RECREATION.SkiLifts.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/SkiLifts/_Statewide/SkiLifts_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/SkiLifts/_Statewide/SkiLifts_shp.zip')
        )
        self.setup()


class SkiAreaLocationsTranslator(BaseTranslator):

    def __init__(self):
        super(SkiAreaLocationsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.RECREATION.SkiAreaLocations.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/SkiAreaLocations/_Statewide/SkiAreaLocations_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/SkiAreaLocations/_Statewide/SkiAreaLocations_shp.zip')
        )
        self.setup()


class SkiTrailsXCTranslator(BaseTranslator):

    def __init__(self):
        super(SkiTrailsXCTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.RECREATION.SkiTrails_XC.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/SkiTrails_XC/_Statewide/SkiTrails_XC_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/SkiTrails_XC/_Statewide/SkiTrails_XC_shp.zip')
        )
        self.setup()


class TrailsTranslator(BaseTranslator):

    def __init__(self):
        super(TrailsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.RECREATION.Trails.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/Trails/_Statewide/Trails_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/Trails/_Statewide/Trails_shp.zip'),
            (FormName.DOWNLOADABLE_RESOURCE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/Trails/_Statewide/Trails.json')
        )
        self.setup()


class TrailHeadsTranslator(BaseTranslator):

    def __init__(self):
        super(TrailHeadsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.RECREATION.TrailHeads.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/Trailheads/_Statewide/Trailheads_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/RECREATION/UnpackagedData/Trailheads/_Statewide/Trailheads_shp.zip')
        )
        self.setup()


class SchoolsTranslator(BaseTranslator):

    def __init__(self):
        super(SchoolsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.SOCIETY.Schools.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/SOCIETY/UnpackagedData/Schools/_Statewide/Schools_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/SOCIETY/UnpackagedData/Schools/_Statewide/Schools_shp.zip')
        )
        self.setup()


class HealthCareTranslator(BaseTranslator):

    def __init__(self):
        super(HealthCareTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.HEALTH.HealthCareFacilities.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/HEALTH/UnpackagedData/HealthCareFacilities/_Statewide/HealthCareFacilities_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/HEALTH/UnpackagedData/HealthCareFacilities/_Statewide/HealthCareFacilities_shp.zip')
        )
        self.setup()


class UsCongressTranslator(BaseTranslator):

    def __init__(self):
        super(UsCongressTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.POLITICAL.USCongressDistricts2012.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/POLITICAL/UnpackagedData/USCongressDistricts2012/_Statewide/USCongressDistricts2012_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/POLITICAL/UnpackagedData/USCongressDistricts2012/_Statewide/USCongressDistricts2012_shp.zip')
        )
        self.setup()


class UtahHouseTranslator(BaseTranslator):

    def __init__(self):
        super(UtahHouseTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.POLITICAL.UtahHouseDistricts2012.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/POLITICAL/UnpackagedData/UtahHouseDistricts2012/_Statewide/UtahHouseDistricts2012_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/POLITICAL/UnpackagedData/UtahHouseDistricts2012/_Statewide/UtahHouseDistricts2012_shp.zip')
        )
        self.setup()


class UtahSenateTranslator(BaseTranslator):

    def __init__(self):
        super(UtahSenateTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.POLITICAL.UtahSenateDistricts2012.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/POLITICAL/UnpackagedData/UtahSenateDistricts2012/_Statewide/UtahSenateDistricts2012_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/POLITICAL/UnpackagedData/UtahSenateDistricts2012/_Statewide/UtahSenateDistricts2012_shp.zip')
        )
        self.setup()


class LiquorTranslator(BaseTranslator):

    def __init__(self):
        super(LiquorTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.SOCIETY.LiquorStores.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/POLITICAL/UnpackagedData/UtahSenateDistricts2012/_Statewide/UtahSenateDistricts2012_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/POLITICAL/UnpackagedData/UtahSenateDistricts2012/_Statewide/UtahSenateDistricts2012_shp.zip')
        )
        self.setup()

    # def set_citation_elements(self):
    #
    #     self.title = 'LiquorStores'
    #
    #     self.pubdate = strftime('%Y%m%d')


class BusRoutesTranslator(BaseTranslator):

    def __init__(self):
        super(BusRoutesTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.TRANSPORTATION.BusRoutes_UTA.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/BusRoutes_UTA/_Statewide/BusRoutes_UTA_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/BusRoutes_UTA/_Statewide/BusRoutes_UTA_shp.zip')
        )
        self.setup()


class BusStopsTranslator(BaseTranslator):

    def __init__(self):
        super(BusStopsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.TRANSPORTATION.BusStops_UTA.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/BusStops_UTA/_Statewide/BusStops_UTA_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/BusStops_UTA/_Statewide/BusStops_UTA_shp.zip')
        )
        self.setup()


class CommuterRailRoutesTranslator(BaseTranslator):

    def __init__(self):
        super(CommuterRailRoutesTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.TRANSPORTATION.CommuterRailRoutes_UTA.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/CommuterRailRoutes_UTA/_Statewide/CommuterRailRoutes_UTA_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/CommuterRailRoutes_UTA/_Statewide/CommuterRailRoutes_UTA_shp.zip')
        )
        self.setup()


class CommuterRailStationsTranslator(BaseTranslator):

    def __init__(self):
        super(CommuterRailStationsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.TRANSPORTATION.CommuterRailStations_UTA.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/CommuterRailStations_UTA/_Statewide/CommuterRailStations_UTA_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/TRANSPORTATION/UnpackagedData/CommuterRailStations_UTA/_Statewide/CommuterRailStations_UTA_shp.zip')
        )
        self.setup()


class LakesTranslator(BaseTranslator):

    def __init__(self):
        super(LakesTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.WATER.Lakes.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/WATER/UnpackagedData/Lakes/_Statewide/Lakes_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/WATER/UnpackagedData/Lakes/_Statewide/Lakes_shp.zip')
        )
        self.setup()


class StreamsTranslator(BaseTranslator):

    def __init__(self):
        super(StreamsTranslator, self).__init__()

        BaseTranslator.all_translators.append(self)

        self.sgid_xml = r'data/SGID10.WATER.Streams.xml'
        self.resource_locations = (
            (FormName.DOWNLOADABLE_GDB,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/WATER/UnpackagedData/Streams/_Statewide/Streams_gdb.zip'),
            (FormName.DOWNLOADABLE_SHAPEFILE,
             'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/WATER/UnpackagedData/Streams/_Statewide/Streams_shp.zip')
        )
        self.setup()


def export_sgid_metadata(output_directory,
                         workspace=r'Database Connections\Connection to sgid.agrc.utah.gov.sde',
                         feature_classes=None):
    '''Export metadata from feature class in feature_classes
    '''
    import arcpy
    workspace = r'Database Connections\Connection to sgid.agrc.utah.gov.sde'
    feature_classes = [
        'SGID10.HEALTH.HealthCareFacilities',
        'SGID10.SOCIETY.LiquorStores',
        'SGID10.POLITICAL.USCongressDistricts2012',
        'SGID10.POLITICAL.UtahHouseDistricts2012',
        'SGID10.POLITICAL.UtahSenateDistricts2012',
        'SGID10.TRANSPORTATION.BusStops_UTA',
        'SGID10.TRANSPORTATION.BusRoutes_UTA',
        'SGID10.TRANSPORTATION.CommuterRailRoutes_UTA',
        'SGID10.TRANSPORTATION.CommuterRailStations_UTA',
        'SGID10.WATER.Streams',
        'SGID10.WATER.Lakes']
    for feature in feature_classes:
        print 'exporting {}'.format(feature)
        arcpy.ExportMetadata_conversion(os.path.join(workspace, feature),
                                        'C:\Program Files (x86)\ArcGIS\Desktop10.3\Metadata\Translator\ARCGIS2FGDC.xml',
                                        os.path.join(output_directory, feature + '.xml'))


def load_text_to_drive2(abstract_text, name, parent_id, suffix='_abstract'):
    temp_txt = 'data/outputs/temp/temp_to_drive.txt'
    if abstract_text is None:
        abstract_text = ' '
    with open(temp_txt, 'w') as txt:
        txt.write(abstract_text.encode("UTF-8"))

    doc_id = drive_loader.create_google_doc(temp_txt, parent_id, name + suffix)
    return doc_id


def load_text_to_drive(abstract_text, name, parent_id, suffix='_abstract'):
    import StringIO
    if abstract_text is None:
        abstract_text = ' '
    txt = StringIO.StringIO()
    txt.write(abstract_text.encode('UTF-8'))
    doc_id = drive_loader.create_google_doc(txt, parent_id, name + suffix)
    txt.close()
    return doc_id


def get_completed_comment(comments):
    completed_matcher = re.compile(r'.*(#completed|#done|#complete|#finished|#lgtm)')
    for comment in comments:
        comment_string = comment['content']
        if completed_matcher.match(comment_string.lower()):
            return comment['id']
    return None


def mark_completed(file_id, comment_id):
    reply_id = drive_loader.comment_reply(file_id, comment_id, '#updated')
    drive_loader.set_property(file_id, {'metaGisiUpdated': 'true'})

    return reply_id


def mark_updated(file_id, update_time):
    drive_loader.set_property(file_id, {'metaGisiUpdated': update_time})


def update_xml_element(xml_path, element_text, element_name):
    element_tree = ET.parse(xml_path)
    root = element_tree.getroot()
    for e in root.iter(element_name):
        e.text = element_text
    element_tree.write(xml_path, encoding='UTF-8')


def assign_to_folder():
    files = {
        'SGID10.CADASTRE.PLSSPoint_GCDB': '1S_UuAR541scal2ksJBnaXo5GK4vgKMKEAfYw6KMrDxY',
        'SGID10.CADASTRE.PLSSQuarterQuarterSections_GCDB': '1rDq_NbHHJTTyVX54u6y9_hhQFDRgsPCCAigT5H_9O4k',
        'SGID10.CADASTRE.PLSSQuarterSections_GCDB': '17u5IM91btLvUsY9hWVZeFHiYthoWAahLOrqMyh3ULEs',
        'SGID10.CADASTRE.PLSSSections_GCDB': '1jIqdlNPgwcrwOL7TxreufN0Qy3IJwUbC09wVPieEHsE',
        'SGID10.CADASTRE.PLSSTownships_GCDB': '1-9NdvukaVq08tuiYGGePhaY0vCeIJbTG-KtyT9ESCAA'
    }
    folders = {
        'mike': '0B3wvsjTJuTRQWjduVXZHTnhNd0U',
        'sean': '0B3wvsjTJuTRQN2E5NEFDYldnR00',
        'plss': '0B3wvsjTJuTRQSlVlZEhFSURSU28'
        }
    s = ','.join(folders.values())
    print s
    for f in files.values():
        drive_loader.add_file_to_folders(f, s)

    drive_loader.add_file_to_folders('13wvPGwihQ7KHDoMn0vY_aMsLwc5X0VMRENFgDj5fl1s', [])
    print '-----------'
    comments = drive_loader.get_file_comments('1YaX-G6f0_9MEleyn_D7FnFfYVGhFpF9y92bpLPec2MY')
    # is_complete(comments)


def list_updated_files(date, parent_folder=ALL_FOLDER_ID):
    files = drive_loader.get_files_updated_after_in_directory(date, parent_folder)
    for f in files:
        print 'Updated: ', f['name']


def load_json(json_path):
    with open(json_path, 'r') as json_file:
        properties = json.load(json_file)

    return properties


def save_json(json_path, properties):
    with open(json_path, 'w') as f_out:
        properties['upload_time_local'] = strftime("%Y_%m_%d %H:%M:%S")
        f_out.write(json.dumps(properties, sort_keys=True, indent=4))


def check_files_and_update(past_update_time):
    update_time = datetime.utcnow().isoformat()
    files = drive_loader.get_files_updated_after_in_directory(past_update_time, ALL_FOLDER_ID)
    xml_paths = []
    for f in files:
        print 'Updating: ', f['name']
        file_id = f['id']
        element_name = f['name'].split('_')[-1]
        new_text = drive_loader.get_doc_as_string(file_id)
        xml_name = drive_loader.get_property(file_id, SRC_FILE_NAME_PROPERTY)
        print xml_name
        xml_path = os.path.join('data', 'outputs', xml_name)
        xml_paths.append(xml_path)
        update_xml_element(xml_path, new_text.replace('&', 'and'), element_name)
        mark_updated(file_id, update_time)

    save_json('update_config.json', {'last_update': update_time})
    return xml_paths


def load_elements_to_drive(xml_files, elements):
    category_folders = {}
    for xml_file in xml_files:
        file_name = os.path.basename(xml_file)
        drive_name = file_name.split('.')[-2]
        category_name = file_name.split('.')[1]

        root = ET.parse(xml_file).getroot()
        element_text = None

        for element in elements:
            for e in root.iter(element):
                element_text = e.text
            if category_name not in category_folders:
                category_folders[category_name] = drive_loader.create_drive_folder(category_name,
                                                                                   [CATEGORIES_FOLDER])
            layer_folder = drive_loader.create_drive_folder(drive_name, [category_folders[category_name]])
            doc_id = load_text_to_drive(element_text, drive_name, layer_folder, suffix='_'+element)
            drive_loader.add_file_to_folders(doc_id, [ALL_FOLDER_ID])
            drive_loader.set_property(doc_id, {'metaSrcName': file_name})
            print 'Uploaded {}, ID: {}'.format(drive_name+'_'+element, doc_id)


def get_empty_element_xml(xml_files, elements):
    empties = []
    for xml_file in xml_files:

        root = ET.parse(xml_file).getroot()
        element_text = None

        for element in elements:
            for e in root.iter(element):
                element_text = e.text
                if element_text is None:
                    empties.append(xml_file)
                    break
    save_json('data/outputs/temp/empties.json', {'empties': empties})


def create_gisi_metadata():
    # # Setup translators and write out new xml
    HealthCareTranslator()
    UsCongressTranslator()
    UtahHouseTranslator()
    UtahSenateTranslator()
    LiquorTranslator()
    BusRoutesTranslator()
    BusStopsTranslator()
    CommuterRailRoutesTranslator()
    CommuterRailStationsTranslator()
    LakesTranslator()
    StreamsTranslator()

    translators = BaseTranslator.all_translators
    for translator in translators:
        print translator.name
        # translator.write_fields_to_xml()

    # for t in BaseTranslator.all_translators:
    #     print '{},'.format(os.path.basename(t.sgid_xml))


def update_working_spreadsheet():
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(r'CenterlineSchema-c1b9c8e23e52.json', scope)
    gc = gspread.authorize(credentials)
    # spreadsheet must be shared with the email in credentials
    spreadSheet = gc.open_by_url(r"https://docs.google.com/spreadsheets/d/1c-kwYFCID80XpRdQKbP96j0xxy5E9WVgHpyNR6GtO2Q/edit#gid=0")
    # worksheets = spreadSheet.worksheets()
    fieldWorkSheet = spreadSheet.worksheet('Sheet1')
    print type(fieldWorkSheet.get_all_values()[0])


def get_feature_class_folders():
    files = drive_loader.get_abstracts_in_directory(ALL_FOLDER_ID)
    parent_ids = {}
    for f in files:
        f['parents'].remove(ALL_FOLDER_ID)
        if len(f['parents']) > 2:
            print f['name'], len(f['parents'])
        parent_id = f['parents'][0]
        if parent_id in parent_ids:
            print f['name'], f['parents']
        parent_ids[parent_id] = drive_loader.get_feature_folder_info(parent_id)

    save_json('data/outputs/lists/feature_folders.json', parent_ids)


def add_full_name(fc_folder_json):
    folders = load_json(fc_folder_json)
    folders.pop('upload_time_local')
    for fc_id in folders:
        fc_info = folders[fc_id]
        parent_id = fc_info['parents'][0]
        parent_name = drive_loader.get_feature_folder_info(parent_id)['name']
        fc_info['full_name'] = '{}.{}'.format(parent_name, fc_info['name'])
    save_json('data/outputs/lists/feature_folders_name.json', folders)


def create_assign_csv(fc_folder_names_json):
    folders = load_json(fc_folder_names_json)
    folders.pop('upload_time_local')
    csv_rows = []
    for fc_id in folders:
        fc_info = folders[fc_id]
        csv_rows.append((fc_info['full_name'], fc_info['webViewLink'], fc_id))

    with open('data/outputs/temp/sheet.csv', 'wb') as assign:
        csv_assign = csv.writer(assign)
        csv_assign.writerows(csv_rows)


def count_xml_files(directory):
    count = 0
    for root, dirs, files in os.walk(directory, topdown=True):
        for directory in dirs:
            print directory
            for name in files:
                extless = name.replace('.xml', '')
                if extless.endswith('l') or extless.endswith('m') or extless.endswith('x'):
                    print name
                    count += 1

    print count

if __name__ == '__main__':
    # # Setup translators and write out new xml
    # create_gisi_metadata()

    # Check for updates
    # past_update_time = load_json('update_config.json')['last_update']
    # check_files_and_update(past_update_time)
    # list_updated_files(past_update_time)

    # Load elemets as google docs
    # xml_files = ['data/outputs/SGID10.CADASTRE.Parcels.xml']#load_json(LAST_GISI_OUTPUT)['output_files']
    # load_elements_to_drive(xml_files, ['purpose', 'abstract'])

    # get_feature_class_folders()
    # add_full_name('data/outputs/lists/feature_folders.json')
    count_xml_files('data')
    # create_assign_csv('data/outputs/lists/feature_folders_name.json')
    # xml_files = []
    # for root, dirs, files in os.walk('data/outputs', topdown=True):
    #     for name in files:
    #         if name.endswith('.xml'):
    #             xml_files.append(os.path.join(root, name))
    # get_empty_element_xml(xml_files, ['abstract'])

    # with open(r'data\outputs\temp\sheet.txt', 'w') as asstxt:
    #     for root, dirs, files in os.walk('data\outputs', topdown=True):
    #         for name in files:
    #             if name.endswith('.xml'):
    #                 n = name.split('.')[1] + '.' + name.split('.')[2]
    #                 asstxt.write(n + '\n')
    #                 print n
                # xml_files.append(os.path.join(root, name))
    # update_working_spreadsheet()
    # fcs = []
    # with open(r'data\outputs\temp\sheet.txt', 'r') as asstxt:
    #     fcs = [line for line in asstxt]
    #
    # print fcs
