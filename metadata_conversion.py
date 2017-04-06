'''Translate metadata from arcgis to GISI
'''
import os
import re
import json
from ntpath import normpath
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from time import strftime, clock
import drive_loader
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials


date_time_run = strftime("%Y%m%d_%H%M%S")


LAST_GISI_OUTPUT = 'data/outputs/temp/lastgisi_output.json'


ABSTRACTS_DRIVE_FOLDER = '0B3wvsjTJuTRQbV9hd1lXSGpTWUE'
CATEGORIES_FOLDER = '0B3wvsjTJuTRQVHdZaWdNZ19rdUk'
ALL_FOLDER_ID = '0B3wvsjTJuTRQa1NaV2hLTnZkQm8'


SRC_FILE_NAME_PROPERTY = 'metaSrcName'
GISI_UPDATED_PROPERTY = 'metaGisiUpdated'


DEFUALT_DISCLAIMER = '''There are no constraints or warranties with regard to the use of this dataset. Users are encouraged to attribute content to: State of Utah, SGID.This product is for informational purposes and may not have been prepared for, or be suitable for legal, engineering, or surveying purposes. Users of this information should review or consult the primary data and information sources to ascertain the usability of the information. AGRC provides these data in good faith and shall in no event be liable for any incorrect results, any lost profits and special, indirect or consequential damages to any party, arising out of or in connection with the use or the inability to use the data hereon or the services provided. AGRC provides these data and services as a convenience to the public. Further more, AGRC reserves the right to change or revise published data and/or these services at any time.'''


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


def update_xml_element(xml_path, element_text, element_name, only_empty=False):
    element_tree = ET.parse(xml_path)
    root = element_tree.getroot()
    for e in root.iter(element_name):
        if only_empty and not (e is None or e.text.strip() == '' or e.text.strip() == 'None'):
            return
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
    # for f in files:
    #     print 'Updated: ', f['name']
    return files


def load_json(json_path, remove_update=False):
    with open(json_path, 'r') as json_file:
        properties = json.load(json_file)
        if remove_update:
            properties.pop('upload_time_local')

    return properties


def save_json(json_path, properties):
    with open(json_path, 'w') as f_out:
        properties['upload_time_local'] = strftime("%Y_%m_%d %H:%M:%S")
        f_out.write(json.dumps(properties, sort_keys=True, indent=4))


def check_files_and_update(past_update_time, parent_folder=ALL_FOLDER_ID):
    update_time = datetime.utcnow().isoformat()
    files = drive_loader.get_files_updated_after_in_directory(past_update_time, parent_folder)
    xml_paths = []
    for f in files:
        print 'Updating: ', f['name']
        file_id = f['id']
        element_name = f['name'].split('_')[-1]
        new_text = drive_loader.get_doc_as_string(file_id)
        xml_name = drive_loader.get_property(file_id, SRC_FILE_NAME_PROPERTY)
        xml_path = os.path.join('data', 'outputs', xml_name)
        xml_paths.append(xml_path)
        update_xml_element(xml_path, new_text.replace('&', 'and'), element_name)
        mark_updated(file_id, update_time)

    path_set = set(xml_paths)
    for xml in path_set:
        update_xml_element(xml_path, DEFUALT_DISCLAIMER, 'useconst', only_empty=True)

    save_json('update_config.json', {'last_update': datetime.utcnow().isoformat()})
    return list(set(xml_paths))


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


def get_working_spreadsheet_completed_ids():
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(r'CenterlineSchema-c1b9c8e23e52.json', scope)
    gc = gspread.authorize(credentials)
    # spreadsheet must be shared with the email in credentials
    spreadSheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1tX3mlUZ3nWIoTKxjiKlYG9U3njIRnJHF956wzS428qw/edit#gid=1226201523")
    # worksheets = spreadSheet.worksheets()
    fieldWorkSheet = spreadSheet.worksheet('sheet.csv')
    rows = fieldWorkSheet.get_all_values()
    complete_ids = []
    for row in rows:
        if 'done' in row[1].lower():
            complete_ids.append(row[3])
    return complete_ids


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


def import_metadata(xmls, connections_json='connections.json'):
    import arcpy
    connections = load_json(connections_json, remove_update=True)
    for xml in xmls:
        feature_name = os.path.basename(xml).replace('.xml', '')
        category_name = feature_name.split('.')[1]
        connection = connections[category_name]
        print 'importing', feature_name
        try:
            arcpy.MetadataImporter_conversion(xml, os.path.join(connection, feature_name))
        except Exception as e:
            print xml, e.message


def check_category_and_update(past_update_time, category_name):
    category_id = drive_loader.get_file_id_by_name_and_directory(category_name, CATEGORIES_FOLDER)
    feature_folder_ids = drive_loader.get_subfolder_ids(category_id)
    updated_xml = []
    for folder_id in feature_folder_ids:
        updated_xml.extend(check_files_and_update(past_update_time, parent_folder=folder_id))

    return updated_xml


if __name__ == '__main__':

    # Check for updates
    # past_update_time = "2017-02-01T19:01:53.630000" # load_json('update_config.json')['last_update']
    # # updated_xml = check_category_and_update(past_update_time, 'RECREATION')
    # # import_metadata(updated_xml)
    #
    # complete_folder_ids = get_working_spreadsheet_completed_ids()
    # # print len(complete_folder_ids)
    #
    # updated_xml = []
    # for folder_id in complete_folder_ids:
    #     updated_xml.extend(check_files_and_update(past_update_time, parent_folder=folder_id))
    #
    # print len(updated_xml)
    # import_metadata(updated_xml)


    # Load elements as google docs
    xml_files = load_json(LAST_GISI_OUTPUT)['output_files']
    load_elements_to_drive(xml_files, ['purpose', 'abstract'])

    # get_feature_class_folders()
    # add_full_name('data/outputs/lists/feature_folders.json')
    # count_xml_files('data')
    # create_assign_csv('data/outputs/lists/feature_folders_name.json')
    # xml_files = []
    # for root, dirs, files in os.walk('data/outputs', topdown=True):
    #     for name in files:
    #         if name.endswith('.xml'):
    #             xml_files.append(os.path.join(root, name))
    # get_empty_element_xml(xml_files, ['abstract'])
