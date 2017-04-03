from apiclient import errors
from apiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import os
import io
import time

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'


SERVICE = None


TEMP_FOLDER = '0B3wvsjTJuTRQLTU1N1BndjdTWGc'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_file_comments(file_id, service=SERVICE):
    if not service:
        service = setup_drive_service()
    file_comments = service.comments().list(fileId=file_id,
                                            includeDeleted='false',
                                            fields='comments(author(emailAddress),content,id,replies(content))').execute()
    return file_comments['comments']
    # for comment in file_comments['comments']:
    #     if comment['content'] == '#completed':
    #         print comment


def add_file_to_folders(file_id, parents, service=SERVICE):
    if not service:
        service = setup_drive_service()
    drive_file = service.files().update(fileId=file_id,
                                        addParents=','.join(parents),
                                        fields='name').execute()
    # print drive_file


def remove_file_from_folders(file_id, parents, service=SERVICE):
    if not service:
        service = setup_drive_service()
    drive_file = service.files().update(fileId=file_id,
                                        removeParents=','.join(parents),
                                        fields='name').execute()
    return drive_file


def setup_drive_service():
    # get auth
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)

    return service


def get_file_id_by_name_and_directory(name, parent_id, service=SERVICE):
    if not service:
        service = setup_drive_service()
    response = service.files().list(q="name='{}' and '{}' in parents  and explicitlyTrashed=false".format(name, parent_id),
                                    spaces='drive',
                                    fields='files(id)').execute()
    files = response.get('files', [])
    if len(files) > 0:
        return files[0].get('id')
    else:
        return None


def get_files_directly_in_directory(parent_id, service=SERVICE):
    if not service:
        service = setup_drive_service()
    page_token = None
    files = []
    while True:
        response = service.files().list(q="mimeType != 'application/vnd.google-apps.folder' and '{}' in parents".format(parent_id),
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()
        files.extend(response.get('files', []))

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return files


def get_abstracts_in_directory(parent_id, service=SERVICE):
    if not service:
        service = setup_drive_service()
    page_token = None
    files = []
    while True:
        response = service.files().list(q="mimeType != 'application/vnd.google-apps.folder' and '{}' in parents and name contains 'abstract'".format(parent_id),
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name, parents)',
                                        pageToken=page_token).execute()
        files.extend(response.get('files', []))

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return files


def get_feature_folder_info(folder_id, service=SERVICE):
    if not service:
        service = setup_drive_service()

        response = service.files().get(fileId=folder_id,
                                       fields='name, webViewLink, parents').execute()
    return response


def get_gisi_not_updated_in_directory(parent_id, service=SERVICE):
    if not service:
        service = setup_drive_service()
    page_token = None
    files = []
    while True:
        response = service.files().list(q="mimeType != 'application/vnd.google-apps.folder' and '%s' in parents and not properties has { key='metaGisiUpdated' and value='true'}" % parent_id,
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()
        files.extend(response.get('files', []))

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return files


def create_drive_file(name, parent_ids, media_body, service):

    file_metadata = {'name': name,
                     'mimeType': 'application/vnd.google-apps.document',
                     'parents': parent_ids}

    request = service.files().create(body=file_metadata,
                                     media_body=media_body,
                                     fields="id")
    response = None
    backoff = 3
    while response is None:
        try:
            status, response = request.next_chunk()
            backoff = 3
        except errors.HttpError, e:
            if e.resp.status in [404]:
                # Start the upload all over again.
                raise Exception('Upload Failed 404')
            elif e.resp.status in [500, 502, 503, 504]:
                if backoff >= 81:
                    raise Exception('Upload Failed: {}'.format(e))
                print 'Retrying upload in: {} seconds'.format(backoff)
                time.sleep(backoff)
                status, response = request.next_chunk()
                backoff *= 3
            else:
                raise Exception('Upload Failed')
    # Do not retry. Log the error and fail.
    #   if status :
    #     print('{} percent {}'.format(name, int(status.progress() * 100)))

    return response.get('id')


def create_drive_folder(name, parent_ids, service=SERVICE):
    if not service:
        service = setup_drive_service()
    existing_file_id = get_file_id_by_name_and_directory(name, parent_ids[0], service)
    if existing_file_id:
        return existing_file_id
        # raise Exception('Drive folder {} already exists at: {}'.format(name, existing_file_id))

    file_metadata = {'name': name,
                     'mimeType': 'application/vnd.google-apps.folder',
                     'parents': parent_ids}

    response = service.files().create(body=file_metadata,
                                      fields="id").execute()

    return response.get('id')


def create_google_doc(txt_file_path, parent_id, name):
    service = setup_drive_service()
    existing_file_id = get_file_id_by_name_and_directory(name, parent_id, service)
    if existing_file_id:
        return existing_file_id

    media_body = MediaIoBaseUpload(txt_file_path,
                                   mimetype='text/plain',
                                   resumable=True)

    file_id = create_drive_file(name, [parent_id], media_body, service)

    return file_id


def set_property(file_id, property_dict, service=SERVICE):
    if not service:
        service = setup_drive_service()
    file_name = service.files().update(fileId=file_id,
                                       fields='name',
                                       body={'properties': property_dict}).execute()
    return file_name


def get_property(file_id, property_name, service=SERVICE):
    if not service:
        service = setup_drive_service()
    file_property = service.files().get(fileId=file_id,
                                        fields='properties({})'.format(property_name)).execute()
    return file_property['properties'][property_name]


def comment_reply(file_id, comment_id, message, service=SERVICE):
    if not service:
        service = setup_drive_service()
    reply_id = service.replies().create(fileId=file_id,
                                        commentId=comment_id,
                                        body={'content': message},
                                        fields='id').execute()
    return reply_id


def get_files_updated_after_in_directory(date, parent_id, service=SERVICE):
    if not service:
        service = setup_drive_service()
    page_token = None
    files = []
    while True:
        response = service.files().list(q="mimeType != 'application/vnd.google-apps.folder' and '{}' in parents and modifiedTime > '{}'".format(parent_id, date),
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name, modifiedTime)').execute()
        files.extend(response.get('files', []))

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return files


def get_doc_as_string(file_id, service=SERVICE):
    if not service:
        service = setup_drive_service()

    copy_metadata = {
        'parents': [TEMP_FOLDER]
    }
    copy_request = service.files().copy(fileId=file_id,
                                        body=copy_metadata,
                                        fields='id').execute()
    temp_id = copy_request['id']
    request = service.files().export_media(fileId=temp_id,
                                           mimeType='text/plain')
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    byte_string = fh.getvalue()
    text = byte_string.decode("utf-8-sig'")
    # print text
    service.files().delete(fileId=temp_id).execute()
    return text


def get_doc_as_string_test(file_id, service=SERVICE):
    if not service:
        service = setup_drive_service()

    request = service.files().export_media(fileId=file_id,
                                           mimeType='text/plain')
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    byte_string = fh.getvalue()
    text = byte_string.decode("utf-8-sig'")
    print text
    return text


def get_id_from_meta_src(meta_src_name, meta_src_property, service=SERVICE):
    if not service:
        service = setup_drive_service()
    page_token = None
    files = []
    query = "mimeType != 'application/vnd.google-apps.folder' and not properties has {{ key='{}' and value='{}'}}".format(meta_src_property, meta_src_name)
    while True:
        response = service.files().list(q=query,
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)').execute()
        files.extend(response.get('files', []))

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return files


if __name__ == '__main__':
    # get auth
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)
