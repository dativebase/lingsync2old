#!/usr/bin/python
# coding=utf8

"""
================================================================================
  LingSync-to-OLD Migrator
================================================================================

This is a command-line utility that migrates a LingSync corpus to an Online
Linguistic Database (OLD). Both the source LingSync corpus and the destination
OLD must be accessible at URLs (possibly local) via HTTP.


Warnings/disclaimers
--------------------------------------------------------------------------------

- DEPENDENCY: requires that the Python Requests library be installed. All other
  imports are from the standard library.

- It is assumed that the destination OLD is empty. Migrating a LingSync corpus
  to an OLD that already has data in it may result in errors or corrupted data.

- Some LingSync data points (entire documents or specific fields/attributes)
  are purposefully not migrated. You will need to check the resulting OLD to
  verify that the conversion is satisfactory.


Usage
--------------------------------------------------------------------------------

Just run `lingsync2old.py` and you will be prompted for the required arguments::

    $ ./lingsync2old.py

You can also supploy the required arguments as options::

    $ ./lingsync2old.py \
            --ls-url=https://corpus.lingsync.org \
            --ls-corpus=my-lingsync-corpus-name \
            --ls-username=my-lingsync-username \
            --ls-password=my-lingsync-password \
            --old-url=my-old-url \
            --old-username=my-old-username \
            --old-password=my-old-password

Full param/option listing:

    --force-download: boolean that, when `True`, forces the downloading of the
        LingSync/CouchDB data, even if we have already downloaded it. Default
        is `False`.

    --force-convert: boolean that, when `True`, forces the converting of the
        LingSync JSON data to OLD JSON data, even if we have already converted
        it. Default is `False`.

    --force-file-download: boolean that, when `True`, forces the downloading of
        a LingSync file (e.g., audio), even if we have already downloaded and
        saved it.

    --verbose: boolean that makes this script say more about what it's doing.

    --ls-url: The LingSync CouchDB URL that we can make requests to for
        extracting the LingSync data. Defaults to 'https://corpus.lingsync.org'.

    --ls-corpus: The name of the LingSync corpus that we want to
        migrate.

    --ls-username: The username of a user who has sufficient privileges to
        request the LingSync corpus' data from the CouchDB API.

    --ls-password: The password corresponding to the LingSync
        username.

    --old-url: The OLD URL that we will upload the converted LingSync
        data to.

    --old-username: The username of a user on the destination OLD who
        has sufficient privileges to make create, update and delete requests,
        i.e., an admin or a contributor.

    --old-password: The password corresponding to the OLD username.


Algorithm
--------------------------------------------------------------------------------

It's essentially a three-step algorithm:

1. Download. Request LingSync data as JSON using the CouchDB API (and save it
   locally).

2. Convert. Build a JSON structure (from 1) that the OLD can digest (and save it
   locally).

3. Upload. Use the output of (2) to send JSON/REST POST requests to the relevant
   OLD web service.

Here is the general mapping from LingSync documents (or implicit entities) to
OLD resources:

    LingSync         OLD
    tags        =>   tags
    users       =>   users
    speakers    =>   speakers
    files       =>   files
    datums      =>   forms
    datalists   =>   corpora
    sessions    =>   collections


Questions
--------------------------------------------------------------------------------

1. Are there tags in LingSync sessions?

2. Are there files in LingSync sessions?

3. Should we fill in empty values with the values of other attributes. E.g., if
   the morpheme_break value is empty, should the transcription value be copied
   to it?


TODOs
--------------------------------------------------------------------------------

- large file (> 20MB) upload to OLD still not implemented.

- downloading LingSync image files still not implemented.

- make this script sensitive to OLD versions, and maybe to LingSync ones too.

"""

from fielddb_client import FieldDBClient
from old_client import OLDClient
import requests
import json
import optparse
import getpass
import sys
import os
import shutil
import re
import pprint
import copy
import datetime
import urlparse
import base64
import mimetypes
import codecs

p = pprint.pprint

# Temporary directories
LINGSYNC_DIR = '_ls2old_lingsyncjson'
OLD_DIR = '_ls2old_oldjson'
FILES_DIR = '_ls2old_files'

DEFAULT_PASSWORD = 'password9_B'
FAKE_EMAIL = u'fakeemail@gmail.com'

# Any file over 20MB is considered "big".
BIG_FILE_SIZE = 20000000

# If we have more than 200MB of file data, this script considers that "big
# data".
BIG_DATA = 200000000

# ANSI escape sequences for formatting command-line output.
ANSI_HEADER = '\033[95m'
ANSI_OKBLUE = '\033[94m'
ANSI_OKGREEN = '\033[92m'
ANSI_WARNING = '\033[93m'
ANSI_FAIL = '\033[91m'
ANSI_ENDC = '\033[0m'
ANSI_BOLD = '\033[1m'
ANSI_UNDERLINE = '\033[4m'

migration_tag_name = None


def flush(string):
    """Print `string` immediately, and with no carriage return.

    """

    print string,
    sys.stdout.flush()


def download_lingsync_json(config_dict, database_name):
    """Download the LingSync data in `database_name` using the CouchDB API.
    Save the returned JSON to a local file.

    """

    c = FieldDBClient(config_dict)

    # Login to the LingSync CouchDB.
    couchdb_login_resp = c.login_couchdb()
    try:
        assert couchdb_login_resp['ok'] is True
        print 'Logged in to CouchDB.'
    except:
        print 'Unable to log in to CouchDB.'
        return None

    # Get the JSON from CouchDB
    flush('Downloading all documents from %s' % database_name)
    all_docs = c.get_all_docs_list(database_name)
    if type(all_docs) is type({}) and all_docs.get('error') == 'unauthorized':
        print (u'%sUser %s is not authorized to access the LingSync corpus'
            u' %s.%s' % (ANSI_FAIL, config_dict['admin_username'],
            database_name, ANSI_ENDC))
        return None
    print 'Downloaded all documents from %s' % database_name

    # Write the LingSync/CouchDB JSON to a local file
    fname = get_lingsync_json_filename(database_name)
    with open(fname, 'w') as outfile:
        json.dump(all_docs, outfile)
    print 'Wrote all documents JSON file to %s' % fname

    return fname


def get_lingsync_json_filename(database_name):
    """Get the relative path to the file where the downloaded LingSync JSON are
    saved for the LingSync corpus `database_name`.

    """

    return os.path.join(LINGSYNC_DIR, '%s.json' % database_name)


def add_optparser_options(parser):
    """Add options to the optparser parser.

    --ls-url: The LingSync CouchDB URL that we can make requests to for
        extracting the LingSync data. Defaults to 'https://corpus.lingsync.org'.

    --ls-corpus: The name of the LingSync corpus that we want to
        migrate.

    --ls-username: The username of a user who has sufficient privileges to
        request the LingSync corpus' data from the CouchDB API.

    --ls-password: The password corresponding to the LingSync
        username.

    --old-url: The OLD URL that we will upload the converted LingSync
        data to.

    --old-username: The username of a user on the destination OLD who
        has sufficient privileges to make create, update and delete requests,
        i.e., an admin or a contributor.

    --old-password: The password corresponding to the OLD username.

    --force-download: boolean that, when `True`, forces the downloading of the
        LingSync/CouchDB data, even if we have already downloaded it. Default
        is `False`.

    --force-convert: boolean that, when `True`, forces the converting of the
        LingSync JSON data to OLD JSON data, even if we have already converted
        it. Default is `False`.

    --force-file-download: boolean that, when `True`, forces the downloading of
        a LingSync file (e.g., audio), even if we have already downloaded and
        saved it.

    --verbose: boolean that makes this script say more about what it's doing.

    """

    parser.add_option("--ls-url", dest="ls_url",
        default='https://corpus.lingsync.org', metavar="LS_URL",
        help="The LingSync CouchDB URL that we can make requests to for"
        " extracting the LingSync data. Defaults to"
        " 'https://corpus.lingsync.org'.")

    parser.add_option("--ls-corpus", dest="ls_corpus", metavar="LS_CORPUS",
        help="The name of the LingSync corpus that we want to migrate.")

    parser.add_option("--ls-username", dest="ls_username",
        metavar="LS_USERNAME", help="The username of a user who has sufficient"
        " privileges to request the LingSync corpus' data from the CouchDB API.")

    parser.add_option("--ls-password", dest="ls_password",
        metavar="LS_PASSWORD", help="The password corresponding to the LingSync"
        " username.")

    parser.add_option("--old-url", dest="old_url", metavar="OLD_URL",
        help="The OLD URL that we will upload the converted LingSync data to.")

    parser.add_option("--old-username", dest="old_username",
        metavar="OLD_USERNAME", help="The username of a user on the destination"
        " OLD who has sufficient privileges to make create, update and delete"
        " requests, i.e., an admin or a contributor.")

    parser.add_option("--old-password", dest="old_password",
        metavar="OLD_PASSWORD", help="The password corresponding to the OLD"
        " username.")

    parser.add_option("-d", "--force-download", dest="force_download",
            action="store_true", default=False, metavar="FORCEDOWNLOAD",
            help="Use this option if you want to download the LingSync data,"
            " even if it has already been downloaded.")

    parser.add_option("-c", "--force-convert", dest="force_convert",
            action="store_true", default=False, metavar="FORCECONVERT",
            help="Use this option if you want to convert the LingSync data"
            " to OLD format, even if it has already been converted.")

    parser.add_option("-f", "--force-file-download", dest="force_file_download",
            action="store_true", default=False, metavar="FORCEFILEDOWNLOAD",
            help="Use this option if you want to download LingSync"
            " audio/video/image files, even if they have already been"
            " downloaded.")

    parser.add_option("-v", "--verbose", dest="verbose",
            action="store_true", default=False, metavar="VERBOSE",
            help="Make this script say more about what it's doing.")


################################################################################
# OLD resource schemata
################################################################################

# This holds dicts that contain default OLD resources. These are copied
# elsewhere in the script when OLD resources-as-dicts are created.

old_schemata = {

    'corpus': {
        'name': u'', # required, unique among corpus names, max 255 chars
        'description': u'', # string description
        'content': u'', # string containing form references
        'tags': [], # OLD sends this as an array of objects (attributes: `id`, `name`) but receives it as an array of integer relational ids, all of which must be valid tag ids.
        'form_search': None # OLD sends this as an object (attributes: `id`, `name`) but receives it as a relational integer id; must be a valid form search id.
    },

    'file': {
        'description': u'', # A description of the file.
        'utterance_type': u'', # If the file represents a recording of an # utterance, then a value here may be # appropriate; possible values accepted by the # OLD currently are 'None', 'Object Language # Utterance', 'Metalanguage Utterance', and # 'Mixed Utterance'.
        'speaker': None, # A reference to the OLD speaker who was the # speaker of this file, if appropriate.
        'elicitor': None, # A reference to the OLD user who elicited this # file, if appropriate.
        'tags': [], # An array of OLD tags assigned to the file.
        'forms': [], # An array of forms associated to this file.
        'date_elicited': u'', # When this file was elicited, if appropriate.
        'base64_encoded_file': u'', # `base64_encoded_file`: When creating a file, # this attribute may contain a base-64 encoded # string representation of the file data, so long # as the file size does not exceed 20MB.  
        'filename': u'', # the filename, cannot be empty, max 255 chars.  # Note: the OLD will remove quotation marks and # replace spaces with underscores. Note also that # the OLD will not allow the file to be created # if the MIMEtype guessed on the basis of the # filename is different from that guessed on the # basis of the file data.
        'name': u'', # the name of the file, max 255 chars; This value # is only valid when the file is created as a # subinterval-referencing file or as a file whose # file data are stored elsewhere, i.e., at the # provided URL.
        'MIME_type': u'' # a string representing the MIME type.
    },

    'form': {
        'transcription': u'', # = ValidOrthographicTranscription(not_empty=True, max=255)
        'phonetic_transcription': u'', # = ValidBroadPhoneticTranscription(max=255)
        'narrow_phonetic_transcription': u'', # = ValidNarrowPhoneticTranscription(max=255)
        'morpheme_break': u'', # = ValidMorphemeBreakTranscription(max=255)
        'grammaticality': u'', # = ValidGrammaticality(if_empty='')
        'morpheme_gloss': u'', # = UnicodeString(max=255)
        'translations': [], # = ValidTranslations(not_empty=True)
        'comments': u'', # = UnicodeString()
        'speaker_comments': u'', # = UnicodeString()
        'syntax': u'', # = UnicodeString(max=1023)
        'semantics': u'', # = UnicodeString(max=1023)
        'status': u'', # = OneOf(h.form_statuses)
        'elicitation_method': None, # = ValidOLDModelObject(model_name='ElicitationMethod')
        'syntactic_category': None, # = ValidOLDModelObject(model_name='SyntacticCategory')
        'speaker': None, # = ValidOLDModelObject(model_name='Speaker')
        'elicitor': None, # = ValidOLDModelObject(model_name='User')
        'verifier': None, # = ValidOLDModelObject(model_name='User')
        'source': None, # = ValidOLDModelObject(model_name='Source')
        'tags': [], # = ForEach(ValidOLDModelObject(model_name='Tag'))
        'files': [], # = ForEach(ValidOLDModelObject(model_name='File'))
        'date_elicited': u'' # = DateConverter(month_style='mm/dd/yyyy')
    },

    'collection': {
        'title': u'',
        'type': u'',
        'url': u'',
        'description': u'',
        'markup_language': u'',
        'contents': u'',
        'contents_unpacked': u'',
        'speaker': None,
        'source': None,
        'elicitor': None,
        'date_elicited': u'',
        'tags': [],
        'files': []
    },

    'user': {
        'username': u'', # = UnicodeString(max=255)
        'password': u'', # = UnicodeString(max=255)
        'password_confirm': u'', # = UnicodeString(max=255)
        'first_name': u'', # = UnicodeString(max=255, not_empty=True)
        'last_name': u'', # = UnicodeString(max=255, not_empty=True)
        'email': u'', # = Email(max=255, not_empty=True)
        'affiliation': u'', # = UnicodeString(max=255)
        'role': u'', # = OneOf(h.user_roles, not_empty=True)
        'markup_language': u'', # = OneOf(h.markup_languages, if_empty='reStructuredText')
        'page_content': u'', # = UnicodeString()
        'input_orthography': None,
        'output_orthography': None
    },

    'speaker': {
        'first_name': u'', # = UnicodeString(max=255, not_empty=True)
        'last_name': u'', # = UnicodeString(max=255, not_empty=True)
        'dialect': u'', # = UnicodeString(max=255)
        'page_content': u'', # = UnicodeString()
        'markup_language': u'', # = OneOf(h.markup_languages, if_empty='reStructuredText')
    },

    'tag': {
        'name': u'',
        'description': u''
    },

    'applicationsettings': {
        'id': None,
        'object_language_name': u'', # 255 chrs max
        'object_language_id': u'', # 3 chrs max, ISO 639-3 3-char Id code
        'metalanguage_name': u'', # 255 chrs max
        'metalanguage_id': u'', # 3 chrs max, ISO 639-3 3-char Id code
        'metalanguage_inventory': u'', # long text; Don't think this is really used for any OLD-side logic.
        'orthographic_validation': u'None', # one of 'None', 'Warning', or 'Error'
        'narrow_phonetic_inventory': u'', # long text; should be comma-delimited graphemes
        'narrow_phonetic_validation': u'None', # one of 'None', 'Warning', or 'Error'
        'broad_phonetic_inventory': u'', # long text; should be comma-delimited graphemes
        'broad_phonetic_validation': u'None', # one of 'None', 'Warning', or 'Error'
        'morpheme_break_is_orthographic': False, # boolean
        'morpheme_break_validation': u'None',  # one of 'None', 'Warning', or 'Error'
        'phonemic_inventory': u'', # long text; should be comma-delimited graphemes
        'morpheme_delimiters': u'', # 255 chars max; should be COMMA-DELIMITED single chars...
        'punctuation': u'', # long text; should be punctuation chars
        'grammaticalities': u'', # 255 chars max ...
        'storage_orthography': None, # id of an orthography
        'input_orthography': None, # id of an orthography
        'output_orthography': None, # id of an orthography
        'unrestricted_users': [] # an array of users who are "unrestricted". In the OLD this is a m2m relation, I think.
    }

}


def get_collection_for_lingsync_doc(doc):
    """A LingSync document is identified by its `collection` attribute, which is
    valuated by a string like 'sessions', or 'datums'. Sometimes, however,
    there is no `collection` attribute and the `fieldDBtype` attribute is
    used and evaluates to a capitalized, singular analog, e.g., 'Session' or
    'Datum'. This function returns a collection value for a LingSync document.

    """

    type2collection = {
        'Session': 'sessions',
        'Corpus': 'private_corpuses', # or 'corpuses'?
        'Datum': 'datums'
    }
    collection = doc.get('collection')
    if not collection:
        fieldDBtype = doc.get('fieldDBtype')
        if fieldDBtype:
            collection = type2collection.get(fieldDBtype)
    return collection


def lingsync2old(fname, lingsync_db_name, force_file_download):
    """Convert the LingSync database (named `lingsync_db_name`, whose data are
    stored in the JSON file `fname`) to an OLD-compatible JSON file. This is
    the primary "convert" function that represents Step 2.

    """

    # Maps names of OLD resources (pluralized) to lists of dicts, where each
    # such dict is a valid payload for an OLD POST request.
    old_data = {}

    # Holds warning messages accrued via the transformation of LingSync data
    # structures to OLD ones.
    warnings = {}

    # This holds all of the `language` values from the LingSync sessions that
    # we process. Since the OLD assumes a single language, we will arbitrarily
    # choose the first one when creating the OLD's application settings.
    languages = set()

    lingsync_data = json.load(open(fname))
    try:
        rows = lingsync_data['rows']
    except KeyError:
        p(lingsync_data)
        sys.exit(u'%sUnable to load LingSync data. Aborting.%s' % (ANSI_FAIL,
            ANSI_ENDC))

    # - LingSync sessions are turned into OLD collections.
    # - LingSync datums are turned into OLD forms.
    # - LingSync corpuses are not used.
    # - LingSync private_corpuses are not used.
    # - LingSync users are turned into OLD users.
    # - LingSync datalists are turned into OLD corpora.
    # - LingSync documents with no `collection` value are logic, not data; i.e.,
    #   mapreduces or something else.

    # Note: we don't necessarily need to loop through all rows for each
    # collection type. We may need to process the sessions first, because the
    # datums refer to them. However, it seems that every datum redundantly
    # holds a copy of its session anyway, so this may not be necessary.

    # LS-Session to OLD-Collection.
    # Deal with LingSync sessions first, since they contain data that will
    # be needed for datums-come-forms later on.
    # if r.get('doc', {}).get('collection') == 'sessions':
    for r in rows:
        if get_collection_for_lingsync_doc(r.get('doc', {})) == 'sessions':
            old_object = process_lingsync_session(r['doc'])
            if old_object:
                old_data, warnings = update_state(old_object, old_data,
                    warnings)
                # Add any language extracted from the session.
                if old_object.get('language'):
                    languages.add(old_object['language'])

    # LS-Datum to OLD-Form.
    for r in rows:
        if get_collection_for_lingsync_doc(r.get('doc', {})) == 'datums':
            old_object = process_lingsync_datum(r['doc'],
                old_data['collections'], lingsync_db_name)
            old_data, warnings = update_state(old_object, old_data, warnings)

    # Note: LingSync corpus and private_corpus documents don't appear to
    # contain any data that need to be migrated to the OLD. They contain
    # metadata about the corpus, including licensing information and basic info
    # about what datum and session fields to expect.
    # Uncomment the following block to inspect the corpus/private_corpus
    # documents in the JSON dump being analyzed.

    # LS-User to OLD-User
    for r in rows:
        if get_collection_for_lingsync_doc(r.get('doc', {})) == 'users':
            old_object = process_lingsync_user(r['doc'])
            old_data, warnings = update_state(old_object, old_data, warnings)

    # LS-Datalist to OLD-Corpus
    for r in rows:
        if get_collection_for_lingsync_doc(r.get('doc', {})) == 'datalists':
            old_object = process_lingsync_datalist(r['doc'])
            old_data, warnings = update_state(old_object, old_data, warnings)

    # Merge/consolidate duplicate users, speakers and tags.
    old_data, warnings = consolidate_resources(old_data, warnings)

    # Get an OLD application settings, using the language(s) and
    # grammaticalities extracted from the LingSync corpus.
    old_application_settings, warnings = get_old_application_settings(old_data,
        languages, warnings)
    old_data['applicationsettings'] = [old_application_settings]

    # Download audio, video or image files from the LingSync application, if
    # necessary.
    old_data, warnings, exit_status = download_lingsync_media_files(old_data,
        warnings, lingsync_db_name, force_file_download)

    if exit_status == 'aborted':
        print ('You chose not to migrate audio/video/image files from LingSync'
            ' to OLD because they were too large.')

    # Tell the user what we've accomplished.
    print_summary(lingsync_db_name, rows, old_data, warnings)

    # Save our OLD data to a JSON file in OLD_DIR/
    old_data_fname = write_old_data_to_disk(old_data, lingsync_db_name)

    return old_data_fname


def create_files_directory_safely(lingsync_db_name):
    """Create a directory to hold the LingSync media files, only if it doesn't
    already exist.

    """

    dirpath = os.path.join(FILES_DIR, lingsync_db_name)
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    return dirpath


def human_bytes(num_bytes):
    """Return an integer byte count in human-readable form.

    """

    if num_bytes is None:
        return 'File size unavailable.'
    KiB = 1024
    MiB = KiB * KiB
    GiB = KiB * MiB
    TiB = KiB * GiB
    PiB = KiB * TiB
    EiB = KiB * PiB
    ZiB = KiB * EiB
    YiB = KiB * ZiB
    if num_bytes > YiB:
        return '%.3g YiB' % (num_bytes / YiB)
    elif num_bytes > ZiB:
        return '%.3g ZiB' % (num_bytes / ZiB)
    elif num_bytes > EiB:
        return '%.3g EiB' % (num_bytes / EiB)
    elif num_bytes > PiB:
        return '%.3g PiB' % (num_bytes / PiB)
    elif num_bytes > TiB:
        return '%.3g TiB' % (num_bytes / TiB)
    elif num_bytes > GiB:
        return '%.3g GiB' % (num_bytes / GiB)
    elif num_bytes > MiB:
        return '%.3g MiB' % (num_bytes / MiB)
    elif num_bytes > KiB:
        return '%.3g KiB' % (num_bytes / KiB)
    else:
        return '%d bytes' % num_bytes


def download_lingsync_media_files(old_data, warnings, lingsync_db_name, force_file_download):
    """If `old_data` contains OLD file resources generated from LingSync files,
    then we need to download their file data and save them for later upload to
    the OLD.

    """

    if len(old_data.get('files', [])) == 0:
        return (old_data, warnings, 'ok')

    files = old_data['files']
    file_count = len(files)
    file_sizes = filter(None,
        [f.get('__lingsync_file_size') for f in files])
    total_files_size = sum(file_sizes)
    total_files_size_human = human_bytes(total_files_size)
    big_file_size_human = human_bytes(BIG_FILE_SIZE)
    big_files = [s for s in file_sizes if s > BIG_FILE_SIZE]
    we_have_big_files = bool(big_files)
    we_have_big_data = total_files_size > BIG_DATA
    if we_have_big_files or we_have_big_data:
        if we_have_big_files and we_have_big_data:
            msg = (u'Your LingSync corpus contains at least %s worth of'
                u' (audio/video/image) file data, including at least one'
                u' file bigger than %s.' % (total_files_size_human,
                big_file_size_human))
        elif we_have_big_files:
            msg = (u'Your LingSync corpus contains audio/video/image files,'
                u' some of which are bigger than %s.' % (
                big_file_size_human,))
        elif we_have_big_data:
            msg = (u'Your LingSync corpus contains at least %s worth of'
                u' (audio/video/image) file data.' % (
                total_files_size_human,))
        response = raw_input(u'%s%s Enter \'y\'/\'Y\' if you want this'
            u' script to download all of those files from LingSync and'
            u' migrate them to your OLD. Enter \'n\'/\'N\' (or anything'
            u' else) to skip the migrating of files:%s ' % (ANSI_WARNING,
            msg, ANSI_ENDC))
        if response not in ['y', 'Y']:
            warnings['general'].add(u'You have lots of file data (i.e.,'
                u' audio, video, or images) in your LingSync corpus and you'
                u' chose not to migrate them using this script.')
            old_data['files'] = []
            return (old_data, warnings, 'aborted')
    dirpath = create_files_directory_safely(lingsync_db_name)
    downloaded_files = []
    for file in old_data['files']:
        url = file.get('__lingsync_file_url')
        fname = file.get('filename')
        fsize = file.get('__lingsync_file_size')
        if not fname:
            try:
                fname = os.path.split(url)[1]
            except:
                fname = None
        if url and fname:
            filepath = os.path.join(dirpath, fname)
            outcome, warnings = download_lingsync_file(url, filepath,
                fsize, warnings, force_file_download)
            if outcome:
                file['__local_file_path'] = filepath
                downloaded_files.append(file)
            else:
                warnings['general'].add(u'We were unable to download the'
                    u' file data for a file associated to LingSync datum'
                    u' %s; download and/or local write failed.' % (
                    file['__lingsync_datum_id'],))
        else:
            warnings['general'].add(u'We were unable to download the file'
                u' data for a file associated to LingSync datum %s; URL or'
                u' filename was not retrievable.' % (
                file['__lingsync_datum_id'],))
    old_data['files'] = downloaded_files
    return (old_data, warnings, 'ok')


def download_lingsync_file(url, filepath, fsize, warnings, force_file_download):
    """Download a LingSync file at `url` save it to `filepath`.

    """

    if os.path.isfile(filepath) and (not force_file_download):
        return (True, warnings)

    file_is_big = False
    if fsize and fsize > BIG_FILE_SIZE:
        file_is_big = True

    with open(filepath, 'wb') as handle:
        response = requests.get(url, stream=file_is_big, verify=False)

        if not response.ok:
            warnings['general'].add(u'Attempt to download LingSync file at %s'
                u' failed.' % (url,))
            return (False, warnings)

        if file_is_big:
            for block in response.iter_content(1024):
                handle.write(block)
        else:
            handle.write(response.content)

    if os.path.isfile(filepath):
        return (True, warnings)
    else:
        return (False, warnings)


def get_old_application_settings(old_data, languages, warnings):
    """Return an OLD application settings dict, given a set of (object)
    language names and the grammaticalities (in the forms in `old_data`).

    """

    appset = copy.deepcopy(old_schemata['applicationsettings'])
    if languages:
        languages = list(languages)
        language = languages[0]
        appset['object_language_name'] = language
        if len(languages) > 1:
            warnings['general'].add(u'Arbitrarily chose \u2018%s\u2019 as the'
                u' OLD object language when the following languages were listed'
                u' in the LingSync corpus: \u2018%s\u2019.' % (language,
                u'\u2019, \u2018'.join(languages)))

    grammaticalities = set()
    for form in old_data.get('forms'):
        grammaticalities.add(form.get('grammaticality', u''))
    grammaticalities = u','.join([g for g in list(grammaticalities) if g])
    appset['grammaticalities'] = grammaticalities

    return (appset, warnings)


def consolidate_users(duplicates):
    """Given an array of duplicate user objects `duplicates`, return a single
    (consolidated) user and an array of warnings, if applicable.

    """

    return_user = {'username': duplicates[0]['username']}
    user_warnings = []
    for attr in duplicates[0]:
        if attr != 'username':
            vals = list(set([u[attr] for u in duplicates if u[attr]]))
            try:
                new_val = vals[0]
            except:
                new_val = u''
            if len(vals) > 1:
                user_warnings.append(u'Lost data when consolidating users: we'
                    u' chose \u2018%s\u2019 as the val for \u2018%s\u2019 and'
                    u' the following values were discarded: \u2018%s\u2019.' % (
                    new_val, attr, u'\u2019, \u2018'.join(vals[1:])))
            return_user[attr] = new_val
    return (return_user, user_warnings)


def consolidate_speakers(duplicates):
    """Given an array of duplicate speaker objects `duplicates`, return a single
    (consolidated) speaker and an array of warnings, if applicable.

    """

    return_speaker = copy.deepcopy(old_schemata['speaker'])
    speaker_warnings = []
    for attr in return_speaker:
        if attr in ['first_name', 'last_name']:
            return_speaker[attr] = duplicates[0][attr]
        else:
            vals = list(set([s[attr] for s in duplicates if s[attr]]))
            try:
                new_val = vals[0]
            except:
                new_val = u''
            if len(vals) > 1:
                speaker_warnings.append(u'Lost data when consolidating'
                    u' speakers: we chose \u2018%s\u2019 as the val for'
                    u' \u2018%s\u2019 and the following values were discarded:'
                    u' \u2018%s\u2019.' % (new_val, attr,
                    u'\u2019, \u2018'.join(vals[1:])))
            return_speaker[attr] = new_val
    return (return_speaker, speaker_warnings)


def consolidate_resources(old_data, warnings):
    """Look for duplicate users, speakers and tags in `old_data` and merge the
    duplicates into a single resource of the relevant type.

    """

    # Consolidate users.
    # If multiple user objects have the same `username` value, we merge them
    # into one user.
    if len(old_data.get('users', [])) > 1:
        users = old_data['users']
        consolidated_users = []
        consolidate_users_warnings = []
        processed = []
        for user in users:
            if user not in processed:
                username = user['username']
                duplicates = [u for u in users if u['username'] == username]
                processed += duplicates
                if len(duplicates) > 1:
                    new_user, user_warnings = consolidate_users(duplicates)
                    consolidate_users_warnings += user_warnings
                    consolidated_users.append(new_user)
                else:
                    consolidated_users.append(user)
        old_data['users'] = consolidated_users
        for warning in consolidate_users_warnings:
            warnings['general'].add(warning)

    # Consolidate speakers
    # If multiple speaker objects have the same `first_name` and `last_name`
    # values, we merge them into one user.
    if len(old_data.get('speakers', [])) > 1:
        speakers = old_data['speakers']
        consolidated_speakers = []
        consolidate_speakers_warnings = []
        processed = []
        for speaker in speakers:
            if speaker not in processed:
                first_name = speaker['first_name']
                last_name = speaker['last_name']
                duplicates = [u for u in speakers if
                        u['first_name'] == first_name and
                        u['last_name'] == last_name]
                processed += duplicates
                if len(duplicates) > 1:
                    new_speaker, speaker_warnings = consolidate_speakers(
                        duplicates)
                    consolidate_speakers_warnings += speaker_warnings
                    consolidated_speakers.append(new_speaker)
                else:
                    consolidated_speakers.append(speaker)
        old_data['speakers'] = consolidated_speakers
        for warning in consolidate_speakers_warnings:
            warnings['general'].add(warning)

    # Consolidate tags
    # If multiple tag objects have the same `name` values, we merge them into
    # one user.
    if len(old_data.get('tags', [])) > 1:
        tags = old_data['tags']
        consolidated_tags = []
        consolidate_tags_warnings = []
        processed = []
        for tag in tags:
            if tag not in processed:
                name = tag['name']
                description = tag['description']
                duplicates = [t for t in tags if t['name'] == name]
                processed += duplicates
                if len(duplicates) > 1:
                    new_tag = tag
                    new_description = u'\n\n'.join([t['description'] for t in
                        duplicates if t['description']])
                    new_tag['description'] = description
                    if new_description != description:
                        consolidate_tags_warnings.append(u'Changed description'
                            u' of tag \u2018%s\u2019 from \u2018%s\u2019 to'
                            u' \u2018%s\u2019' % (name, description,
                            new_description))
                    consolidated_tags.append(new_tag)
                else:
                    consolidated_tags.append(tag)
        old_data['tags'] = consolidated_tags
        for warning in consolidate_tags_warnings:
            warnings['general'].add(warning)

    return old_data, warnings


def get_old_json_filename(database_name):
    """Return the relative path where we store the JSON file that holds the
    LingSync data in a format that the OLD can ingest.

    """

    return os.path.join(OLD_DIR, '%s.json' % database_name)


def write_old_data_to_disk(old_data, database_name):
    """Save the OLD data extracted from the LingSync corpuse to a JSON file so
    we don't need to re-migrate/convert it every time.

    """

    fname = get_old_json_filename(database_name)
    with open(fname, 'w') as outfile:
        json.dump(old_data, outfile, indent=4)
    return fname


def get_lingsync_corpus_summary(rows):
    """Return a string summarizing the LingSync documents that we downloaded.

    """

    collections = {}
    summary = [u'\nLingSync documents downloaded.']
    for r in rows:
        collection = get_collection_for_lingsync_doc(r.get('doc', {}))
        if collection is None:
            collection = u'NOT DATA'
        collections.setdefault(collection, 0)
        collections[collection] += 1
    for c in sorted(collections.keys()):
        collection_count = collections[c]
        summary.append(u'  %s: %d' % (c, collection_count))
    return u'\n'.join(summary)


def get_summary_of_old_data(old_data):
    """Return a string summarizing the OLD resources that will be created.

    """

    summary = [u'\nOLD resources to be created.']
    for resource_name in sorted(old_data.keys()):
        resource_list = old_data[resource_name]
        summary.append(u'  %s: %d' % (resource_name, len(resource_list)))
    return u'\n'.join(summary)


def print_summary(lingsync_db_name, rows, old_data, warnings):
    """Print a summary of the OLD data and warnings generated.
    Also save to disk the summaries of downloaded LingSync data and converted
    OLD data. We save these so that the --verbose option can work consistently.

    """

    lingsync_summary = get_lingsync_corpus_summary(rows)
    path = os.path.join(LINGSYNC_DIR, '%s-summary.txt' % lingsync_db_name)
    with codecs.open(path, mode='w', encoding='utf-8') as f:
        f.write(lingsync_summary)
    print lingsync_summary

    old_summary = get_summary_of_old_data(old_data)
    path = os.path.join(OLD_DIR, '%s-summary.txt' % lingsync_db_name)
    with codecs.open(path, mode='w', encoding='utf-8') as f:
        f.write(old_summary)
    print old_summary

    warnings_text = []
    if warnings:

        warnings_count = 0
        for warning_locus, warnings_set in warnings.iteritems():
            warnings_count += len(warnings_set)

        if warnings_count == 1:
            warnings_text.append(u'\n%s%d Conversion Warning.%s' % (ANSI_WARNING,
                warnings_count, ANSI_ENDC))
        else:
            warnings_text.append(u'\n%s%d Conversion Warnings.%s' % (
                ANSI_WARNING, warnings_count, ANSI_ENDC))

        index = 0
        if warnings.get('general'):
            warnings_text.append(u'\n  General warnings:')
            for warning in warnings['general']:
                index += 1
                warnings_text.append(u'    %d. %s' % (index, warning))

        for warning_locus in sorted(warnings.keys()):
            if warning_locus != 'general':
                warnings_set = warnings[warning_locus]
                warnings_text.append(u'\n  Warning(s) for %s:' % warning_locus)
                for warning in sorted(warnings_set):
                    index += 1
                    warnings_text.append(u'    %d. %s' % (index, warning))
    else:
        warnings_text.append(u'\nNo warnings.')

    warnings_text = u'\n'.join(warnings_text)
    path = os.path.join(OLD_DIR, '%s-conversion-warnings.txt' %
        lingsync_db_name)
    with codecs.open(path, mode='w', encoding='utf-8') as f:
        f.write(warnings_text)
    print warnings_text


def update_state(old_object, old_data, warnings):
    """Update `old_data` and `warnings` with the contents of `old_object`,
    where `old_object` is the OLD resource-as-object/dict that was derived from
    a LingSync document.

    """

    # Add our primary "old_resource" `old_data`
    if old_object['old_resource']:
        key = old_object['old_resource']
        val = old_object['old_value']
        old_data.setdefault(key, [])
        if val not in old_data[key]:
            old_data[key].append(val)

    # Add any auxiliary resources to `old_data`
    if len(old_object['old_auxiliary_resources']) > 0:
        for rname, rlist in old_object['old_auxiliary_resources'].items():
            existing = old_data.setdefault(rname, [])
            for resource in rlist:
                if resource not in existing:
                    existing.append(resource)

    # Add any gathered warnings to accrued warnings
    if old_object['warnings']['docspecific']:
        warnings_key = u'OLD %s resource generated from LingSync %s %s' % (
            old_object['old_resource'], old_object['lingsync_type'],
            old_object['originaldoc']['_id'])
        warnings.setdefault(warnings_key, set())
        for warning in old_object['warnings']['docspecific']:
            warnings[warnings_key].add(warning)
    if old_object['warnings']['general']:
        warnings.setdefault('general', set())
        for warning in old_object['warnings']['general']:
            warnings['general'].add(warning)

    return (old_data, warnings)


def timestamp2human(timestamp):
    """Return a timestamp in a "human-readable" format.

    """

    try:
        return datetime.datetime\
            .fromtimestamp(int(timestamp) * 0.001).strftime("%Y-%m-%d %H:%M")
    except:
        return None


def process_lingsync_comments_val(ls_comments, warnings):
    """Process the value of a LingSync datum comments attribute or comments
    datum field.

    """

    comments_to_return = []
    if ls_comments and (type(ls_comments) is type([])):
        for comment_obj in ls_comments:
            if type(comment_obj) is type({}) and comment_obj.get('text'):
                author = u''
                if comment_obj.get('username'):
                    author = u' by %s' % comment_obj['username']
                created = u''
                if comment_obj.get('dateCreated'):
                    human_date_created = timestamp2human(
                        comment_obj['dateCreated'])
                    if human_date_created:
                        created = u' on %s' % human_date_created
                    else:
                        print u'WARNING: unable to parse timestamp %s' % (
                            comment_obj['dateCreated'],)
                modified = u''
                if comment_obj.get('timestampModified'):
                    human_date_modified = timestamp2human(
                        comment_obj['timestampModified'])
                    if human_date_modified:
                        modified = u' (last modified %s)' % human_date_modified
                    else:
                        print u'WARNING: unable to parse timestamp %s' % (
                            comment_obj['timestampModified'],)
                comment_ = u'Comment %s%s%s: %s' % (author, created, modified,
                    punctuate_period_safe(comment_obj['text']))
                comments_to_return.append(comment_)
            else:
                warnings['docspecific'].append(u'Unable to process the following comment (from'
                    u' of datum %s): \u2018%s\u2019' % (datum_id,
                    unicode(comment_obj)))
    else:
        if (type(ls_comments) is type('')) or (type(ls_comments) is type(u'')):
            if ls_comments.strip():
                comments_to_return.append(u'Comment: %s' % (
                    punctuate_period_safe(ls_comments),))
    return (comments_to_return, warnings)


def process_lingsync_datalist(doc):
    """Convert a LingSync datalist document to an OLD corpus dict.

    QUESTIONS:

    1. Can a datalist have tags?

    2. What does a datalist's `audioVideo` value look like?

    """

    # Pretty-print the user document, for inspection.
    # p(doc)

    datalist_id = doc.get('_id')

    auxiliary_resources = {}

    # These are the LingSync datalist attrs that we know how to deal with for
    # to-OLD conversion.
    known_attrs = [
        '_id', # u'0d69a355b63fa165273111aa739802c1',
        '_rev', # u'1-285126da331e5b2c7df7552599de2960',
        'audioVideo', # [],
        'collection', # u'datalists',
        'comments', # [],
        'dateCreated', # u'"2014-11-10T02:29:25.168Z"',
        'dateModified', # u'"2014-11-10T02:29:25.309Z"',
        'datumIds', # [u'a8b939f86a76109b121100e944b6a758', ...],
        'description', # u'This is the result of searching for : morphemes:#nit- In Blackfoot on Sun Nov 09 2014 18:29:24 GMT-0800 (PST)',
        'pouchname',
        'timestamp', # 1415586565309,
        'title' # u'All Data as of Sun Nov 09 2014 18:29:25 GMT-0800 (PST)'}
    ]

    # Add warnings to this.
    warnings = {
        'general': [],
        'docspecific': []
    }


    for k in doc:
        if k not in known_attrs:
            warnings['docspecific'].append(u'\u2018%s\u2019 not a recognized'
                u' attribute in datalist %s' % (k, datalist_id))

    # This will be our return value.
    oldobj = {
        'originaldoc': doc,
        'lingsync_type': 'datalist',
        'old_resource': 'corpora',
        'old_value': {}, # Valuate this with `old_corpus`
        'old_auxiliary_resources': {}, # Valuate this with `auxiliary_resources`
        'warnings': []
    }

    # This dict will be used to create the OLD corpus.
    old_corpus = copy.deepcopy(old_schemata['corpus'])

    # Description.
    old_description = []

    # Datalist description -> corpus description
    ls_description = doc.get('description')
    if ls_description:
        old_description.append(ls_description)

    # Datalist metadata (id, timestamps, etc) -> corpus description
    datalist_metadata = []
    datalist_metadata.append(u'This corpus was generated from LingSync datalist'
        u' %s.' % datalist_id)
    ls_dateCreated = doc.get('dateCreated')
    if ls_dateCreated:
        datalist_metadata.append(u'It was created in LingSync on %s.' % (
            ls_dateCreated,))
    ls_dateModified = doc.get('dateModified')
    if ls_dateModified:
        datalist_metadata.append(u'It was last modified in LingSync on %s.' % (
            ls_dateModified,))
    datalist_metadata = u' '.join(datalist_metadata)
    old_description.append(datalist_metadata)

    # Datalist comments -> corpus description
    ls_comments = doc.get('comments')
    if ls_comments:
        processed_comments, warnings = process_lingsync_comments_val(
            ls_comments, warnings)
        if processed_comments:
            old_description += processed_comments

    old_description = u'\n\n'.join(old_description).strip()
    if old_description:
        old_corpus['description'] = old_description

    # Name. Not empty, max 255 chars. From LingSync datalist title.
    title_too_long = False
    ls_title = doc.get('title')
    if not ls_title:
        old_name = u'Corpus from LingSync datalist %s' % datalist_id
        warnings['docspecific'].append(u'Datalist %s has no title value; the corpus generated'
            ' from it has "%s" as its name value.' % (datalist_id, old_name))
    elif len(ls_title) > 255:
        title_too_long = True
        warnings['docspecific'].append('The title "%s" of datalist %s is too long and will be'
            ' truncated.' % (ls_title, datalist_id))
        old_name = ls_title[:255]
    else:
        old_name = ls_title
    old_corpus['name'] = old_name

    old_corpus['__lingsync_datalist_id'] = datalist_id
    old_corpus['__lingsync_datalist_datum_ids'] = doc.get('datumIds', [])
    oldobj['old_value'] = old_corpus
    oldobj['old_auxiliary_resources'] = auxiliary_resources
    oldobj['warnings'] = warnings

    return oldobj


def my_strip(thing):
    """Safely strip `thing`.

    """

    try:
        return thing.strip()
    except:
        return thing


def process_lingsync_user(doc):
    """Convert a LingSync user document to an OLD user dict.

    """

    # Pretty-print the user document, for inspection.
    # p(doc)

    user_id = doc.get('_id')

    auxiliary_resources = {}

    # These are the LingSync user attrs that we know how to deal with for
    # to-OLD conversion.
    known_attrs = [
        '_id',
        '_rev',
        'authUrl',
        'collection',
        'gravatar',
        'id',
        'username',
        'firstname',
        'lastname',
        'description',
        'markAsNeedsToBeSaved',
        'researchInterest',
        'email',
        'subtitle',
        'affiliation'
    ]

    # Add warnings to this.
    warnings = {
        'general': [],
        'docspecific': []
    }

    for k in doc:
        if k not in known_attrs:
            warnings['docspecific'].append(u'\u2018%s\u2019 not a recognized'
                u' attribute in user %s' % (k, user_id))

    # This will be our return value.
    oldobj = {
        'originaldoc': doc,
        'lingsync_type': 'user',
        'old_resource': 'users',
        'old_value': {}, # Valuate this with `old_user`
        'old_auxiliary_resources': {}, # Valuate this with `auxiliary_resources`
        'warnings': []
    }

    # This dict will be used to create the OLD user.
    old_user = copy.deepcopy(old_schemata['user'])

    ls_username = my_strip(doc.get('username'))
    ls_firstname = my_strip(doc.get('firstname'))
    ls_lastname = my_strip(doc.get('lastname'))
    ls_description = my_strip(doc.get('description'))
    ls_markAsNeedsToBeSaved = my_strip(doc.get('markAsNeedsToBeSaved'))
    ls_researchInterest = my_strip(doc.get('researchInterest'))
    ls_email = my_strip(doc.get('email'))
    ls_subtitle = my_strip(doc.get('subtitle'))
    ls_affiliation = my_strip(doc.get('affiliation'))

    if ls_username:
        if ls_firstname:
            old_first_name = ls_firstname
        else:
            old_first_name = ls_username
        if ls_lastname:
            old_last_name = ls_lastname
        else:
            old_last_name = ls_username
        if ls_email:
            old_email = ls_email
        else:
            old_email = FAKE_EMAIL
            warnings['general'].append(u'Created a user (with username %s) with a fake'
                u' email: %s. Please fix manually, i.e., from within the'
                u' Dative/OLD interface.' % (ls_username, FAKE_EMAIL))
        old_page_content = []
        if ls_description:
            old_page_content.append(ls_description)
        if ls_researchInterest:
            old_page_content.append(u'Research interest: %s' % (
                punctuate_period_safe(ls_researchInterest),))
        if ls_affiliation:
            old_page_content.append(u'Affiliation: %s' % (
                punctuate_period_safe(ls_affiliation),))
        old_page_content = u'\n\n'.join(old_page_content).strip()
        if old_page_content:
            old_user['page_content'] = old_page_content
        old_user['username'] = ls_username
        old_user['first_name'] = old_first_name
        old_user['last_name'] = old_last_name
        old_user['email'] = old_email
        old_user['role'] = u'administrator'
    else:
        old_user = None

    oldobj['old_value'] = old_user
    oldobj['old_auxiliary_resources'] = auxiliary_resources
    oldobj['warnings'] = warnings

    return oldobj


def process_lingsync_datum(doc, collections, lingsync_db_name):
    """Process a LingSync datum. This will be encoded as an OLD form.

    """

    # Pretty-print the datum document, for inspection.
    # p(doc)

    datum_id = doc['_id']
    datum_fields = doc['datumFields']

    # These are the LingSync datum fields that we know how to deal with for
    # to-OLD conversion.
    known_fields = [
        'judgement',
        'morphemes',
        'utterance',
        'gloss',
        'translation',
        'validationStatus',
        'tags',
        'syntacticCategory',
        'syntacticTreeLatex',
        'enteredByUser', # Can contain a dict in its 'user' attribute (see below)
        'modifiedByUser', # Can contain an array in its 'users' attribute. Forget what, exactly, this array can contain.
        'comments',
        'markAsNeedsToBeSaved', # Ignoring this. Strangely, there can be multiple fields with this label in a datimFields array ...
        'checked', # Ignoring this. It can evaluate to `true`, but and may be relevant to `validationStatus` and the OLD form's `status`, but I think it's safe to ignore it.
        'notes', # non-standard but attested
        'phonetic' # non-standard but attested
    ]

    known_attrs = [
        '_id', # u'c297e5ceecafe6b340876e07ac477736',
        '_rev', # u'2-63e6d77f0e9f834000b77ff59fa7abd2',
        'audioVideo', # [],
        'collection', # u'datums',
        'comments', # [],
        'dateEntered', # u'2015-04-01T16:50:30.852Z',
        'dateModified', # u'2015-04-01T16:50:30.852Z',
        'datumFields', # []
        'datumTags', # [],
        'images', # [],
        'jsonType', # u'Datum',
        'pouchname',
        'session', # {...} redundantly stores the session of each datum ...
        'timestamp', # 1427907030852,
        'trashed', # u'deleted'
        'api', # Ignorable
        'dateCreated', # Unix timestamp; Is this different value from `dateEntered`? Doesn't really matter for this migration script.
        'dbname', # Ignorable
        'fieldDBtype', # Ignorable
        'version' # Ignorable
    ]

    # Fill this with OLD resources that are implicit in the LingSync datum.
    auxiliary_resources = {}

    # Add warnings to this.
    warnings = {
        'general': [],
        'docspecific': []
    }

    for k in doc:
        if k not in known_attrs:
            warnings['docspecific'].append(u'\u2018%s\u2019 not a recognized'
                u' attribute in datum %s' % (k, datum_id))
    for obj in datum_fields:
        if obj['label'] not in known_fields:
            warnings['docspecific'].append(u'\u2018%s\u2019 not a recognized'
                u' label in fields for datum %s' % (obj['label'], datum_id))

    # This will be our return value.
    oldobj = {
        'originaldoc': doc,
        'lingsync_type': 'datum',
        'old_resource': 'forms',
        'old_value': {}, # Valuate this with `old_form`
        'old_auxiliary_resources': {}, # Valuate this with `auxiliary_resources`
        'warnings': []
    }

    # This dict will be used to create the OLD collection.
    old_form = copy.deepcopy(old_schemata['form'])
    old_form['status'] = u'tested'

    # LingSync datum metadata, as well as truncated or invalid values, and
    # LingSync comments will all be placed in this value, which is stringified,
    # ultimately.
    old_comments = []

    # Certain LingSync values may also be made into tags.
    old_tags = []

    # NOTE: these values cannot be valuated by LingSync datum values.
    # 'elicitation_method': None, # = ValidOLDModelObject(model_name='ElicitationMethod')
    # 'syntactic_category': None, # = ValidOLDModelObject(model_name='SyntacticCategory')
    # 'source': None, # = ValidOLDModelObject(model_name='Source')

    # These values (from LingSync datum fields) are used elsewhere.
    ls_judgement = get_val_from_datum_fields('judgement', datum_fields)
    ls_morphemes = get_val_from_datum_fields('morphemes', datum_fields)
    ls_utterance = get_val_from_datum_fields('utterance', datum_fields)
    ls_gloss = get_val_from_datum_fields('gloss', datum_fields)
    ls_translation = get_val_from_datum_fields('translation', datum_fields)
    ls_validationStatus = get_val_from_datum_fields('validationStatus', datum_fields)
    ls_tags = get_val_from_datum_fields('tags', datum_fields)
    ls_syntacticTreeLatex = get_val_from_datum_fields('syntacticTreeLatex', datum_fields)
    ls_datumTags = doc.get('datumTags')
    ls_session = doc.get('session')

    # Date Elicited. Date in 'MM/DD/YYYY' format. From
    # datum.session.sessionFields.dateElicited.
    # Attempt to create a MM/DD/YYYY string from `date_session_elicited`. At
    # present, we are only recognizing date strings in MM/DD/YYYY and
    # YYYY-MM-DD formats.
    date_datum_elicited_unparseable = False
    if ls_session:
        session_fields = ls_session.get('sessionFields', [])
        date_session_elicited = get_val_from_session_fields('dateElicited', session_fields)
        if date_session_elicited:
            date_elicited = None
            try:
                datetime_inst = datetime.datetime.strptime(date_session_elicited,
                    '%Y-%m-%d')
            except Exception, e:
                try:
                    datetime_inst = datetime.datetime.strptime(
                        date_session_elicited, '%m/%d/%Y')
                except Exception, e:
                    datetime_inst = None
                    date_datum_elicited_unparseable = True
                    warnings['docspecific'].append(u'Unable to parse %s to an OLD-compatible date'
                        u' in MM/DD/YYYY format for datum %s.' % (
                        date_session_elicited, datum_id))
            if datetime_inst:
                y = datetime_inst.year
                m = datetime_inst.month
                d = datetime_inst.day
                date_elicited = u'%s/%s/%s' % (str(m).zfill(2),
                    str(d).zfill(2), str(y))
            else:
                date_elicited = None
            if date_elicited:
                old_form['date_elicited'] = date_elicited

    # Files. Array of OLD file objects. The `audioVideo` attribute holds an
    # array of objects, each of which has 'URL' and 'type' attributes. The
    # `images` attribute holds ...
    ls_audioVideo = doc.get('audioVideo')
    ls_images = doc.get('images')
    if ls_audioVideo and (type(ls_audioVideo) is type([])):
        for av in ls_audioVideo:
            if (type(av) is type({})) and av.get('URL') and \
            (av.get('trashed')  != 'deleted'):

                # We're guessing the MIME type based on the extension, not the
                # file contents, cuz we're lazy right now...
                mime_type = mimetypes.guess_type(av['URL'])[0]
                print mime_type
                if (not mime_type) or (mime_type not in old_allowed_file_types):
                    continue
                old_file = copy.deepcopy(old_schemata['file'])
                old_file['MIME_type'] = mime_type
                file_description = [(u'This file was generated from the LingSync'
                    u' audio/video file stored at %s.' % av['URL'])]
                if av.get('description'):
                    file_description.append(av['description'].strip())
                if av.get('dateCreated'):
                    file_description.append(u'This file was created on LingSync'
                        u' at %s.' % av['dateCreated'])
                old_file['description'] = u'\n\n'.join(file_description)
                if av.get('filename'):
                    old_file['filename'] = av['filename'].strip()
                # Loop through all of the A/V attributes that are "known"
                # and issue warnings when unknown ones are encountered.
                for attr in av:
                    if attr not in known_audio_video_attrs:
                        warnings['docspecific'].append(u'Attribute'
                            u' \u2018%s\u2019 is not recognized in the'
                            u' `audioVideo` value of datum %s' % (attr,
                            datum_id))
                # Store these "private" keys for possible use during file data
                # download.
                old_file['__lingsync_datum_id'] = datum_id
                old_file['__lingsync_file_url'] = av['URL']
                if av.get('size'):
                    old_file['__lingsync_file_size'] = av['size']
                # LingSync's `type` attr is OLD's MIME_type. We probably want
                # to programmatically extract this value from the filename
                # and/or the file data though.
                if av.get('type'):
                    old_file['__lingsync_MIME_type'] = av['type']
                old_form['files'].append(old_file)
                auxiliary_resources.setdefault('files', []).append(old_file)

    # Files -- Images. Add `datum.images` to `form.files`, once we know what is
    # in a LingSync datum's images attribute.
    if ls_images:
        warnings['docspecific'].append(u'Datum %s has an `images` attribute that has been'
            u' ignored.' % datum_id)

    # Tags. [] or a list of OLD tags.
    if ls_tags:
        if type(ls_tags) is type(u''):
            for tag in ls_tags.split():
                old_tag = copy.deepcopy(old_schemata['tag'])
                old_tag['name'] = tag
                old_tags.append(old_tag)
        else:
            warnings['docspecific'].append(u'Unable to use value \u2018%s\u2019'
                u' from datumField tags of datum %s' % (unicode(ls_tags),
                datum_id))
    if ls_datumTags:
        if type(ls_datumTags) is type([]):
            for tag in ls_datumTags:
                if type(tag) is type({}):
                    if tag.get('tag'):
                        old_tag = copy.deepcopy(old_schemata['tag'])
                        old_tag['name'] = tag['tag']
                        old_tags.append(old_tag)
                    else:
                        warnings['docspecific'].append(u'Tag object \u2018%s\u2019'
                            u' from datum.datumTags of datum %s has no `tag`'
                            u' attribute and cannot be used.' % (
                            unicode(tag), datum_id))
                else:
                    warnings['docspecific'].append(u'Unable to use tag \u2018%s\u2019'
                        u' from datum.datumTags of datum %s' %
                        (unicode(tag), datum_id))
        else:
            warnings['docspecific'].append(u'Unable to use value \u2018%s\u2019'
                u' from datum.datumTags of datum %s' %
                (unicode(ls_datumTags), datum_id))

    # If `ls_trashed == 'deleted'` then we mark the to-be-uploaded form as such
    # and we will delete it in the OLD after creating it.
    ls_trashed = doc.get('trashed')
    if ls_trashed == 'deleted':
        old_form['__lingsync_deleted'] = True

    # Speaker. Null or a valid speaker resource. From datum.session.consultants.
    # WARNING: it's not practical to try to perfectly parse free-form
    # consultants values.

    speakers = []
    if ls_session:
        session_fields = ls_session.get('sessionFields')
        if not session_fields:
            session_fields = ls_session.get('fields', [])
        consultants = get_val_from_session_fields('consultants', session_fields)
        dialect = get_val_from_session_fields('dialect', session_fields)
        if not dialect:
            dialect = ls_session.get('dialect')
        if consultants:
            consultants_list = consultants.split()
            # If consultants is two capitalized words, e.g., Dave Smith, then
            # we assume we have a first name/ last name situation.
            if len(consultants_list) == 2 and \
            consultants_list[0] == consultants_list[0].lower().capitalize() and \
            consultants_list[1] == consultants_list[1].lower().capitalize():
                old_speaker = copy.deepcopy(old_schemata['speaker'])
                old_speaker['first_name'] = consultants_list[0]
                old_speaker['last_name'] = consultants_list[1]
                speakers.append(old_speaker)
            # Otherwise, we assume we have an initials situation (e.g., DS).
            else:
                for consultant in consultants_list:
                    old_speaker = copy.deepcopy(old_schemata['speaker'])
                    # If consultant is all-caps, we assume it is initials, where the
                    # first char is the first name initial and the remaining char(s)
                    # is/are the last name initial(s).
                    if consultant.upper() == consultant:
                        old_speaker['first_name'] = consultant[0]
                        old_speaker['last_name'] = consultant[1:]
                    else:
                        old_speaker['first_name'] = consultant
                        old_speaker['last_name'] = consultant
                    if dialect:
                        old_speaker['dialect'] = dialect
                    speakers.append(old_speaker)

    if len(speakers) >= 1:
        old_form['speaker'] = speakers[0]
        if len(speakers) > 1:
            warnings['docspecific'].append('Datum %s has more than one'
                ' consultant listed. Since OLD forms only allow one speaker, we'
                ' are just going to associate the first speaker to the OLD form'
                ' created form this LingSync datum. The additional LingSync'
                ' speakers will still be created as OLD speakers, however, and'
                ' ALL LingSync consultants will be documented in the form\'s'
                ' comments field.' % datum_id)
            speaker_strs = [u'%s %s' % (s['first_name'], s['last_name']) for s
                in speakers]
            old_comments.append(punctuate_period_safe(
                'Consultants: %s' % u', '.join(speaker_strs)))

    for speaker in speakers:
        auxiliary_resources.setdefault('speakers', []).append(speaker)

    # Elicitor. Null or a valid user resource. From datum enteredByUser.
    ls_enteredByUser = get_val_from_datum_fields('enteredByUser', datum_fields)
    if ls_enteredByUser:
        warnings['general'].append(u'Form elicitor values are being supplied by'
            u' datum.session.enteredByUser values. This may be inaccurate. Change'
            u' as needed in the Dative/OLD interface.')
        old_elicitor = copy.deepcopy(old_schemata['user'])
        old_elicitor['username'] = ls_enteredByUser
        old_elicitor['first_name'] = ls_enteredByUser
        old_elicitor['last_name'] = ls_enteredByUser
        warnings['general'].append(u'Created a user (with username %s) with a'
            u' fake email: %s. Please fix manually, i.e., from within the'
            u' Dative/OLD interface.' % (ls_enteredByUser, FAKE_EMAIL))
        old_elicitor['email'] = FAKE_EMAIL
        old_elicitor['role'] = u'administrator'
        old_form['elicitor'] = old_elicitor
        auxiliary_resources.setdefault('users', []).append(old_elicitor)

    # Status. Must be 'tested' or 'requires testing'. LingSync's
    # validationStatus is similar. A common value is 'Checked'. It's not
    # clear which validationStatus values should cause OLD's `status` to be
    # 'requires testing'. Testing with more LingSync corpora is needed.
    if ls_validationStatus:
        if ls_validationStatus != 'Checked':
            old_tags.append(u'validation status: %s' % ls_validationStatus)
            warnings['docspecific'].append(u'Unrecognized validationStatus \u2018%s\u2019 in'
                u' datum %s' % ( ls_validationStatus, datum_id))

    # Transcription. Not empty, max 255 chars. From LingSync utterance.
    ls_utterance_too_long = False
    if not ls_utterance:
        old_transcription = u'PLACEHOLDER'
        # warnings['docspecific'].append(u'Datum %s has no utterance value; the form generated'
        #     ' from it has "PLACEHOLDER" as its transcription value.' % datum_id)
    elif len(ls_utterance) > 255:
        ls_utterance_too_long = True
        warnings['docspecific'].append('The utterance "%s" of datum %s is too long and will be'
            ' truncated.' % (ls_utterance, datum_id))
        old_transcription = ls_utterance[:255]
    else:
        old_transcription = ls_utterance
    old_form['transcription'] = old_transcription

    # Morpheme Break. Max 255 chars. From LingSync morphemes.
    ls_morphemes_too_long = False
    if ls_morphemes:
        if len(ls_morphemes) > 255:
            ls_morphemes_too_long = True
            warnings['docspecific'].append('The morphemes "%s" of datum %s is too long and'
                ' will be truncated.' % (ls_morphemes, datum_id))
            old_form['morpheme_break'] = ls_morphemes[:255]
        else:
            old_form['morpheme_break'] = ls_morphemes

    # Phonetic Transcription. Max 255 chars. From the non-standard LingSync
    # field "phonetic".
    ls_phonetic = get_val_from_datum_fields('phonetic', datum_fields)
    ls_phonetic_too_long = False
    if ls_phonetic:
        if len(ls_phonetic) > 255:
            ls_phonetic_too_long = True
            warnings['docspecific'].append('The phonetic value "%s" of datum %s'
                ' is too long and will be truncated.' % (ls_phonetic, datum_id))
            old_form['phonetic_transcription'] = ls_phonetic[:255]
        else:
            old_form['phonetic_transcription'] = ls_phonetic

    # Grammaticality. From LingSync judgement.
    if ls_judgement:
        # In some LingSync corpora, users added comments into the
        # grammaticality field. We try to detect and repair that here.
        if len(ls_judgement) > 3:
            warnings['general'].append(u'You have some grammaticality values'
                ' that contain more than three characters, suggesting that'
                ' these values are comments and not true grammaticalities. We'
                ' have tried to separate the true grammaticalities from the'
                ' comments. Search for "Comment from LingSync judgement field:"'
                ' in the comments field of forms in the resulting OLD database.')
            grammaticality_prefix = []
            for char in ls_judgement:
                if char in (u'*', u'?', u'#', u'!'):
                    grammaticality_prefix.append(char)
                else:
                    break
            old_form['grammaticality'] = u''.join(grammaticality_prefix)
            comment = punctuate_period_safe(u'Comment from LingSync judgement'
                u' field: %s' % ls_judgement)
            old_comments.append(comment)
        else:
            old_form['grammaticality'] = ls_judgement

    # Morpheme Gloss. Max 255 chars. From LingSync gloss.
    ls_gloss_too_long = False
    if ls_gloss:
        if len(ls_gloss) > 255:
            ls_gloss_too_long = True
            warnings['docspecific'].append('The gloss "%s" of datum %s is too long and'
                ' will be truncated.' % (ls_gloss, datum_id))
            old_form['morpheme_gloss'] = ls_gloss[:255]
        else:
            old_form['morpheme_gloss'] = ls_gloss

    # Translations. Has to be at least one. From LingSync translation.
    if ls_translation:
        old_translation_transcription = ls_translation
    else:
        old_translation_transcription = u'PLACEHOLDER'
        # warnings['docspecific'].append(u'Datum %s has no translation value; the form generated'
        #     ' from it has "PLACEHOLDER" as its translation transcription'
        #     ' value.' % datum_id)
    old_form['translations'] = [{
        'transcription': old_translation_transcription,
        'grammaticality': u''
    }]

    # Syntax. Max 1023 chars. From LingSync syntacticTreeLatex.
    ls_syntacticTreeLatex_too_long = False
    if ls_syntacticTreeLatex:
        if len(ls_syntacticTreeLatex) > 1023:
            ls_syntacticTreeLatex_too_long = True
            warnings['docspecific'].append('The syntacticTreeLatex "%s" of datum %s is too'
                ' long and will be truncated.' % (ls_syntacticTreeLatex, datum_id))
            old_form['syntax'] = ls_syntacticTreeLatex[:255]
        else:
            old_form['syntax'] = ls_syntacticTreeLatex


    # Comments.
    # This is a text grab-bag of data points. I use this to summarize the
    # LingSync datum that this form was derived from. Certain attributes are
    # added here only if they were found invalid or too long above.
    # All of these values should go into prose in the OLD's form.comments value.

    # Datum metadata -> form comments
    ls_modifiedByUser = get_val_from_datum_fields('modifiedByUser', datum_fields)
    ls_dateModified = doc.get('dateModified')
    ls_dateEntered = doc.get('dateEntered')
    # We remember the date entered so that we can get the correct sort order for
    # forms in collections (since datums are sorted in their sessions according
    # to date entered).
    if ls_dateEntered:
        old_form['date_entered'] = ls_dateEntered
    else:
        print '%sWarning: no date entered value.%s' % (ANSI_WARNING, ANSI_ENDC)
    old_form_creation_metadata = []
    if ls_enteredByUser and ls_dateEntered:
        old_form_creation_metadata.append(u'This form was created from LingSync'
            u' datum %s (in corpus %s), which was created by %s on %s.' % (
            datum_id, lingsync_db_name, ls_enteredByUser, ls_dateEntered))
    if ls_modifiedByUser and ls_dateModified:
        old_form_creation_metadata.append(u'The datum was last modified in'
            u' LingSync by %s on %s.' % (ls_modifiedByUser, ls_dateModified))
    if date_datum_elicited_unparseable:
        date_session_elicited
        old_form_creation_metadata.append(u'The datum was elicited on %s.' % (
            date_session_elicited))
    old_form_creation_metadata = ' '.join(old_form_creation_metadata).strip()
    if old_form_creation_metadata:
        old_comments.append(old_form_creation_metadata)

    # Datum comments field -> form comments
    ls_comments = get_val_from_datum_fields('comments', datum_fields)
    if ls_comments:
        processed_comments, warnings = process_lingsync_comments_val(
            ls_comments, warnings)
        if processed_comments:
            old_comments += processed_comments

    # Datum comments attribute -> form comments
    ls_comments_attr = doc.get('comments')
    if ls_comments_attr:
        processed_comments, warnings = process_lingsync_comments_val(
            ls_comments_attr, warnings)
        if processed_comments:
            old_comments += processed_comments

    # Datum notes field -> form comments. (Some LingSync corpora have the
    # non-standard "notes" field in their datums.)
    ls_notes = get_val_from_datum_fields('notes', datum_fields)
    if ls_notes:
        old_comments.append('LingSync notes: %s' % punctuate_period_safe(ls_notes))

    # Datum errored fields -> put them (redundantly) into a paragraph in form
    # comments.
    # The datum.syntacticCategory string can't be used to specify the OLD
    # forms' syntactic_category_string field since that field is read-only. We
    # can add it to the comments prose though.
    old_form_errored_data = []
    ls_syntacticCategory = get_val_from_datum_fields('syntacticCategory',
        datum_fields)
    if ls_utterance_too_long:
        old_form_errored_data.append(u'LingSync datum utterance value without'
            u' truncation: \u2018%s\u2019' %
            (punctuate_period_safe(ls_utterance),))
    if ls_morphemes_too_long:
        old_form_errored_data.append(u'LingSync morphemes value without'
            u' truncation: \u2018%s\u2019' % (
            punctuate_period_safe(ls_morphemes),))
    if ls_phonetic_too_long:
        old_form_errored_data.append(u'LingSync phonetic value without'
            u' truncation: \u2018%s\u2019' % (
            punctuate_period_safe(ls_phonetic),))
    if ls_gloss_too_long:
        old_form_errored_data.append(u'LingSync datum gloss value without'
            u' truncation: \u2018%s\u2019' % punctuate_period_safe(ls_gloss))
    if ls_syntacticCategory:
        old_form_errored_data.append(u'LingSync syntacticCategory value:'
            u' \u2018%s\u2019' % ( ls_syntacticCategory))
    if ls_syntacticTreeLatex_too_long:
        old_form_errored_data.append(u'LingSync datum syntacticTreeLatex value'
            u' without truncation: \u2018%s\u2019' % (
            punctuate_period_safe(ls_syntacticTreeLatex),))
    old_form_errored_data = ' '.join(old_form_errored_data).strip()
    if old_form_errored_data:
        old_comments.append(old_form_errored_data)

    old_comments = u'\n\n'.join(old_comments).strip()
    if old_comments:
        old_form['comments'] = old_comments

    # Process accumulated tags. They can come from various datum values, so we
    # add them to the old_form at the end.
    old_form['tags'] = old_tags
    for tag in old_tags:
        auxiliary_resources.setdefault('tags', []).append(tag)

    if ls_session:
        session_id = ls_session['_id']
        old_form['__lingsync_session_id'] = session_id
    else:
        print '%sWarning: no LingSync session for datum %s.%s' % (
            ANSI_WARNING, datum_id, ANSI_ENDC)
    old_form['__lingsync_datum_id'] = datum_id
    oldobj['old_value'] = old_form
    oldobj['old_auxiliary_resources'] = auxiliary_resources
    oldobj['warnings'] = warnings

    return oldobj


# These are the attributes of a LingSync Datum's AudioVideo object attribute
# that we know about.
known_audio_video_attrs = [
    '_id',
    'dateCreated',
    'URL',
    'api',
    'checksum',
    'dbname',
    'description',
    'fieldDBtype',
    'fileBaseName',
    'filename',
    'mtime',
    'name',
    'pouchname',
    'praatAudioExtension',
    'resultInfo',
    'resultStatus',
    'script',
    'serviceVersion',
    'size',
    'syllablesAndUtterances',
    'textGridInfo',
    'textGridStatus',
    'textgrid',
    'trashed',
    'type',
    'uploadInfo',
    'version',
    'webResultInfo',
    'webResultStatus'
]


def process_lingsync_session(doc):
    """Process a LingSync session. This will be encoded as an OLD collection.

    Note: the LingSync session fields/attributes dateSEntered and other
    datetime/timestamp entered/modified values are *not* migrated. The OLD
    has its own creation/modification timestamps and these values cannot be
    user-specified.

    """

    # Pretty-print the session document, for inspection.
    # p(doc)

    session_id = doc['_id']
    session_fields = doc.get('sessionFields')
    if not session_fields:
        session_fields = doc.get('fields')
    if not session_fields:
        print (u'ERROR: unable to find `sessionFields` or `fields` attr in'
            u' session:')
        p(doc)
        return None

    # If a session is marked as deleted, we don't add it to the OLD.
    if doc.get('trashed') == 'deleted':
        return None

    # These are the LingSync session fields that we know how to deal with for
    # to-OLD conversion.
    known_fields = [
        'goal',
        'consultants',
        'dialect',
        'language',
        'dateElicited',
        'user',
        'dateSEntered',
        'participants', # Sometimes a field with this label. I'm ignoring it. It seems to consitently be an empty string.
        'DateSessionEntered', # Sometimes a field with this label. I'm ignoring it. It seems to consistently be an empty string.
        'dateSessionEntered' # Sometimes a field with this label. I'm ignoring it. It seems to be the same date as the date_created datetime, just in a different format.
    ]

    known_attrs = [
        '_id',
        '_rev',
        'collection',
        'comments',
        'dateCreated',
        'dateModified',
        'lastModifiedBy',
        'pouchname',
        'sessionFields',
        'title', # This attr occurs in some sessions. I am ignoring this attr in sessions; I think it holds 'Change this session'.
        'timestamp', # This attr also occurs only sometimes. I am ignoring it. It appears to be the same value as the dateModified.
        'api', # Ignorable
        'dbname', # Ignorable
        'fieldDBtype', # Ignorable
        'fields', # Ignorable
        'modifiedByUser', # NOTE: this should maybe be migrated, but its `value` value is just a string of usernames and its `json.users` value is an array of objects whose only relevant attribute appears to be `username`, which is redundant with the aforementioned `value`. No modification timestamp for each modification.
        'version', # Ignorable
        'dialect', # Note: this attr appears to be valuated when its corresponding field is not, and vice versa.
        'language', # Note: this attr appears to be valuated when its corresponding field is not, and vice versa.
        'trashed', # May be set to 'deleted'. Note: we are ignoring deleted sessions and will not create and then delete OLD corpora to simulate them (as we do with forms).
        'trashedReason' # Optional text describing why the session was deleted.
    ]

    # Fill this with OLD resources that are implicit in the LingSync session.
    auxiliary_resources = {}

    # Add warnings to this.
    warnings = {
        'general': [],
        'docspecific': []
    }

    for k in doc:
        if k not in known_attrs:
            warnings['docspecific'].append(u'\u2018%s\u2019 not a recognized'
                u' attribute in session %s' % (k, session_id))
    for obj in session_fields:
        if obj['label'] not in known_fields:
            warnings['docspecific'].append(u'\u2018%s\u2019 not a recognized'
                u' label in fields for session %s' % (obj['label'], session_id))

    # This will be our return value.
    oldobj = {
        'originaldoc': doc,
        'lingsync_type': 'session',
        'old_resource': 'collections',
        'old_value': {}, # Valuate this with `old_collection`
        'old_auxiliary_resources': [], # Valuate this with `auxiliary_resources`
        'warnings': []
    }

    # This dict will be used to create th eOLD collection.
    old_collection = copy.deepcopy(old_schemata['collection'])
    old_collection['type'] = u'elicitation'

    # Get the values of the LingSync session fields.
    goal = get_val_from_session_fields('goal', session_fields)
    consultants = get_val_from_session_fields('consultants', session_fields)
    date_session_elicited = get_val_from_session_fields('dateElicited',
        session_fields)
    user = get_val_from_session_fields('user', session_fields)
    date_created = doc.get('dateCreated')
    date_modified = doc.get('dateModified')
    last_modified_by = doc.get('lastModifiedBy')

    # We use the dialect and language fields if present. If not, we try to get
    # these values from the corresponding attributes.
    dialect = get_val_from_session_fields('dialect', session_fields)
    if not dialect:
        dialect = doc.get('dialect')
    language = get_val_from_session_fields('language', session_fields)
    if not language:
        language = doc.get('language')

    # Title. Get the OLD collection's title value.
    if (not goal) or len(goal) == 0:
        warnings['docspecific'].append('Session %s has no goal so its date elicited is being'
            ' used for title of the the OLD collection built from it.' % (
            session_id,))
        if date_session_elicited and len(date_session_elicited) > 0:
            title = u'Elicitation Session on %s' % date_session_elicited
        else:
            warnings['docspecific'].append('Session %s has no date elicited so its id is being'
                ' used for the title of the OLD collection built from it.' % (
                session_id,))
            title = u'Elicitation Session %s' % session_id
    elif len(goal) > 255:
        warnings['docspecific'].append('The goal "%s" of session %s is too long and will be'
            ' truncated.' % (goal, session_id))
        title = goal[:255]
    else:
        title = goal
    old_collection['title'] = title

    # Description.
    # Get the OLD collection's description value. This will contain most of the
    # metadata from the LingSync session. It's redundant, but it's informative,
    # so that's fine.
    description = []
    description.append(u'This collection was created from a LingSync session with id'
        ' %s.' % session_id)
    if goal:
        description.append(u'Goal: %s' % punctuate_period_safe(goal))
    if consultants:
        description.append(u'Consultants: %s' % (
            punctuate_period_safe(consultants),))
    if language:
        description.append(u'Language: %s' % (
            punctuate_period_safe(language),))
    if dialect:
        description.append(u'Dialect: %s' % (
            punctuate_period_safe(dialect),))
    if date_session_elicited:
        description.append(u'Elicitation session date: %s' % (
            punctuate_period_safe(date_session_elicited),))

    creation_metadata = []
    if user and date_created:
        creation_metadata.append(u'Session created in LingSync by %s on %s.' % (
            user, date_created))
    if last_modified_by and date_modified:
        creation_metadata.append(u'Session last modified in LingSync by %s on'
            ' %s.' % (last_modified_by, date_modified))
    creation_metadata = ' '.join(creation_metadata).strip()
    if creation_metadata:
        description.append(creation_metadata)

    ls_comments = doc.get('comments')
    if ls_comments:
        comments_string = lingsync_comments2old_description(doc['comments'])
        if comments_string:
            description.append(comments_string)
        description = u'\n\n'.join(description)
        old_collection['description'] = description

    # Speaker.
    # Use the LingSync `consultants` and `dialect` fields to create one or more
    # OLD speakers.
    speakers = []
    if consultants:
        for consultant in consultants.split():
            old_speaker = copy.deepcopy(old_schemata['speaker'])
            # If consultant is all-caps, we assume it is initials wehre the
            # first char is the first name initial and the remaining char(s)
            # is/are the last name initial(s).
            if consultant.upper() == consultant:
                old_speaker['first_name'] = consultant[0]
                old_speaker['last_name'] = consultant[1:]
            else:
                old_speaker['first_name'] = consultant
                old_speaker['last_name'] = consultant
            if dialect:
                old_speaker['dialect'] = dialect
            speakers.append(old_speaker)
    if len(speakers) >= 1:
        old_collection['speaker'] = speakers[0]
        if len(speakers) > 1:
            warnings['docspecific'].append('Session %s has more than one consultant listed. Since'
                ' OLD collections only allow one speaker, we are just going to'
                ' associate the first speaker to the OLD collection created form this'
                ' LingSync session. The additional LingSync speakers will still be'
                ' created as OLD speakers, however.' % session_id)
    for speaker in speakers:
        auxiliary_resources.setdefault('speakers', []).append(speaker)

    # Elicitor.
    # Use the LingSync session's user to valuate the OLD collection's elicitor.
    # We stupidly just set the username, first_name, and lastname attributes to
    # the LingSync user value.
    if user:
        old_elicitor = copy.deepcopy(old_schemata['user'])
        old_elicitor['username'] = user
        old_elicitor['first_name'] = user
        old_elicitor['last_name'] = user
        old_elicitor['email'] = FAKE_EMAIL
        old_elicitor['role'] = u'administrator'
        old_collection['elicitor'] = old_elicitor
        auxiliary_resources.setdefault('users', []).append(old_elicitor)

    # Date elicited.
    # Attempt to create a MM/DD/YYYY string from `date_session_elicited`. At
    # present, we are only recognizing date strings in MM/DD/YYYY and
    # YYYY-MM-DD formats.
    if date_session_elicited:
        date_elicited = None
        try:
            datetime_inst = datetime.datetime.strptime(date_session_elicited,
                '%Y-%m-%d')
        except Exception, e:
            try:
                datetime_inst = datetime.datetime.strptime(
                    date_session_elicited, '%m/%d/%Y')
            except Exception, e:
                datetime_inst = None
                # No point in warning about this. Any unparseable dateElicited
                # values will be put in the text of the description anyway.
                # warnings['docspecific'].append(u'Unable to parse %s to an'
                #     u' OLD-compatible date in MM/DD/YYYY format.' % (
                #     date_session_elicited,))
        if datetime_inst:
            y = datetime_inst.year
            m = datetime_inst.month
            d = datetime_inst.day
            date_elicited = u'%s/%s/%s' % (str(m).zfill(2),
                str(d).zfill(2), str(y))
        else:
            date_elicited = None
        if date_elicited:
            old_collection['date_elicited'] = date_elicited

    old_collection['__lingsync_session_id'] = session_id
    oldobj['old_value'] = old_collection
    oldobj['old_auxiliary_resources'] = auxiliary_resources
    oldobj['warnings'] = warnings
    if language:
        oldobj['language'] = language

    # return (old_collection, auxiliary_resources, warnings)
    return oldobj


def punctuate_period_safe(string):
    """Add a period at the end of `string` if no sentence-final punctuation is
    there already.

    """

    if string[-1] in ['?', '.', '!']:
        return string
    else:
        return '%s.' % string


def get_dict_from_session_fields(attr, session_fields):
    """Given a list of dicts (`session_fields`), return the first one whose
    'label' value is `attr`.

    """

    val_list = [f for f in session_fields if f['label'] == attr]
    if len(val_list) is 0:
        return None
    elif len(val_list) is 1:
        return val_list[0]
    else:
        print 'WARNING: more than one %s in field list!' % attr
        return val_list[0]


def get_val_from_session_fields(attr, session_fields):
    """Given a list of dicts (`session_fields`), return the first one whose
    'label' value is `attr` and return its 'value' value.

    """

    val_dict = get_dict_from_session_fields(attr, session_fields)
    if val_dict:
        return val_dict.get('value')
    else:
        return val_dict


def get_dict_from_datum_fields(attr, datum_fields):
    """Given a list of dicts (`datum_fields`), return the first one whose
    'label' value is `attr`.

    """

    val_list = [f for f in datum_fields if f['label'] == attr]
    if len(val_list) is 0:
        return None
    elif len(val_list) is 1:
        return val_list[0]
    else:
        print 'WARNING: more than one %s in field list!' % attr
        p(val_list)
        return val_list[0]


def get_val_from_datum_fields(attr, datum_fields):
    """Given a list of dicts (`datum_fields`), return the first one whose
    'label' value is `attr` and return its 'value' value.

    """

    val_dict = get_dict_from_datum_fields(attr, datum_fields)
    if val_dict:
        return val_dict.get('value')
    else:
        return val_dict


def lingsync_comments2old_description(comments_list):
    """Return a LingSync session comments array as a string of text that can be
    put into the description of an OLD collection. Each comment should be its
    own paragraph.

    """

    if len(comments_list) > 0:
        print 'We have comments in this session!'
        p(comments_list)
    return ''


def main():
    """This function performs the conversion.

    """

    options, lingsync_config, lingsync_db_name = get_params()
    lingsync_data_fname = download(options, lingsync_config, lingsync_db_name)
    old_data_fname = convert(options, lingsync_data_fname, lingsync_db_name)
    upload(options, old_data_fname)
    cleanup()


def createdirs():
    """Create the needed directories in the current folder.

    """

    for dirpath in [LINGSYNC_DIR, OLD_DIR, FILES_DIR]:
        if not os.path.isdir(dirpath):
            os.makedirs(dirpath)

def cleanup():
    """If the user doesn't dissent, destroy the local directories used to hold
    the LingSync JSON downloaded, the OLD JSON created by the conversion, and
    the media files downloaded from LingSync.

    """

    r = raw_input('Save migration files? Enter \'y\'/\'Y\' to save the JSON'
        u' from the LingSync download and the OLD-compatible JSON from the OLD'
        u' download. Otherwise, these files will be destroyed.')
    if r not in ['y', 'Y']:
        for dirpath in [LINGSYNC_DIR, OLD_DIR, FILES_DIR]:
            if os.path.isdir(dirpath):
                shutil.rmtree(dirpath)


def get_params():
    """Get the parameters and options entered at the command line.
    Return them in their raw form, as well as a config dict that
    `FieldDBClient` can init on and the name of the LingSync corpus.

    """

    createdirs()
    usage = "usage: ./%prog [options]"
    parser = optparse.OptionParser(usage)
    add_optparser_options(parser)
    (options, args) = parser.parse_args()
    lingsync_url = getattr(options, 'ls_url', None)
    lingsync_db_name = lingsync_corpus = getattr(options, 'ls_corpus', None)
    lingsync_username = getattr(options, 'ls_username', None)
    lingsync_password = getattr(options, 'ls_password', None)
    old_url = getattr(options, 'old_url', None)
    old_username = getattr(options, 'old_username', None)
    old_password = getattr(options, 'old_password', None)

    # If the required params haven't been supplied as options, we prompt the
    # user for them.
    if len(filter(None, [lingsync_url, lingsync_corpus, lingsync_username,
        lingsync_password, old_url, old_username, old_password])) < 7:

        if not lingsync_db_name:
            lingsync_db_name = lingsync_corpus = raw_input(u'%sPlease enter the'
                u' name of the LingSync corpus to migrate:%s ' % (ANSI_WARNING,
                ANSI_ENDC))
            if lingsync_corpus:
                options.ls_corpus = lingsync_corpus
            else:
                sys.exit(u'%sYou must provide a LingSync corpus name.'
                    u' Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

        if not lingsync_username:
            lingsync_username = raw_input(u'%sPlease enter the username of a'
                u' LingSync user with sufficient privileges to fetch data from'
                u' corpus %s:%s ' % (ANSI_WARNING, lingsync_corpus, ANSI_ENDC))
            if lingsync_username:
                options.ls_username = lingsync_username
            else:
                sys.exit(u'%sYou must provide a LingSync username. Aborting.%s'
                    % (ANSI_FAIL, ANSI_ENDC))

        if not lingsync_password:
            lingsync_password = getpass.getpass(u'%sPlease enter the password'
                u' for LingSync user %s:%s ' % (ANSI_WARNING, lingsync_username,
                ANSI_ENDC))
            if lingsync_password:
                options.ls_password = lingsync_password
            else:
                sys.exit(u'%sYou must provide the password for your LingSync'
                    u' user. Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

        if not old_url:
            old_url = raw_input(u'%sPlease enter the URL of the destination'
                u' OLD:%s ' % (ANSI_WARNING, ANSI_ENDC))
            if old_url:
                options.old_url = old_url
            else:
                sys.exit(u'%sYou must provide a destination OLD URL.'
                    u' Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

        if not old_username:
            old_username = raw_input(u'%sPlease enter the username of an'
                u' OLD user with sufficient privileges to fetch, add, update'
                u' and delete data from the OLD at %s:%s ' % (ANSI_WARNING,
                old_url, ANSI_ENDC))
            if old_username:
                options.old_username = old_username
            else:
                sys.exit(u'%sYou must provide an OLD username. Aborting.%s'
                    % (ANSI_FAIL, ANSI_ENDC))

        if not old_password:
            old_password = getpass.getpass(u'%sPlease enter the password for'
                u' OLD user %s:%s ' % (ANSI_WARNING, old_username,
                ANSI_ENDC))
            if old_password:
                options.old_password = old_password
            else:
                sys.exit(u'%sYou must provide the password for your OLD user.'
                    u' Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

    # The FieldDBClient class constructor requires a strange dict param where
    # the URLs are split into scheme, host and port.
    parsed_lingsync_url = urlparse.urlparse(lingsync_url)
    parsed_old_url = urlparse.urlparse(old_url)
    lingsync_config = {
        "auth_protocol": parsed_lingsync_url.scheme,
        "auth_host": parsed_lingsync_url.hostname,
        "auth_port": parsed_lingsync_url.port,
        "corpus_protocol": parsed_lingsync_url.scheme,
        "corpus_host": parsed_lingsync_url.hostname,
        "corpus_port": parsed_lingsync_url.port,
        "couch_protocol": parsed_lingsync_url.scheme,
        "couch_host": parsed_lingsync_url.hostname,
        "couch_port": parsed_lingsync_url.port,
        "username": lingsync_username,
        "password": lingsync_password,
        "admin_username": lingsync_username,
        "admin_password": lingsync_password,
        "server_code": "local",
        "app_version_when_created": "unknown"
    }
    print '\n%sLingSync-to-OLD Migration.%s' % (ANSI_HEADER, ANSI_ENDC)
    print (u'We are going to move the data in the LingSync corpus %s at %s to'
        u' an OLD database at %s' % (lingsync_db_name, lingsync_url, old_url))
    return (options, lingsync_config, lingsync_db_name)


def download(options, lingsync_config, lingsync_db_name):
    """Step 1: Download the LingSync JSON data, if necessary, and set
    `lingsync_data_fname` to the path to the JSON file where those data
    are stored locally (after the fetch).

    """

    print '\n%sStep 1. Download the LingSync data.%s' % (ANSI_HEADER, ANSI_ENDC)
    if options.force_download:
        flush('Downloading the LingSync data...')
        lingsync_data_fname = download_lingsync_json(lingsync_config,
            lingsync_db_name)
    else:
        lingsync_data_fname = get_lingsync_json_filename(lingsync_db_name)
        if os.path.isfile(lingsync_data_fname):
            print 'We already have the LingSync data in %s.' % (
                lingsync_data_fname,)
        else:
            print ('The LingSync data have not been downloaded; downloading them'
                u' now')
            lingsync_data_fname = download_lingsync_json(lingsync_config,
                lingsync_db_name)
    if lingsync_data_fname is None:
        sys.exit('Unable to download the LingSync JSON data.\nAborting.')
    return lingsync_data_fname


def convert(options, lingsync_data_fname, lingsync_db_name):
    """Step 2: Process the LingSync JSON data and write a new JSON file that
    holds an OLD-compatible data structure.

    """

    print ('\n%sStep 2. Convert the LingSync data to an OLD-compatible'
        u' structure.%s' % (ANSI_HEADER, ANSI_ENDC))
    if options.force_convert:
        flush('Converting the LingSync data to an OLD-compatible format...')
        old_data_fname = lingsync2old(lingsync_data_fname, lingsync_db_name,
            options.force_file_download)
    else:
        old_data_fname = get_old_json_filename(lingsync_db_name)
        if os.path.isfile(old_data_fname):
            print 'We already have the converted OLD data in %s.' % (
                old_data_fname,)
            if options.verbose:
                path = os.path.join(LINGSYNC_DIR,
                    '%s-summary.txt' % lingsync_db_name)
                if os.path.isfile(path):
                    print open(path).read()
                path = os.path.join(OLD_DIR,
                    '%s-summary.txt' % lingsync_db_name)
                if os.path.isfile(path):
                    print open(path).read()
                path = os.path.join(OLD_DIR,
                    '%s-conversion-warnings.txt' % lingsync_db_name)
                if os.path.isfile(path):
                    print open(path).read()
        else:
            print ('The LingSync data have not yet been converted; doing that'
                u' now.')
            old_data_fname = lingsync2old(lingsync_data_fname,
                lingsync_db_name, options.force_file_download)
    if old_data_fname is None:
        sys.exit('Unable to convert the LingSync JSON data to an OLD-compatible'
            ' format.\nAborting.')
    return old_data_fname


def create_old_application_settings(old_data, c):
    """Create the application settings in `old_data` on the OLD that the client
    `c` is connected to. Return the `relational_map`.

    """

    appsett = old_data['applicationsettings'][0]
    # Only set new grammaticalities if the existing grammaticalities doesn't
    # contain all of the grammaticality values we need.
    existing_appsett = c.get('applicationsettings')[-1]
    existing_grammaticalities = existing_appsett\
        .get('grammaticalities', u'').split(u',')
    to_add_grammaticalities = appsett.get('grammaticalities', u'').split(u',')
    if set(to_add_grammaticalities).issubset(set(existing_grammaticalities)):
        existing_appsett['grammaticalities'] = \
            u','.join(existing_grammaticalities)
    else:
        existing_appsett['grammaticalities'] = \
            u','.join(to_add_grammaticalities)
    existing_appsett['object_language_name'] = appsett['object_language_name']
    r = c.create('applicationsettings', existing_appsett)
    try:
        assert r['object_language_name'] == appsett['object_language_name']
        print 'Created the OLD application settings.'
    except:
        print r
        sys.exit(u'%sSomething went wrong when attempting to create an OLD'
            u' application settings using the LingSync data. Aborting.%s' % (
            ANSI_FAIL, ANSI_ENDC))


def upload(options, old_data_fname):
    """Step 3: Upload the generated OLD data to the OLD at the specified URL.

    Sub-steps:

    1. Create application settings.
    2. Create users, speakers and tags.
    3. Create files.
    4. Create forms.
    5. Create corpora and collections.

    """

    # Keys will be OLD resource names. Values will be dicts that map LingSync
    # identifiers (i.e., ids, usernames, tagnames) to OLD identifiers (ids).
    relational_map = {}

    print (u'\n%sStep 3. Upload the converted data to the OLD web service.%s' % (
        ANSI_HEADER, ANSI_ENDC))

    # Get converted JSON data.
    try:
        old_data = json.load(open(old_data_fname))
    except:
        sys.exit(u'%sUnable to locate file %s. Aborting.%s' % (ANSI_FAIL,
            old_data_fname, ANSI_ENDC))

    # Get an OLD client.
    old_url = getattr(options, 'old_url', None)
    old_username = getattr(options, 'old_username', None)
    old_password = getattr(options, 'old_password', None)
    lingsync_corpus_name = getattr(options, 'ls_corpus', None)
    c = OLDClient(old_url)

    # Log in to the OLD.
    logged_in = c.login(old_username, old_password)
    if not logged_in:
        sys.exit(u'%sUnable to log in to %s with username %s and password %s.'
            u' Aborting.%s' % (ANSI_FAIL, old_url, old_username, old_password,
            ANSI_ENDC))

    # Create the resources.
    create_old_application_settings(old_data, c)
    relational_map, users_created = create_old_users(old_data, c, old_url,
        relational_map)
    relational_map, speakers_created = create_old_speakers(old_data, c,
        old_url, relational_map)
    relational_map, tags_created = create_old_tags(old_data, c, old_url,
        lingsync_corpus_name, relational_map)
    relational_map, files_created = create_old_files(old_data, c, old_url,
        relational_map)
    relational_map, forms_created = create_old_forms(old_data, c, old_url,
        relational_map)
    relational_map, corpora_created = create_old_corpora(old_data, c, old_url,
        relational_map)
    relational_map, collections_created = create_old_collections(old_data, c,
        old_url, relational_map)

    # Alert the user about the results of the upload.
    print u'\n%sSummary.%s' % (ANSI_HEADER, ANSI_ENDC)
    if users_created.get('created'):
        c = len(users_created['created'])
        print u'%d OLD %s created.' % (c, pluralize_by_count('user', c))
        print (u'%sAll created OLD users are administrators and have the'
            u' password \u2018%s\u2019; some may also have the fake email'
            u' address %s.%s' % (ANSI_WARNING, DEFAULT_PASSWORD, FAKE_EMAIL,
            ANSI_ENDC))
    if users_created.get('updated'):
        c = len(users_created['updated'])
        print (u'%d pre-existing and LingSync-matching OLD %s updated or'
            u' left unaltered.' % (c, pluralize_by_count('user', c)))
    if speakers_created.get('created'):
        c = len(speakers_created['created'])
        print u'%d OLD %s created.' % (c, pluralize_by_count('speaker', c))
    if speakers_created.get('updated'):
        c = len(speakers_created['updated'])
        print (u'%d pre-existing and LingSync-matching OLD %s updated or'
            u' left unaltered.' % (c, pluralize_by_count('speaker', c)))
    if tags_created:
        c = len(tags_created)
        print u'%d OLD %s created.' % (c, pluralize_by_count('tag', c))
    if files_created:
        c = len(files_created)
        print u'%d OLD %s created.' % (c, pluralize_by_count('file', c))
    if forms_created.get('created'):
        c = len(forms_created['created'])
        print u'%d OLD %s created.' % (c, pluralize_by_count('form', c))
    if forms_created.get('deleted'):
        c = len(forms_created['deleted'])
        print (u'%d OLD %s created and then deleted (to simulate trashed'
            u' LingSync forms).' % (c, pluralize_by_count('form', c)))
    if corpora_created:
        c = len(corpora_created)
        print u'%d OLD %s created.' % (c, pluralize_by_count('corpus', c))
    if collections_created:
        c = len(collections_created)
        print u'%d OLD %s created.' % (c, pluralize_by_count('collection', c))


def pluralize_by_count(noun, count):
    """Pluralize string `noun`, depending on the number of them (`count`).

    """

    if count == 1:
        return noun
    else:
        return pluralize(noun)


def pluralize(noun):
    """Pluralize `noun`: extremely domain-specific.

    """

    if noun.endswith('pus'):
        return u'%sora' % noun[:-2]
    else:
        return u'%ss' % noun


def create_old_collections(old_data, c, old_url, relational_map):
    """Create the collections in `old_data` on the OLD that the client `c` is
    connected to.

    """

    resources_created = []

    if old_data.get('collections'):
        flush('Creating OLD collections...')
        relational_map.setdefault('collections', {})

        # Get the "migration tag" id.
        migration_tag_id = relational_map.get('tags',
            {}).get(migration_tag_name)
        if not migration_tag_id:
            sys.exit(u'%sFailed to get the OLD id for the migration tag.'
                u' Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

        # Issue the create (POST) requests.
        for collection in old_data['collections']:

            session_id = collection.get('__lingsync_session_id')

            # Convert arrays of tag objects to arrays of OLD tag ids.
            if collection.get('tags'):
                new_tags = []
                for tag in collection['tags']:
                    tag_id = relational_map.get('tags', {}).get(tag['name'])
                    if tag_id:
                        new_tags.append(tag_id)
                    else:
                        print (u'%sWarning: unable to find id for OLD tag'
                            u' "%s".%s' % (ANSI_WARNING, tag['name'],
                            ANSI_ENDC))
                collection['tags'] = new_tags

            # Convert speaker objects to OLD speaker ids.
            if collection.get('speaker'):
                speakerobj = collection['speaker']
                key = u'%s %s' % (speakerobj['first_name'],
                    speakerobj['last_name'])
                speaker_id = relational_map.get('speakers', {}).get(key)
                if speaker_id:
                    collection['speaker'] = speaker_id
                else:
                    collection['speaker'] = None
                    print (u'%sWarning: unable to find id for OLD speaker'
                        u' "%s".%s' % (ANSI_WARNING, key, ANSI_ENDC))

            # Convert elicitor objects to OLD elicitor ids.
            if collection.get('elicitor'):
                elicitorobj = collection['elicitor']
                key = elicitorobj['username']
                elicitor_id = relational_map.get('users', {}).get(key)
                if elicitor_id:
                    collection['elicitor'] = elicitor_id
                else:
                    collection['elicitor'] = None
                    print (u'%sWarning: unable to find id for OLD elicitor'
                        u' "%s".%s' % (ANSI_WARNING, key, ANSI_ENDC))

            # Get the `contents` value as a bunch of references to form ids.
            # TODO: something is going wrong here. Some OLD collections are
            # being created without any forms in them even though the LingSync
            # sessions that they are derived from do have datums in them.
            contents = []
            for form in old_data.get('forms', []):
                if not form.get('__lingsync_deleted'):
                    form_s_id = form.get('__lingsync_session_id')
                    form_d_id = form['__lingsync_datum_id']
                    if form_s_id == session_id:
                        form_id = relational_map.get('forms', {}).get(form_d_id)
                        if form_id:
                            contents.append((form['date_entered'], form_id))
                        else:
                            print (u'%sWarning: unable to find id for OLD form'
                                u' generated from LingSync datum %s.%s' % (
                                ANSI_WARNING, form['__lingsync_datum_id'],
                                ANSI_ENDC))
            if not contents:
                print '%sWARNING: collection "%s" has no contents.%s' % (
                    ANSI_WARNING, collection['title'], ANSI_ENDC)
            collection['contents'] = u'\n'.join([u'form[%d]' % t[1] for t in
                sorted(contents)])

            # Create the collection on the OLD
            collection['tags'].append(migration_tag_id)
            r = c.create('collections', collection)
            try:
                assert r.get('id')
                relational_map['collections'][session_id] = r['id']
                resources_created.append(r['id'])
            except:
                p(r)
                sys.exit(u'%sFailed to create an OLD collection for the LingSync'
                    u' session \u2018%s\u2019. Aborting.%s' % (ANSI_FAIL,
                    session_id, ANSI_ENDC))

        print 'Done.'

    return (relational_map, resources_created)


def create_old_corpora(old_data, c, old_url, relational_map):
    """Create the corpora in `old_data` on the OLD that the client `c` is
    connected to.

    """

    resources_created = []

    if old_data.get('corpora'):
        flush('Creating OLD corpora...')
        relational_map.setdefault('corpora', {})

        # Get the "migration tag" id.
        migration_tag_id = relational_map.get('tags',
            {}).get(migration_tag_name)
        if not migration_tag_id:
            sys.exit(u'%sFailed to get the OLD id for the migration tag.'
                u' Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

        # Issue the create (POST) requests.
        for corpus in old_data['corpora']:

            datalist_id = corpus.get('__lingsync_datalist_id')
            datum_ids_array = corpus.get('__lingsync_datalist_datum_ids', [])

            # Convert arrays of tag objects to arrays of OLD tag ids.
            if corpus.get('tags'):
                new_tags = []
                for tag in corpus['tags']:
                    tag_id = relational_map.get('tags', {}).get(tag['name'])
                    if tag_id:
                        new_tags.append(tag_id)
                    else:
                        print (u'%sWarning: unable to find id for OLD tag'
                            u' "%s".%s' % (ANSI_WARNING, tag['name'],
                            ANSI_ENDC))
                corpus['tags'] = new_tags

            # Get the `content` value as a comma-delimited list of form ids.
            content = []
            for d_id in datum_ids_array:
                f_id = relational_map.get('forms', {}).get(d_id)
                if f_id:
                    content.append(f_id)
                else:
                    print (u'%sWarning: unable to find OLD form id'
                        u' corresponding to LingSync datum %s. Corpus %s will not'
                        u' contain all of the data that it did as a datalist in'
                        u' LingSync.%s' % (ANSI_WARNING, d_id, corpus['name'],
                        ANSI_ENDC))
            corpus['content'] = u', '.join([unicode(id) for id in content])

            # Create the corpus on the OLD
            corpus['tags'].append(migration_tag_id)
            r = c.create('corpora', corpus)
            try:
                assert r.get('id')
                relational_map['corpora'][datalist_id] = r['id']
                resources_created.append(r['id'])
            except:
                p(r)
                sys.exit(u'%sFailed to create an OLD corpus for the LingSync'
                    u' datalist \u2018%s\u2019. Aborting.%s' % (ANSI_FAIL,
                    datalist_id, ANSI_ENDC))

        print 'Done.'

    return (relational_map, resources_created)


def create_old_forms(old_data, c, old_url, relational_map):
    """Create the forms in `old_data` on the OLD that the client `c` is
    connected to.

    """

    resources_created = {
        'created': [],
        'deleted': [],
    }

    if old_data.get('forms'):
        flush('Creating OLD forms...')
        relational_map.setdefault('forms', {})

        # Get the "migration tag" id.
        migration_tag_id = relational_map.get('tags',
            {}).get(migration_tag_name)
        if not migration_tag_id:
            sys.exit(u'%sFailed to get the OLD id for the migration tag.'
                u' Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

        # Issue the create (POST) requests.
        for form in old_data['forms']:

            datum_id = form.get('__lingsync_datum_id')

            # Convert arrays of tag objects to arrays of OLD tag ids.
            if form.get('tags'):
                new_tags = []
                for tag in form['tags']:
                    tag_id = relational_map.get('tags', {}).get(tag['name'])
                    if tag_id:
                        new_tags.append(tag_id)
                    else:
                        print (u'%sWarning: unable to find id for OLD tag'
                            u' "%s".%s' % (ANSI_WARNING, tag['name'],
                            ANSI_ENDC))
                form['tags'] = new_tags

            # Convert speaker objects to OLD speaker ids.
            if form.get('speaker'):
                speakerobj = form['speaker']
                key = u'%s %s' % (speakerobj['first_name'],
                    speakerobj['last_name'])
                speaker_id = relational_map.get('speakers', {}).get(key)
                if speaker_id:
                    form['speaker'] = speaker_id
                else:
                    form['speaker'] = None
                    print (u'%sWarning: unable to find id for OLD speaker'
                        u' "%s".%s' % (ANSI_WARNING, key, ANSI_ENDC))

            # Convert elicitor objects to OLD elicitor ids.
            if form.get('elicitor'):
                elicitorobj = form['elicitor']
                key = elicitorobj['username']
                elicitor_id = relational_map.get('users', {}).get(key)
                if elicitor_id:
                    form['elicitor'] = elicitor_id
                else:
                    form['elicitor'] = None
                    print (u'%sWarning: unable to find id for OLD elicitor'
                        u' "%s".%s' % (ANSI_WARNING, key, ANSI_ENDC))

            # Convert arrays of file objects to arrays of OLD file ids.
            if form.get('files'):
                file_id_array = relational_map.get('files', {}).get(datum_id)
                if file_id_array and (type(file_id_array) is type([])):
                    form['files'] = file_id_array
                else:
                    form['files'] = []
                    print (u'%sWarning: unable to get the array of OLD file ids'
                        u' for the OLD form generated from the LingSync datum'
                        u' with id %s.%s' % (ANSI_WARNING, datum_id, ANSI_ENDC))


            # Create the form on the OLD
            form['tags'].append(migration_tag_id)
            try:
                r = c.create('forms', form)
            except requests.exceptions.SSLError:
                print ('%sWarning: SSLError; probably'
                    ' CERTIFICATE_VERIFY_FAILED.%s' % (ANSI_WARNING, ANSI_ENDC))
                r = c.create('forms', form, False)
            try:
                assert r.get('id')
                resources_created['created'].append(r['id'])
                # We don't want to map datum ids to form ids for
                # trashed/deleted datums/forms.
                if not form.get('__lingsync_deleted'):
                    relational_map['forms'][datum_id] = r['id']
            except:
                sys.exit(u'%sFailed to create an OLD form for the LingSync'
                    u' datum \u2018%s\u2019. Aborting.%s' % (ANSI_FAIL,
                    datum_id, ANSI_ENDC))

            # Delete migrated OLD forms that were previously trashed in
            # LingSync.
            if form.get('__lingsync_deleted'):
                r = c.delete('forms/%s' % r['id'], {})
                try:
                    assert r.get('id')
                    resources_created['deleted'].append(r['id'])
                except:
                    p(r)
                    sys.exit(u'%sFailed to delete on the OLD the trashed'
                        u' LingSync form %s that was migrated.%s' % (ANSI_FAIL,
                        datum_id, ANSI_ENDC))
        print 'Done.'

    return (relational_map, resources_created)


def create_old_files(old_data, c, old_url, relational_map):
    """Create the files in `old_data` on the OLD that the client `c` is
    connected to.

    """

    resources_created = []

    if old_data.get('files'):
        relational_map.setdefault('files', {})
        flush('Creating OLD files...')

        # Issue the create (POST) requests.
        for file in old_data['files']:
            #p(file)
            path = file.get('__local_file_path')
            if not path:
                print u'%sNo file path.%s' % (ANSI_WARNING, ANSI_ENDC)
                continue
            if not os.path.isfile(path):
                print u'%sNo file at %s.%s' % (ANSI_WARNING, path, ANSI_ENDC)
                continue
            size = os.path.getsize(path)
            # Files bigger than 20MB have to be uploaded using Multipart
            # form-data, not as base64-encoded JSON.
            if size > 20971520:
                # NOTE: app.lingsync.org is failing when I attempt to upload a
                # 50MB file. So I'll have to simulate this case ... TODO
                print 'PASSING ON MULTIPART FORM DATA!!!!'
                pass
            else:
                with open(path, 'rb') as f:
                    file['base64_encoded_file'] = base64.b64encode(f.read())
                r = c.create('files', file)
                try:
                    assert r.get('id')
                    resources_created.append(r['id'])
                    # Note: we map the LingSync id of the datum that the file
                    # was associated to to a list of OLD file ids. This way,
                    # when we create the OLD forms, we can use their datum ids
                    # to get the list of OLD file ids that should be in their
                    # `files` attribute.
                    relational_map['files']\
                        .setdefault(file['__lingsync_datum_id'], [])\
                        .append(r['id'])
                except:
                    sys.exit(u'%sFailed to create an OLD file \u2018%s\u2019.'
                        u' Aborting.%s' % (ANSI_FAIL, file['filename'],
                        ANSI_ENDC))
        print 'Done.'

    return (relational_map, resources_created)


# These are the IME/MIME types that the OLD currently allows.
old_allowed_file_types = (
    u'application/pdf',
    u'image/gif',
    u'image/jpeg',
    u'image/png',
    u'audio/mpeg',
    u'audio/ogg',
    u'audio/x-wav',
    u'video/mpeg',
    u'video/mp4',
    u'video/ogg',
    u'video/quicktime',
    u'video/x-ms-wmv'
)

def create_old_tags(old_data, c, old_url, lingsync_corpus_name, relational_map):
    """Create the tags in `old_data` on the OLD that the client `c` is
    connected to.

    """

    resources_created = []

    relational_map.setdefault('tags', {})
    flush('Creating OLD tags...')

    # Create a tag for this migration
    global migration_tag_name
    migration_tag_name = u'Migrated from LingSync corpus %s on %s' % (
        lingsync_corpus_name, datetime.datetime.utcnow().isoformat())
    migration_tag_description = (u'This resource was generated during an'
        u' automated migration from the LingSync corpus %s.' % (
        lingsync_corpus_name,))
    migration_tag = {
        'name': migration_tag_name,
        'description': migration_tag_description
    }
    r = c.create('tags', migration_tag)
    try:
        assert r.get('id')
        resources_created.append(r['id'])
        relational_map['tags'][r['name']] = r['id']
    except:
        sys.exit(u'%sFailed to create the migration tag on the OLD.'
            u' Aborting.%s' % (ANSI_FAIL, migration_tag['name'], ANSI_ENDC))

    if old_data.get('tags'):
        tags_to_create = []
        tags = old_data.get('tags')
        tag_names = [t['name'] for t in tags]

        # Retrieve the existing tags from the OLD. This may affect what
        # tags we create.
        existing_tags = c.get('tags')
        existing_tag_names = [t['name'] for t in existing_tags]

        # Populate our lists of tags to create and update. If a tag
        # already exists, we may just use it instead of creating or even
        # updating.
        for tag in tags:
            if tag['name'] in existing_tag_names:
                counterpart = [t for t in existing_tags if
                    t['name'] == tag['name']][0]
                relational_map['tags'][counterpart['name']] = counterpart['id']
            else:
                tags_to_create.append(tag)

        # Issue the create (POST) requests.
        for tag in tags_to_create:
            r = c.create('tags', tag)
            try:
                assert r.get('id')
                resources_created.append(r['id'])
                relational_map['tags'][tag['name']] = r['id']
            except:
                p(r)
                sys.exit(u'%sFailed to create an OLD tag \u2018%s\u2019.'
                    u' Aborting.%s' % (ANSI_FAIL, tag['name'], ANSI_ENDC))

        print 'Done.'

    return (relational_map, resources_created)


def create_old_speakers(old_data, c, old_url, relational_map):
    """Create the speakers in `old_data` on the OLD that the client `c` is
    connected to.

    """

    resources_created = {
        'created': [],
        'updated': []
    }

    if old_data.get('speakers'):
        flush('Creating OLD speakers...')
        relational_map.setdefault('speakers', {})
        speakers_to_create = []
        speakers_to_update = []
        speakers = old_data.get('speakers')
        speaker_names = [(s['first_name'], s['last_name']) for s in speakers]

        # Retrieve the existing speakers from the OLD. This may affect what
        # speakers we create.
        existing_speakers = c.get('speakers')
        existing_speaker_names = [(s['first_name'], s['last_name']) for s in
            existing_speakers]
        duplicates = list(set(existing_speaker_names) & set(speaker_names))
        ls_speaker_overwrites_old = False
        if len(duplicates) > 0:
            duplicates_string = u'", "'.join([u'%s %s' % (s[0], s[1]) for s in
                duplicates])
            response = raw_input(u'%sUpdate existing speakers? The OLD at %s'
                u' already contains the speaker(s) "%s". Enter \'y\'/\'Y\' to'
                u' update these OLD speakers with the data from LingSync. Enter'
                u' \'n\'/\'N\' (or anything else) to use the existing OLD'
                u' speakers without modification.%s' % (ANSI_WARNING, old_url,
                duplicates_string, ANSI_ENDC))
            if response in ['y', 'Y']:
                ls_speaker_overwrites_old = True

        # Populate our lists of speakers to create and update. If a speaker
        # already exists, we may just use it instead of creating or even
        # updating.
        for speaker in speakers:
            if (speaker['first_name'], speaker['last_name']) in \
            existing_speaker_names:
                counterpart_original = [s for s in existing_speakers if
                    s['first_name'] == speaker['first_name'] and
                    s['last_name'] == speaker['last_name']][0]
                if ls_speaker_overwrites_old:
                    counterpart = copy.deepcopy(counterpart_original)
                    if speaker['dialect'] != counterpart['dialect']:
                        counterpart['dialect'] = speaker['dialect']
                    if speaker['page_content'] and \
                    speaker['page_content'] != counterpart['page_content']:
                        counterpart['page_content'] = speaker['page_content']
                    if counterpart_original != counterpart:
                        speakers_to_update.append(counterpart)
                else:
                    key = u'%s %s' % (counterpart_original['first_name'],
                        counterpart_original['last_name'])
                    relational_map['speakers'][key] = counterpart_original['id']
            else:
                speakers_to_create.append(speaker)

        # Issue the create (POST) and update (PUT) requests.
        for speaker in speakers_to_create:
            r = c.create('speakers', speaker)
            key = u'%s %s' % (speaker['first_name'], speaker['last_name'])
            try:
                assert r.get('id')
                relational_map['speakers'][key] = r['id']
                resources_created['created'].append(r['id'])
            except:
                sys.exit(u'%sFailed to create an OLD speaker \u2018%s\u2019.'
                    u' Aborting.%s' % (ANSI_FAIL, key, ANSI_ENDC))
        for speaker in speakers_to_update:
            resources_created['updated'].append(r['id'])
            r = c.update('speakers/%s' % speaker['id'], speaker)
            key = u'%s %s' % (speaker['first_name'], speaker['last_name'])
            if r.get('error') == (u'The update request failed because the'
                u' submitted data were not new.'):
                relational_map['speakers'][key] = speaker['id']
            else:
                try:
                    assert r.get('id')
                    relational_map['speakers'][key] = speaker['id']
                except:
                    sys.exit(u'%sFailed to update OLD speaker %s'
                        u' (\u2018%s\u2019). Aborting.%s' % (ANSI_FAIL,
                        speaker['id'], key, speaker['speakername'], ANSI_ENDC))
        print 'Done.'

    return (relational_map, resources_created)


def create_old_users(old_data, c, old_url, relational_map):
    """Create the users in `old_data` on the OLD that the client `c` is
    connected to.

    """

    users_created = {
        'created': [],
        'updated': [],
    }

    if old_data.get('users'):
        flush('Creating OLD users...')
        relational_map.setdefault('users', {})
        users_to_create = []
        users_to_update = []
        users = old_data.get('users')
        usernames = [u['username'] for u in users]

        # Retrieve the existing users from the OLD. This may affect what users
        # we create.
        existing_users = c.get('users')
        existing_usernames = filter(None, [u.get('username') for u in
            existing_users])
        duplicates = list(set(existing_usernames) & set(usernames))
        ls_user_overwrites_old = False
        if len(duplicates) > 0:
            duplicates_string = u'", "'.join(duplicates)
            response = raw_input(u'%sUpdate existing users? The OLD at %s'
                u' already contains the user(s) "%s". Enter \'y\'/\'Y\' to'
                u' update these OLD users with the data from LingSync. Enter'
                u' \'n\'/\'N\' (or anything else) to use the existing OLD users'
                u' without modification.%s' % (ANSI_WARNING, old_url,
                duplicates_string, ANSI_ENDC))
            if response in ['y', 'Y']:
                ls_user_overwrites_old = True

        # Populate our lists of users to create and update. If a user already
        # exists, we may just use it instead of creating or even updating.
        for user in users:
            if user['username'] in existing_usernames:
                counterpart_original = [u for u in existing_users if
                    u['username'] == user['username']][0]
                if ls_user_overwrites_old:
                    counterpart = copy.deepcopy(counterpart_original)
                    # Don't change an existing user's password
                    # counterpart['password'] = user['password']
                    # counterpart['password_confirm'] = user['password']
                    if user['first_name'] != counterpart['username']:
                        counterpart['first_name'] = user['first_name']
                    if user['last_name'] != counterpart['username']:
                        counterpart['last_name'] = user['last_name']
                    if user['email'] != FAKE_EMAIL:
                        counterpart['email'] = user['email']
                    if user['affiliation']:
                        counterpart['affiliation'] = user['affiliation']
                    counterpart['role'] = user['role']
                    if user['page_content']:
                        counterpart['page_content'] = user['page_content']
                    counterpart['password'] = u''
                    counterpart['password_confirm'] = u''
                    users_to_update.append(counterpart)
                else:
                    relational_map['users'][counterpart_original['username']] = \
                        counterpart_original['id']
            else:
                user['password'] = DEFAULT_PASSWORD
                user['password_confirm'] = DEFAULT_PASSWORD
                # If the LingSync username is OLD-invalid, we make it valid
                # here.
                p = re.compile('[^\w]+')
                if p.search(user['username']):
                    print u'WARNING: username %s is OLD-invalid.' % user['username']
                    new_username = []
                    for char in user['username']:
                        if not p.search(char):
                            new_username.append(char)
                    new_username = u''.join(new_username)
                    if new_username:
                        user['__original_username'] = user['username']
                        user['username'] = new_username
                        print (u'%sWarning: we have changed the LingSync'
                            u' username %s to the OLD-valid username %s.%s' % (
                            ANSI_WARNING, user['__original_username'],
                            user['username'], ANSI_ENDC))
                    else:
                        sys.exit(u'%sError: unable to create a valid OLD'
                            u' username for LingSync user with username %s.%s' % (
                            ANSI_FAIL, user['username'], ANSI_ENDC))
                users_to_create.append(user)

        # Issue the create (POST) and update (PUT) requests.
        for user in users_to_create:
            r = c.create('users', user)
            try:
                assert r.get('id')
                if user.get('__original_username'):
                    key = user['__original_username']
                else:
                    key = user['username']
                relational_map['users'][key] = r['id']
                users_created['created'].append(key)
            except:
                sys.exit(u'%sFailed to create an OLD user with username'
                    u' \u2018%s\u2019. Aborting.%s' % (ANSI_FAIL,
                    user['username'], ANSI_ENDC))
        for user in users_to_update:
            r = c.update('users/%s' % user['id'], user)
            users_created['updated'].append(user['username'])
            if r.get('error') == (u'The update request failed because the'
                u' submitted data were not new.'):
                relational_map['users'][user['username']] = user['id']
            else:
                try:
                    assert r.get('id')
                    relational_map['users'][user['username']] = user['id']
                except:
                    p(r)
                    sys.exit(u'%sFailed to update OLD user %s (\u2018%s\u2019).'
                        u' Aborting.%s' % (ANSI_FAIL, user['id'], user['username'],
                        ANSI_ENDC))

        print 'Done.'

    return (relational_map, users_created)


if __name__ == '__main__':
    main()

