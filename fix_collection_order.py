#!/usr/bin/python
# coding=utf8

"""
================================================================================
  Fix Collection Form Order
================================================================================

This script fixes the order of forms in a collection, where the collection was
created by the lingsync2old.py migration script. A previous version of that
script failed to order forms in collections correctly when those collections
were created from LingSync sessions.


Usage
--------------------------------------------------------------------------------

Run `fix_collection_order.py` and you will be prompted for the required arguments::

    $ ./fix_collection_order.py

You can also supploy the required arguments as options::

    $ ./fix_collection_order.py \
            --ls-json-file=path-to-my-lingsync-json-dump-file \
            --old-json-file=path-to-my-lingsync-json-dump-file \
            --old-url=my-old-url \
            --old-username=my-old-username \
            --old-password=my-old-password

Full param/option listing:

    --ls-json-file: The path to the .json file that was created by
        lingsync2old.py and which contains the unmodified LingSync dump.

    --old-json-file: The path to the .json file that was created by
        lingsync2old.py and which contains the OLD-compatible dicts that were
        created from the raw LingSync input.

    --old-url: The URL of the OLD whose collections we need to fix.

    --old-username: The username of a user on the destination OLD who
        has sufficient privileges.

    --old-password: The password corresponding to the OLD username.

"""

from old_client import OLDClient
import requests
import json
import optparse
import getpass
import unicodedata
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

# ANSI escape sequences for formatting command-line output.
ANSI_HEADER = '\033[95m'
ANSI_OKBLUE = '\033[94m'
ANSI_OKGREEN = '\033[92m'
ANSI_WARNING = '\033[93m'
ANSI_FAIL = '\033[91m'
ANSI_ENDC = '\033[0m'
ANSI_BOLD = '\033[1m'
ANSI_UNDERLINE = '\033[4m'


def flush(string):
    """Print `string` immediately, and with no carriage return.

    """

    print string,
    sys.stdout.flush()



def add_optparser_options(parser):
    """Add options to the optparser parser.

    --ls-json-file: The path to the .json file containing the raw LingSync data.

    --old-json-file: The path to the .json file that was created by
        lingsync2old.py and which contains the OLD-compatible dicts that were
        created from the raw LingSync input.

    --old-url: The OLD URL that we will upload the converted LingSync
        data to.

    --old-username: The username of a user on the destination OLD who
        has sufficient privileges to make create, update and delete requests,
        i.e., an admin or a contributor.

    --old-password: The password corresponding to the OLD username.

    """

    parser.add_option("--ls-json-file", dest="ls_json_file",
        metavar="LS_JSON_FILE", help="The path to the .json file containing the"
        " raw LingSync data.")

    parser.add_option("--old-json-file", dest="old_json_file",
        metavar="OLD_JSON_FILE", help="The path to the .json file that was"
        " created by lingsync2old.py and which contains the OLD-compatible dicts"
        " that were created from the raw LingSync input.")

    parser.add_option("--old-url", dest="old_url", metavar="OLD_URL",
        help="The URL of the OLD whose collections we want to fix.")

    parser.add_option("--old-username", dest="old_username",
        metavar="OLD_USERNAME", help="The username of a user on the destination"
        " OLD who has sufficient privileges.")

    parser.add_option("--old-password", dest="old_password",
        metavar="OLD_PASSWORD", help="The password corresponding to the OLD"
        " username.")


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


def fix_collections(options):
    """Make HTTP requests to fix the order of forms in the OLD's collections.

    """

    # Get raw LingSync JSON data.
    ls_json_file = getattr(options, 'ls_json_file')
    try:
        ls_data = json.load(open(ls_json_file))
    except:
        sys.exit(u'%sUnable to locate file %s. Aborting.%s' % (ANSI_FAIL,
            ls_json_file, ANSI_ENDC))

    # Get converted OLD data.
    old_json_file = getattr(options, 'old_json_file')
    try:
        old_data = json.load(open(old_json_file))
    except:
        sys.exit(u'%sUnable to locate file %s. Aborting.%s' % (ANSI_FAIL,
            old_json_file, ANSI_ENDC))
    forms = old_data['forms']

    # `datums` holds the raw LingSync dicts representing all of the datums.
    datumid2dateentered = {}
    for datum in (r['doc'] for r in ls_data['rows']
        if get_collection_for_lingsync_doc(r['doc']) == 'datums'):
        datumid2dateentered[datum['_id']] = datum['dateEntered']


    # Get an OLD client.
    old_url = getattr(options, 'old_url', None)
    old_username = getattr(options, 'old_username', None)
    old_password = getattr(options, 'old_password', None)
    c = OLDClient(old_url)

    # Log in to the OLD.
    logged_in = c.login(old_username, old_password)
    if not logged_in:
        sys.exit(u'%sUnable to log in to %s with username %s and password %s.'
            u' Aborting.%s' % (ANSI_FAIL, old_url, old_username, old_password,
            ANSI_ENDC))

    # Populate the `formid2dateentered` dict, so that it maps OLD form ids to
    # date entered values taken from the raw LingSync data.
    formid2dateentered = {}
    patt3 = re.compile('This form was created from LingSync datum (\w+)')
    for form in c.get('forms'):
        form_id = form['id']
        datum_id = patt3.findall(form['comments'])
        if len(datum_id) == 0:
            print '%sUnable to find LingSync datum id for OLD form %d: %s.%s' % (
                ANSI_WARNING, form_id, form['transcription'], ANSI_ENDC)
            datum_id = None
        else:
            if len(datum_id) > 1:
                print ('%sWarning: found multiple LingSync datum ids for OLD'
                    ' form %d.%s' % (ANSI_WARNING, form_id, ANSI_ENDC))
            datum_id = datum_id[0]
        if datum_id:
            date_entered = datumid2dateentered[datum_id]
        else:
            date_entered = '0'
        formid2dateentered[form_id] = date_entered

    # Issue the requests to fix each of the OLD collections, in turn.
    collections = c.get('collections')
    # print len(collections)
    patt1 = re.compile('^(form\[\d+\])*$')
    patt2 = re.compile('form\[(\d+)\]')
    manualfix = {}
    for collection in collections:
        # print collection['contents']
        # If there's anything besides form references in the collection, then
        # we know the user has manually updated it and we can't fix it
        # automatedly; best we can do is tell the user the order of form
        # references that matches the LingSync version.
        tmp = collection['contents'].replace(' ', '').replace('\n', '')
        if patt1.search(tmp) or collection['contents'].strip() == '':
            contents_modified = False
        else:
            contents_modified = True

        # print '\n%d' % collection['id']
        current_form_ids = map(int, patt2.findall(tmp))
        sorted_form_ids = [x[1] for x in sorted(
            [(formid2dateentered[id_], id_) for id_ in current_form_ids])]
        new_contents = '\n'.join(['form[%d]' % x for x in sorted_form_ids])
        if contents_modified:
            manualfix[collection['id']] = new_contents
        else:
            if current_form_ids == sorted_form_ids:
                print ('Collection %d already has its forms in the correct'
                    ' order.' % collection['id'])
            else:
                print 'Fixing collection %d.' % collection['id']
                collection['contents'] = new_contents
                # We must fix any relational data and or date elicited values
                # for the update request.
                if collection['elicitor']:
                    collection['elicitor'] = collection['elicitor']['id']
                if collection['speaker']:
                    collection['speaker'] = collection['speaker']['id']
                if collection['source']:
                    collection['source'] = collection['source']['id']
                if collection['tags']:
                    collection['tags'] = [t['id'] for t in collection['tags']]
                if collection['files']:
                    collection['files'] = [t['id'] for t in collection['files']]
                if collection['date_elicited']:
                    # Convert yyyy-mm-dd to mm/dd/yyyy format
                    parts = collection['date_elicited'].split('-')
                    collection['date_elicited'] = '%s/%s/%s' % (parts[1],
                        parts[2], parts[0])
                resp = c.put('collections/%d' % collection['id'], collection)
                if resp.get('contents') != new_contents:
                    print ('Something went wrong when attempting to update the'
                        ' contents of collection %d. It should have the following'
                        ' contents value\n%s' % (collection['id'], new_contents))
                    p(resp)

    for id in manualfix:
        new_contents = manualfix[id]
        print ('Collection %d has been altered by a user on the OLD so we'
            ' can\'t fix its form order here. You will have to do it. Please make'
            ' sure that the order of form references matches the following:\n%s.' % (
            id, new_contents))

    print 'Done.'


def norm(ustr):
    return unicodedata.normalize('NFD', ustr)


def get_params():
    """Get the parameters and options entered at the command line.

    """

    usage = "usage: ./%prog [options]"
    parser = optparse.OptionParser(usage)
    add_optparser_options(parser)
    (options, args) = parser.parse_args()
    lingsync_json_file = getattr(options, 'ls_json_file', None)
    old_json_file = getattr(options, 'old_json_file', None)
    old_url = getattr(options, 'old_url', None)
    old_username = getattr(options, 'old_username', None)
    old_password = getattr(options, 'old_password', None)

    # If the required params haven't been supplied as options, we prompt the
    # user for them.
    if len(filter(None, [lingsync_json_file, old_json_file, old_url,
        old_username, old_password])) < 5:

        if not lingsync_json_file:
            lingsync_json_file = getpass.getpass(u'%sPlease enter the path to'
                ' the .json file containing the raw LingSync data:%s ' % (
                ANSI_WARNING, ANSI_ENDC))
            if lingsync_json_file:
                options.ls_json_file = lingsync_json_file
            else:
                sys.exit(u'%sYou must provide the path to the LingSync .json'
                    'file. Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

        if not old_json_file:
            old_json_file = getpass.getpass(u'%sPlease enter the path to'
                ' the .json file containing the converted OLD data:%s ' % (
                ANSI_WARNING, ANSI_ENDC))
            if old_json_file:
                options.old_json_file = old_json_file
            else:
                sys.exit(u'%sYou must provide the path to the OLD .json'
                    'file. Aborting.%s' % (ANSI_FAIL, ANSI_ENDC))

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

    print '\n%sFixing form order in OLD collections%s' % (ANSI_HEADER, ANSI_ENDC)
    print (u'We are going to fix the order of forms in the collections of the'
        ' OLD at %s.' % old_url)

    return options


def main():
    """This function performs the conversion.

    """

    options = get_params()
    fix_collections(options)


if __name__ == '__main__':
    main()

