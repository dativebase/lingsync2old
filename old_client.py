#!/home/joel/env/bin/python
# coding=utf8

# Copyright 2013 Joel Dunham
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""OLD Client --- functionality for connecting to an OLD application.

The primary class defined here is OLDClient. Use it to connect to a
server-side OLD web service.

"""

import requests
import codecs
import unicodedata
import simplejson as json
from time import sleep
import locale
import sys

# Wrap sys.stdout into a StreamWriter to allow writing unicode.
# This allows piping of unicode output.
# See http://stackoverflow.com/questions/4545661/unicodedecodeerror-when-redirecting-to-file
#sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)

class Log(object):
    """A simple logger so that print statements can be filtered.

    """

    def __init__(self, silent=False):
        self.silent = silent

    def debug(self, msg):
        if not self.silent:
            print u'[DEBUG] %s' % msg

    def info(self, msg):
        if not self.silent:
            print u'[INFO] %s' % msg

    def warn(self, msg):
        if not self.silent:
            print u'[WARN] %s' % msg

# The global ``log`` instance can be used instead of ``print`` and output can be
# silenced by setting ``log.silent`` to ``True``.
log = Log()


class OLDClient(object):
    """Create an OLD instance to connect to a live OLD application.

    Basically this is just some OLD-specific conveniences wrapped around
    a requests.Session instance.

    """

    def __init__(self, host, port, scheme='http'):
        self.__setcreateparams__()
        self.host = host
        self.port = port
        self.baseurl = '%s://%s:%s' % (scheme, host, port)
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})

    def login(self, username, password):
        payload = json.dumps({'username': username, 'password': password})
        response = self.session.post('%s/login/authenticate' % self.baseurl,
            data=payload)
        return response.json().get('authenticated', False)

    def get(self, path, params=None, verbose=True):
        response = self.session.get('%s/%s' % (self.baseurl, path),
            params=params)
        return self.return_response(response, verbose=verbose)

    def post(self, path, data=json.dumps({})):
        response = self.session.post('%s/%s' % (self.baseurl, path),
            data=json.dumps(data))
        return self.return_response(response)

    create = post

    def put(self, path, data=json.dumps({})):
        response = self.session.put('%s/%s' % (self.baseurl, path),
            data=json.dumps(data))
        return self.return_response(response)

    update = put

    def delete(self, path, data=json.dumps({})):
        response = self.session.delete('%s/%s' % (self.baseurl, path),
            data=json.dumps(data))
        return self.return_response(response)

    def search(self, path, data):
        response = self.session.request('SEARCH', '%s/%s' % (self.baseurl,
            path), data=json.dumps(data))
        return self.return_response(response)

    def return_response(self, response, verbose=True):
        try:
            return response.json()
        except Exception, e:
            if verbose:
                print 'Exception in return_response'
                print e
            return response

    def human_readable_seconds(self, seconds):
        return u'%02dm%02ds' % (seconds / 60, seconds % 60)

    def normalize(self, unistr):
        """Return a unistr using canonical decompositional normalization (NFD).

        """

        try:
            return unicodedata.normalize('NFD', unistr)
        except TypeError:
            return unicodedata.normalize('NFD', unicode(unistr))
        except UnicodeDecodeError:
            return unistr

    def poll(self, requester, changing_attr, changing_attr_originally,
             log, wait=2, vocal=True, task_descr='task'):
        """Poll a resource by calling ``requester`` until the value of ``changing_attr``
        no longer matches ``changing_attr_originally``.

        """

        seconds_elapsed = 0
        while True:
            response = requester()
            if changing_attr_originally != response[changing_attr]:
                if vocal:
                    log.info('Task %s terminated' % task_descr)
                break
            else:
                if vocal:
                    log.info('Waiting for %s to terminate: %s' %
                        (task_descr, self.human_readable_seconds(seconds_elapsed)))
            sleep(wait)
            seconds_elapsed = seconds_elapsed + wait
        return response

    def __setcreateparams__(self):
        """Set up instance variables for the create params of each
        OLD object type. Note that this should not be necessary, i.e.,
        the OLD should be modified so that creation of an object does 
        not require sending a bunch of empty attribute values.

        """

        self.application_settings_create_params = {
            'object_language_name': u'',
            'object_language_id': u'',
            'metalanguage_name': u'',
            'metalanguage_id': u'',
            'metalanguage_inventory': u'',
            'orthographic_validation': u'None', # Value should be one of [u'None', u'Warning', u'Error']
            'narrow_phonetic_inventory': u'',
            'narrow_phonetic_validation': u'None',
            'broad_phonetic_inventory': u'',
            'broad_phonetic_validation': u'None',
            'morpheme_break_is_orthographic': u'',
            'morpheme_break_validation': u'None',
            'phonemic_inventory': u'',
            'morpheme_delimiters': u'',
            'punctuation': u'',
            'grammaticalities': u'',
            'unrestricted_users': [],        # A list of user ids
            'storage_orthography': u'',        # An orthography id
            'input_orthography': u'',          # An orthography id
            'output_orthography': u''         # An orthography id
        }
        self.collection_create_params = {
            'title': u'',
            'type': u'',
            'url': u'',
            'description': u'',
            'markup_language': u'',
            'contents': u'',
            'speaker': u'',
            'source': u'',
            'elicitor': u'',
            'enterer': u'',
            'date_elicited': u'',
            'tags': [],
            'files': []
        }
        self.corpus_create_params = {
            'name': u'',
            'description': u'',
            'content': u'',
            'form_search': u'',
            'tags': []
        }
        self.file_create_params = {
            'name': u'',
            'description': u'',
            'date_elicited': u'',    # mm/dd/yyyy
            'elicitor': u'',
            'speaker': u'',
            'utterance_type': u'',
            'embedded_file_markup': u'',
            'embedded_file_password': u'',
            'tags': [],
            'forms': [],
            'file': ''      # file data Base64 encoded
        }
        self.file_create_params_base64 = {
            'filename': u'',        # Will be filtered out on update requests
            'description': u'',
            'date_elicited': u'',    # mm/dd/yyyy
            'elicitor': u'',
            'speaker': u'',
            'utterance_type': u'',
            'tags': [],
            'forms': [],
            'base64_encoded_file': '' # file data Base64 encoded; will be filtered out on update requests
        }
        self.file_create_params_MPFD = {
            'filename': u'',        # Will be filtered out on update requests
            'description': u'',
            'date_elicited': u'',    # mm/dd/yyyy
            'elicitor': u'',
            'speaker': u'',
            'utterance_type': u'',
            'tags-0': u'',
            'forms-0': u''
        }
        self.file_create_params_sub_ref = {
            'parent_file': u'',
            'name': u'',
            'start': u'',
            'end': u'',
            'description': u'',
            'date_elicited': u'',    # mm/dd/yyyy
            'elicitor': u'',
            'speaker': u'',
            'utterance_type': u'',
            'tags': [],
            'forms': []
        }
        self.file_create_params_ext_host = {
            'url': u'',
            'name': u'',
            'password': u'',
            'MIME_type': u'',
            'description': u'',
            'date_elicited': u'',    # mm/dd/yyyy
            'elicitor': u'',
            'speaker': u'',
            'utterance_type': u'',
            'tags': [],
            'forms': []
        }
        self.form_create_params = {
            'transcription': u'',
            'phonetic_transcription': u'',
            'narrow_phonetic_transcription': u'',
            'morpheme_break': u'',
            'grammaticality': u'',
            'morpheme_gloss': u'',
            'translations': [],
            'comments': u'',
            'speaker_comments': u'',
            'elicitation_method': u'',
            'tags': [],
            'syntactic_category': u'',
            'speaker': u'',
            'elicitor': u'',
            'verifier': u'',
            'source': u'',
            'status': u'tested',
            'date_elicited': u'',     # mm/dd/yyyy
            'syntax': u'',
            'semantics': u''
        }
        self.form_search_create_params = {
            'name': u'',
            'search': u'',
            'description': u'',
            'searcher': u''
        }
        self.morpheme_language_model_create_params = {
            'name': u'',
            'description': u'',
            'corpus': u'',
            'vocabulary_morphology': u'',
            'toolkit': u'',
            'order': u'',
            'smoothing': u'',
            'categorial': False
        }
        self.morphology_create_params = {
            'name': u'',
            'description': u'',
            'lexicon_corpus': u'',
            'rules_corpus': u'',
            'script_type': u'lexc',
            'extract_morphemes_from_rules_corpus': False,
            'rules': u'',
            'rich_upper': True,
            'rich_lower': False,
            'include_unknowns': False
        }
        self.morphological_parser_create_params = {
            'name': u'',
            'phonology': u'',
            'morphology': u'',
            'language_model': u'',
            'description': u''
        }
        self.orthography_create_params = {
            'name': u'',
            'orthography': u'',
            'lowercase': False,
            'initial_glottal_stops': True
        }
        self.page_create_params = {
            'name': u'',
            'heading': u'',
            'markup_language': u'',
            'content': u'',
            'html': u''
        }
        self.phonology_create_params = {
            'name': u'',
            'description': u'',
            'script': u''
        }
        self.source_create_params = {
            'file': u'',
            'type': u'',
            'key': u'',
            'address': u'',
            'annote': u'',
            'author': u'',
            'booktitle': u'',
            'chapter': u'',
            'crossref': u'',
            'edition': u'',
            'editor': u'',
            'howpublished': u'',
            'institution': u'',
            'journal': u'',
            'key_field': u'',
            'month': u'',
            'note': u'',
            'number': u'',
            'organization': u'',
            'pages': u'',
            'publisher': u'',
            'school': u'',
            'series': u'',
            'title': u'',
            'type_field': u'',
            'url': u'',
            'volume': u'',
            'year': u'',
            'affiliation': u'',
            'abstract': u'',
            'contents': u'',
            'copyright': u'',
            'ISBN': u'',
            'ISSN': u'',
            'keywords': u'',
            'language': u'',
            'location': u'',
            'LCCN': u'',
            'mrnumber': u'',
            'price': u'',
            'size': u'',
        }
        self.speaker_create_params = {
            'first_name': u'',
            'last_name': u'',
            'page_content': u'',
            'dialect': u'dialect',
            'markup_language': u'reStructuredText'
        }
        self.syntactic_category_create_params = {
            'name': u'',
            'type': u'',
            'description': u''
        }
        self.user_create_params = {
            'username': u'',
            'password': u'',
            'password_confirm': u'',
            'first_name': u'',
            'last_name': u'',
            'email': u'',
            'affiliation': u'',
            'role': u'',
            'markup_language': u'',
            'page_content': u'',
            'input_orthography': None,
            'output_orthography': None
        }


def printform(form):
    """Pretty print an OLD form to the terminal.

    """

    tmp = [('id', form['id'])]
    if form.get('narrow_phonetic_transcription', None):
        tmp.append(('NP', form['narrow_phonetic_transcription']))
    if form.get('phonetic_transcription', None):
        tmp.append(('BP', form['phonetic_transcription']))
    tmp.append(('TR', '%s%s' % (form['grammaticality'], form['transcription'])))
    if form.get('morpheme_break', None):
        tmp.append(('MB', form['morpheme_break']))
    if form.get('morpheme_gloss', None):
        tmp.append(('MG', form['morpheme_gloss']))
    tmp.append(('TL', ', '.join([u'\u2018%s\u2019' % tl['transcription'] for tl
        in form['translations']])))
    if form.get('syntactic_category_string', None):
        tmp.append(('SCS', form['syntactic_category_string']))
    if form.get('break_gloss_category', None):
        tmp.append(('BGC', form['break_gloss_category']))
    if form.get('syntactic_category', None):
        tmp.append(('SC', form['syntactic_category']['name']))
    print u'\n'.join([u'%-5s%s' % (u'%s:' % t[0], t[1]) for t in tmp])


