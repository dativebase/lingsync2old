#!/usr/bin/python
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

"""FieldDB Client --- functionality for connecting to FieldDB web services.

This module may be useful for developers or for people just wanting to get their
FieldDB data into Python. It's also good for understanding how to use the FieldDB
and CouchDB APIs.

"""

import requests
import pprint
import simplejson as json
import uuid
import copy
import optparse

# For logging HTTP requests & responses
import logging
try:
    import http.client as http_client
except ImportError:
    import httplib as http_client # Python 2


# Stop the Certificate warnings with `verify=False`
requests.packages.urllib3.disable_warnings()

p = pprint.pprint

def verbose():
    """Call this to spit the HTTP requests/responses to stdout.
    From http://stackoverflow.com/questions/10588644/how-can-i-see-the-entire-http-request-thats-being-sent-by-my-python-application
    I don't know how it works or how to turn it off once it's called ...

    """

    http_client.HTTPConnection.debuglevel = 1
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


class FieldDBClient(object):
    """Create a FieldDB instance to connect to live FieldDB web services.

    Basically this is just some FieldDB-specific conveniences wrapped around
    a Python `requests.Session` instance.

    """

    def __init__(self, options):
        """Options is a dict of options, or a string path to a JSON object/dict.

        """

        if type(options) is str:
            options = json.load(open(options, 'rb'))
        self._process_options(options)
        self.session = requests.Session()
        self.session.verify = False # https without certificates, wild!
        self.session.headers.update({'Content-Type': 'application/json'})

    def _process_options(self, options):

        self.auth_protocol = options.get('auth_protocol', 'https')
        self.auth_host = options.get('auth_host', 'localhost')
        self.auth_port = options.get('auth_port', '')
        # self.auth_port = options.get('auth_port', '3183')

        self.corpus_protocol = options.get('corpus_protocol', 'http')
        self.corpus_host = options.get('corpus_host', '127.0.0.1')
        self.corpus_port = options.get('corpus_port ', '')
        # self.corpus_port = options.get('corpus_port ', '9292')

        self.couch_protocol = options.get('couch_protocol', 'http')
        self.couch_host = options.get('couch_host', 'localhost')
        self.couch_port = options.get('couch_port ', '')
        # self.couch_port = options.get('couch_port ', '5984')

        self.username = options.get('username', 'someusername')
        self.password = options.get('password', 'somesecret')

        self.admin_username = options.get('admin_username', 'someusername')
        self.admin_password = options.get('admin_password', 'somesecret')

        self.server_code = options.get('server_code', 'local')
        self.app_version_when_created = options.get('app_version_when_created',
            'unknown')

    # URL getters
    ############################################################################

    def _get_url(self, protocol, host, port):
        return '%s://%s:%s' % (protocol, host, port)

    def _get_url_cred(self, protocol, host, port):
        return '%s://%s:%s@%s:%s' % (protocol, self.username, self.password,
            host, port)

    def get_auth_url(self):
        return self._get_url(self.auth_protocol, self.auth_host, self.auth_port)

    def get_corpus_url(self):
        return self._get_url(self.corpus_protocol, self.corpus_host,
            self.corpus_port)

    def get_couch_url(self):
        return self._get_url(self.couch_protocol, self.couch_host,
            self.couch_port)

    def get_auth_url_cred(self):
        return self._get_url_cred(self.auth_protocol, self.auth_host,
            self.auth_port)

    def get_corpus_url_cred(self):
        return self._get_url_cred(self.corpus_protocol, self.corpus_host,
            self.corpus_port)

    # General methods
    ############################################################################

    def get_uuid(self):
        return uuid.uuid4().hex

    # Authentication Web Service
    ############################################################################
    #
    # Here is the API (see AuthenticationWebService/service.js):
    #
    # POST /login (attempt a login)
    # POST /register (create a new user)
    # POST /newcorpus

    # POST /changepassword
    # POST /corpusteam (list of team members on a corpus)
    # POST /addroletouser
    # POST /updateroles

    def login(self):
        """Login to the FieldDB Authentication web service.

        """
        response = self.session.post(
            '%s/login' % self.get_auth_url(),
            data=json.dumps({
                'username': self.username,
                'password': self.password}))
        rjson = response.json()
        if rjson.has_key('user'):
            self.user = rjson['user']
            self.cookies = response.cookies
        return rjson

    def register(self, username, password, email):
        """Register a new user via the FieldDB Authentication web service.

        ..note::

            It is not clear to me if the appVersionWhenCreated param is
            important.

        """

        return self.session.post(
            '%s/register' % self.get_auth_url(),
            data=json.dumps({
                'username': username,
                'password': password,
                'email': email,
                'serverCode': self.server_code,
                'authUrl': self.get_auth_url(),
                'appVersionWhenCreated': self.app_version_when_created
            })).json()

    def new_corpus(self, new_corpus_name):
        """Create a new FieldDB corpus via the FieldDB Authentication web
        service.

        POST /newcorpus with a JSON object payload.

        If successful, `response['corpusadded'] is True`
        If unsuccessful:
        {u'corpusadded': True,
         u'info': [u'User details saved.'],
          u'userFriendlyErrors': [u'There was an error creating your corpus.
          Blackfoot']}

        """

        return self.session.post(
            '%s/newcorpus' % self.get_auth_url(),
            data=json.dumps({
                'newCorpusName': new_corpus_name,
                'username': self.username,
                'password': self.password,
                'serverCode': self.server_code,
                'authUrl': self.get_auth_url(),
                'appVersionWhenCreated': self.app_version_when_created
            })).json()


    # Corpus Web Service
    ############################################################################

    # Direct CouchDB Requests
    ############################################################################

    def login_couchdb(self):
        """Login via the CouchDB HTTP API using the admin user.

        """

        return self.session.post(
            '%s/_session' % self.get_couch_url(),
            data=json.dumps({
                'name': self.admin_username,
                'password': self.admin_password})).json()

    def get_greeting(self):
        return self.session.get(self.get_couch_url()).json()

    def get_database_list(self):
        url = '%s/_all_dbs' % self.get_couch_url()
        return self.session.get(url).json()

    def get_users(self):
        return self.get_all_docs_list('zfielddbuserscouch')

    def get__users(self):
        return self.get_all_docs_list('_users')

    def get_usernames(self):
        """Use the CouchDB API to get the usernames of the user documents in
        zfielddbuserscouch.

        """
        return [u['doc']['username'] for u
                in self.get_users()['rows']
                if u['doc'].has_key('username')]

    def get__usernames(self):
        """Use the CouchDB API to get the usernames of the user documents in
        _users.

        .. note::

            This assumes that the id is something like
            'org.couchdb.user:username'.

        """
        return [u['doc']['_id'].split(':')[1] for u in
                self.get__users()['rows']
                if u['doc'].get('type') == 'user']

    def delete_user_and_corpora(self, username):
        """Use the CouchDB API to delete a FieldDB user.

        ..warning::

            This involves deleting the user's documents from both of the users
            databases as well as deleting the users corpora and activity feed
            databases. I do not know if it should involve the deletion of other
            data as well.

        ..warning::

            This is just for testing. If you delete a database (=corpus) that
            another user has access to and you don't alter that other user's
            roles accordingly, the database will be in an inconsistent state.

        """

        _users_db = '_users'
        users_db = 'zfielddbuserscouch'

        dbs_to_delete = [db_name for db_name in self.get_database_list() if
            db_name.startswith('%s-' % username)]
        for db in dbs_to_delete:
            delete_db_resp = self.delete_database(db)
            if delete_db_resp.get('ok') is True:
                print '... Deleted database "%s".' % db

        user = self.get_document(users_db, username)
        user_id = user.get('_id')
        user_rev = user.get('_rev')

        _user = self.get_document(_users_db, 'org.couchdb.user:%s' % username)
        _user_id = _user.get('_id')
        _user_rev = _user.get('_rev')

        if user_id:
            r = self.delete_document(users_db, user_id, user_rev)
            if r.get('ok') is True:
                print '... Deleted user "%s".' % user_id

        if _user_id:
            r = self.delete_document(_users_db, _user_id, _user_rev)
            if r.get('ok') is True:
                print '... Deleted user "%s".' % _user_id

    def create_database(self, database_name):
        """Only CouchDB admins can create databases.

        """

        url = '%s/%s' % (self.get_couch_url(), database_name)
        return self.session.put(url).json()

    def delete_database(self, database_name):
        #url = '%s/%s' % (self.get_couch_url_cred(), database_name)
        url = '%s/%s' % (self.get_couch_url(), database_name)
        return self.session.delete(url).json()

    def replicate_database(self, source_name, target_name):
        url = '%s/_replicate' % self.get_couch_url()
        payload=json.dumps({
            'source': source_name,
            'target': target_name,
            'create_target': True})
        return self.session.post(
            url,
            data=payload,
            headers={'content-type': 'application/json'}).json()

    # Documents
    ############################################################################

    def create_document(self, database_name, document):
        document = json.dumps(document)
        url = '%s/%s' % (self.get_couch_url(), database_name)
        return self.session.post(
            url,
            data=document,
            headers = {'content-type': 'application/json'}).json()

    def get_document(self, database_name, document_id):
        url = '%s/%s/%s' % (self.get_couch_url(), database_name, document_id)
        return self.session.get(url).json()

    def get_all_docs_list(self, database_name):
        url = '%s/%s/_all_docs' % (self.get_couch_url(), database_name)
        return self.session.get(url, params={'include_docs': 'true'}).json()

    def update_document(self, database_name, document_id, document_rev,
        new_document):
        url = '%s/%s/%s' % (self.get_couch_url(), database_name, document_id)
        new_document['_rev'] = document_rev
        return self.session.put(url,
            data=json.dumps(new_document),
            headers = {'content-type': 'application/json'}).json()

    def delete_document(self, database_name, document_id, document_rev):
        url = '%s/%s/%s?rev=%s' % (self.get_couch_url(), database_name,
            document_id, document_rev)
        return self.session.delete(url).json()


class FieldDBClientTester(object):
    """Class with a `test` method that has a bunch of `assert` statements that
    make sure that a FieldDBClient instance is behaving as we expect it to. Most
    of this is straight out of http://guide.couchdb.org/

    Usage::

        >>> tester = FieldDBClientTester(fielddb_client)
        >>> tester.test()

    """

    def __init__(self, fielddb_instance, database_name='fruits',
        database_clone_name='fruits_clone'):
        self.fielddb = fielddb_instance
        self.database_name = database_name
        self.database_clone_name = database_clone_name

    fruits = {
        "orange": {
            "item" : "orange",
            "prices" : {
                "Fresh Mart" : 1.99,
                "Price Max" : 3.19,
                "Citrus Circus" : 1.09
            }
        },
        "apple": {
            "item" : "apple",
            "prices" : {
                "Fresh Mart" : 1.59,
                "Price Max" : 5.99,
                "Apples Express" : 0.79
            }
        },
        "banana": {
            "item" : "banana",
            "prices" : {
                "Fresh Mart" : 1.99,
                "Price Max" : 0.79,
                "Banana Montana" : 4.22
            }
        }
    }

    def clean_up_couch(self):
        """Clean up the couch by deleting the databases we've created.

        """

        database_list = self.fielddb.get_database_list()
        if self.database_name in database_list:
            self.fielddb.delete_database(self.database_name)
            print '... Deleted database "%s".' % self.database_name
        if self.database_clone_name in database_list:
            self.fielddb.delete_database(self.database_clone_name)
            print '... Deleted database "%s".' % self.database_clone_name

    def test(self):
        """Run some tests by making requests to the Auth service, the Corpus
        service, and the CouchDB API and verifying that these behave as
        expected. The tests are just simple `assert` statements.

        """

        user_to_add_username = 'devlocal'
        user_to_add_password = 'devlocal'
        user_to_add_email = 'a@b.com'

        temporary_user_username = 'temporary'
        temporary_user_password = 'temporary'
        temporary_user_email = 'tem@porary.com'

        print '\nTesting the FieldDB client.'

        # Clean Up.
        self.clean_up_couch()

        # Login to the Authentication web service.
        login_resp = self.fielddb.login()
        assert login_resp.has_key('user')
        assert login_resp['user']['username'] == self.fielddb.username
        print '... Logged in to Authentication web service as "%s".' % \
            self.fielddb.username

        # Login to CouchDB with the admin account.
        couchdb_login_resp = self.fielddb.login_couchdb()
        assert couchdb_login_resp['ok'] is True
        print '... Logged in to CouchDB as "%s".' % self.fielddb.admin_username

        # Get users.
        users_list = self.fielddb.get_usernames()
        assert type(users_list) == type([])
        print '... Got users list.'

        # Create devlocal user if it doesn't exist.
        if user_to_add_username not in users_list:
            self.fielddb.register(user_to_add_username, user_to_add_password,
                user_to_add_email)
            print '... Registered user "%s".' % user_to_add_username
        else:
            print '... User "%s" is already registered.' % user_to_add_username

        # Create temporary user.
        register_request = self.fielddb.register(temporary_user_username,
            temporary_user_password, temporary_user_email)
        if register_request.get('userFriendlyErrors'):
            print '... User "%s" is already registered.' % temporary_user_username
        else:
            print '... Registered user "%s".' % temporary_user_username

        # Delete temporary user.
        self.fielddb.delete_user_and_corpora(temporary_user_username)
        print '... Deleted user "%s" and its corpora/databases.' % \
            temporary_user_username

        # Get the CouchDB greeting.
        greeting = self.fielddb.get_greeting()
        assert greeting.has_key('couchdb')
        print '... Got CouchDB greeting.'

        # Get the database list.
        database_list = self.fielddb.get_database_list()
        assert type(database_list) is type([])
        print '... Got database list.'

        # Create a FieldDB corpus via the auth service
        new_corpus_name = 'Blackfoot'
        r = self.fielddb.new_corpus(new_corpus_name)
        if r.get('userFriendlyErrors'):
            print '... Corpus "%s" already exists.' % new_corpus_name
        else:
            print '... Corpus "%s" created.' % new_corpus_name

        # Create a CouchDB database.
        if self.database_name not in database_list:
            create_response = self.fielddb.create_database(self.database_name)
            try:
                assert create_response['ok'] is True
            except:
                pprint.pprint(create_response)
            print '... Created database "%s".' % self.database_name
        else:
            print '... Database "%s" already exists.' % self.database_name

        # Create documents.
        apple_create_response = self.fielddb.create_document(self.database_name,
            self.fruits['apple'])
        orange_create_response = self.fielddb.create_document(self.database_name,
            self.fruits['orange'])
        banana_create_response = self.fielddb.create_document(self.database_name,
            self.fruits['banana'])
        apple_id = apple_create_response['id']
        orange_id = apple_create_response['id']
        banana_id = apple_create_response['id']
        assert apple_create_response['ok'] is True
        assert orange_create_response['ok'] is True
        assert banana_create_response['ok'] is True
        assert type(apple_id) is unicode # id is a UUID, e.g., u'59da119f7911695425ab79f8a7060709'}
        assert len(apple_id) is 32
        print '... Created apple, orange, and banana documents.'

        # Get a document.
        banana = self.fielddb.get_document(self.database_name, banana_id)
        assert banana.has_key('_id')
        assert banana['_id'] == banana_id
        assert banana.has_key('_rev')
        assert banana['_rev'][0] == u'1'
        assert banana.has_key('item')
        assert type(banana['prices']) is dict
        print '... Retrieved the banana document.'

        # Update a document.
        new_banana = copy.deepcopy(self.fruits['banana'])
        new_banana['foo'] = 'bar'
        new_banana['item'] = 'waaaaanana'
        update_response = self.fielddb.update_document(self.database_name,
            banana['_id'], banana['_rev'], new_banana)
        assert update_response['rev'][0] == u'2'
        assert update_response['ok'] is True
        assert update_response['id'] == banana_id
        print '... Updated the banana document.'

        # Get an updated document.
        new_banana = self.fielddb.get_document(self.database_name, banana['_id'])
        assert new_banana['_id'] == banana_id
        assert new_banana['item'] == u'waaaaanana'
        print '... Retrieved the updated banana.'

        # Replicate a database.
        replicate_response = self.fielddb.replicate_database(self.database_name,
            self.database_clone_name)
        new_database_list = self.fielddb.get_database_list()
        assert len(new_database_list) == len(database_list) + 2
        print '... Replicated database "%s".' % self.database_name

        # Get all documents in a database
        all_docs_list = self.fielddb.get_all_docs_list(self.database_name)
        assert len(all_docs_list) == 3
        print '... Got the three fruit documents in the database.'

        # Design Documents
        ########################################################################

        # Create a design document.
        data = {
            "_id": "_design/example",
            "views": {
                "foo": {
                    "map": "function(doc){emit(doc._id, doc._rev)}"
                },
                "add_syntactic_category": {
                    "map": open('views/add_syntactic_category/map.js',
                        'r').read()
                }
            }
        }
        dd_create_response = self.fielddb.create_document(self.database_name,
                data)
        assert dd_create_response['id'] == u'_design/example'
        assert dd_create_response['rev'][0] == u'1'
        print '... Created a design document.'

        # Get the first design document.
        view = self.fielddb.get_document(self.database_name,
            '_design/example/_view/foo')
        assert view.has_key('rows')
        print '... Got design document "foo".'

        # Get the second design document.
        view = self.fielddb.get_document(self.database_name,
            '_design/example/_view/add_syntactic_category')
        assert view.has_key('rows')
        print '... Got design document "add_syntactic_category".'

        # Clean Up.
        self.clean_up_couch()

        print 'Testing complete.'
        print


def add_optparser_options(parser):
    """Adds options to the optparser parser.

    """

    parser.add_option("-d", "--delete", default=None, metavar="USERNAME",
        help="username of a FieldDB user to be deleted along with all of their "
            "databases")

if __name__ == '__main__':

    """Use this module as a command-line utility. Basic usage is::

        $ ./fielddb-client.py config.json

    where `config.json is a JSON config file containing an object with the
    following attributes, the values of which are all strings::

        auth_protocol
        auth_host
        auth_port
        corpus_protocol
        corpus_host
        corpus_port
        couch_protocol
        couch_host
        couch_port
        username
        password
        admin_username
        admin_password

    """

    parser = optparse.OptionParser()
    add_optparser_options(parser)
    (options, args) = parser.parse_args()
    config_path = args[0] # required first argument

    fielddb_client = FieldDBClient(config_path)

    if getattr(options, 'delete', False):
        print 'Deleting user %s and all of their database.' % options.delete
        fielddb_client.login()
        fielddb_client.login_couchdb()
        fielddb_client.delete_user_and_corpora(options.delete)
    else:
        # Default behaviour is to run some tests.
        tester = FieldDBClientTester(fielddb_client)
        tester.test()

