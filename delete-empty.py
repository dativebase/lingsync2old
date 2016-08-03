"""Simple script to delete all forms with "PLACEHOLDER" as their transcription
and translation value.
"""
import sys
import json
from old_client import OLDClient

url = 'URL'
username = 'USERNAME'
password = 'PASSWORD'
c = OLDClient(url)

logged_in = c.login(username, password)
if not logged_in:
    sys.exit('Could not log in')

search = {
    "query": {
        "filter": ['and', [
            ['Form', 'transcription', '=', 'PLACEHOLDER'],
            ['Form', 'translations', 'transcription', '=', 'PLACEHOLDER']
        ]]
        }
    }
empty_forms = c.search('forms', search)
print 'Deleting %d forms.' % len(empty_forms)

deleted_count = 0
for form in empty_forms:
    delete_path = 'forms/%d' % form['id']
    resp = c.delete(delete_path)
    if (type(resp) is not dict) or resp['id'] != form['id']:
        print 'Failed to delete form %d' % form['id']
    else:
        deleted_count += 1

print 'Deleted %d forms.' % deleted_count


