"""This script was written for the express purpose of fixing the tags in a
particular OLD after the lingsync2old.py migration script failed to do so.

It SHOULD NOT BE RUN on an existing OLD without careful inspection first since
doing so may cause data corruption.

"""

import json, pprint, re
from old_client import OLDClient


def get_correct_tags(): 

    correct_tags = set()
    datum_ids2tag_set = {}

    with open('tag-fix-data.json') as f:
        tag_fix_data = json.load(f)
    for tag_original, tag_meta in tag_fix_data.iteritems():
        correct_tag_set = [tn.strip() for tn in tag_original.split(',') if \
            tn.strip()]
        for tag in correct_tag_set:
            correct_tags.add(tag)
        for datum_id in tag_meta['datum_ids']:
            datum_ids2tag_set.setdefault(datum_id, [])
            datum_ids2tag_set[datum_id] += correct_tag_set
            datum_ids2tag_set[datum_id] = list(set(datum_ids2tag_set[datum_id]))
    return correct_tags, datum_ids2tag_set

def get_current_tags(c):
    return c.get('tags')


def login():
    c = OLDClient('<URL>')
    logged_in = c.login('<USERNAME>', '<PASSWORD>')
    if not logged_in:
        sys.exit(u'%sUnable to log in to %s with username %s and password %s.'
            u' Aborting.%s' % (ANSI_FAIL, old_url, old_username, old_password,
            ANSI_ENDC))
    return c


def delete_current_tags(current_tags, c):
    for tag in current_tags:
        if tag['id'] > 3:
            r = c.delete('tags/%d' % tag['id'])
            if r.get('id') == tag['id']:
                print 'Deleted tag %d: %s' % (tag['id'], tag['name'])


def create_correct_tags(correct_tags, c):
    tag_name2id = {}
    for tag_name in correct_tags:
        tag = {
            'name': tag_name,
            'description': u''
        }
        r = c.create('tags', tag)
        if r.get('id'):
            tag_name2id[tag_name] = r['id']
        else:
            print
            print 'FAIL: failed to create a tag named %s' % tag_name
            print r
            print
    return tag_name2id


def get_tag_name2id(c):
    tag_name2id = {}
    current_tags = get_current_tags(c)
    for tag in current_tags:
        tag_name2id[tag['name']] = tag['id']
    return tag_name2id


def main():

    c = login()

    # 1. Get all current tags.
    # current_tags = get_current_tags(c)
    # print '\n\n\n'
    # print 'Current Tags'
    # print '\n'.join(sorted([t['name'] for t in current_tags]))
    # print '\n\n\n'

    # 2. Delete all current tags, except the import tag.
    # delete_current_tags(current_tags, c)

    # 3. Get the correct tags and a mapper from datum ids to lists of tags.
    correct_tags, datum_ids2tag_set = get_correct_tags()
    # print '\n'.join(sorted(list(correct_tags)))
    # print '\n\n\n'
    # for datum_id, tag_list in datum_ids2tag_set.iteritems():
    #     print '%s\n    %s' % (datum_id, ' | '.join(tag_list))

    # 4. Create all of the correct tags.
    # WARNING: don't call this twice!
    # tag_name2id = create_correct_tags(correct_tags, c)

    tag_name2id = get_tag_name2id(c)
    # pprint.pprint(tag_name2id)

    # 5. Get all forms
    forms = c.get('forms')

    with open('tag-fix-data.json') as f:
        tag_fix_data = json.load(f)

    # 6. Update forms with correct tags, based on datum id.
    p = re.compile('This form was created from LingSync datum ([abcdefABCDEF0123456789]+)')
    for form in forms:
        if 'This form was created from LingSync datum' in form['comments']:
            if p.search(form['comments']):
                datum_id = p.search(form['comments']).group(1)
                tag_set = datum_ids2tag_set.get(datum_id)
                if tag_set:
                    tags = [tag['id'] for tag in form['tags']]
                    for tag_name in tag_set:
                        tags.append(tag_name2id[tag_name])
                    # print ('Form "%s" (from datum %s) should have tags:\n'
                    #     '    "%s"\n    %s\n' % (form['transcription'], datum_id, 
                    #     '", "'.join(tag_set), ', '.join([str(i) for i in tags])))

                    form['tags'] = tags
                    if form['elicitation_method']:
                        form['elicitation_method'] = form['elicitation_method']['id']
                    if form['syntactic_category']:
                        form['syntactic_category'] = form['syntactic_category']['id']
                    if form['speaker']:
                        form['speaker'] = form['speaker']['id']
                    if form['elicitor']:
                        form['elicitor'] = form['elicitor']['id']
                    if form['verifier']:
                        form['verifier'] = form['verifier']['id']
                    if form['source']:
                        form['source'] = form['source']['id']
                    if form['files']:
                        form['files'] = [t['id'] for t in form['files']]
                    if form['date_elicited']:
                        x = form['date_elicited']
                        if len(x.split('-')) == 3:
                            y, m, d = x.split('-')
                            form['date_elicited'] = u'%s/%s/%s' % (m, d, y)
                    r = c.update('forms/%d' % form['id'], form)
                    if not r.get('id'):
                        print '\n\nFailed to update form %d' % form['id']
                        print r
                        print '\n\n'


if __name__ == '__main__':
    main()
