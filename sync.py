#!/usr/bin/python -tt

import requests
import json
import os

from urlparse import urlparse

# Command line arguments follow the GNU conventions.
from getopt import gnu_getopt
from sys import argv, exit

def list_of_dicts_to_dict(lst, key, flt):
    return {sd[key]: {k: v for k, v in sd.iteritems() if k in flt} for sd in lst}

def lists_of_dicts_equal(l1, l2, key, flt):
    d1 = list_of_dicts_to_dict(l1, key, flt)
    d2 = list_of_dicts_to_dict(l2, key, flt)
    return d1 == d2

def filter_dict(d, flt):
    return {k:d[k] for k in d if k in flt}

def dicts_equal(d1, d2, flt):
    d1 = filter_dict(d1, flt)
    d2 = filter_dict(d2, flt)
    return d1 == d2

def parse_hash(value):
    try:
        return value.split(':',1)
    except ValueError:
        return [value, '']


class CkanApi:
    def __init__(self, url, key, temp_path):
        self.url = url
        self.key = key
        self.temp_path = temp_path
        self.headers = {'Authorization': self.key}

    def list_organizations(self):
        r = requests.get(self.url+'action/organization_list', headers=self.headers)
        return r.json()['result']

    def get_organization(self, id):
        r = requests.get(self.url+'action/organization_show', params={'id': id}, headers=self.headers)
        return r.json()

    def create_organization(self, data):
        r = requests.post(self.url+'action/organization_create', json=data, headers=self.headers)
        assert r.json().get('success')
        return r.json()

    def update_organization(self, data):
        r = requests.post(self.url+'action/organization_update', json=data, headers=self.headers)
        assert r.json().get('success')
        return r.json()

    def list_packages(self):
        r = requests.get(self.url+'action/package_list', headers=self.headers)
        return r.json()['result']

    def get_package(self, id):
        r = requests.get(self.url+'action/package_show', params={'id': id}, headers=self.headers)
        return r.json()

    def get_resources(self, package_id):
        return self.get_package(package_id)['result'].get('resources')

    def get_resource_by_hash(self, hash):
        r = requests.get(self.url+'action/resource_search', params={'query': 'hash:'+hash}, headers=self.headers)
        return r.json()

    def create_package(self, data):
        r = requests.post(self.url+'action/package_create', json=data, headers=self.headers)
        assert r.json().get('success')
        return r.json()

    def update_package(self, data):
        r = requests.post(self.url+'action/package_update', json=data, headers=self.headers)
        assert r.json().get('success')
        return r.json()

    def delete_package(self, name):
        params = {'id': name}
        r = requests.post(self.url+'action/package_delete', json=params, headers=self.headers)
        assert r.json().get('success')
        return r.json()

    def purge_package(self, name):
        params = {'id': name}
        r = requests.post(self.url+'action/dataset_purge', json=params, headers=self.headers)
        assert r.json().get('success')
        return r.json()

    def create_resource(self, data, files, json=False):
        datakey = 'json' if json else 'data'
        r = requests.post(self.url + 'action/resource_create', **{
            datakey: data,
            'files': files,
            'headers': self.headers,
        })
        assert r.json().get('success')
        return r.json()

    def update_resource(self, data, files, json=False):
        datakey = 'json' if json else 'data'
        r = requests.post(self.url + 'action/resource_update', **{
            datakey: data,
            'files': files,
            'headers': self.headers,
        })
        assert r.json().get('success')
        return r.json()

    def delete_resource(self, id):
        params = {'id': id}
        r = requests.post(self.url+'action/resource_delete', json=params, headers=self.headers)
        assert r.json().get('success')
        return r.json()

    def download(self, url, filename):
        r = requests.get(url)
        with open('%s/%s' % (self.temp_path, filename), 'wb') as fd:
            for chunk in r.iter_content(4096):
                fd.write(chunk)

    def empty_trash(self):
        parsed_uri = urlparse(self.url)
        trash_uri = '{uri.scheme}://{uri.netloc}/ckan-admin/trash'.format(uri=parsed_uri)
        r = requests.get(trash_uri, params={'purge-packages': 'purge'}, headers=self.headers)



def sync_organization(organization, source, dest):
    # How many changes were done.
    update_counter = 0
    s_org = source.get_organization(organization)['result']
    if (dest.get_organization(organization)['success'] == False):
        params = {
                'display_name': s_org['display_name'],
                'description': s_org['description'],
                'name': s_org['name'],
                'type': s_org['type'],
                'state': s_org['state'],
                'title': s_org['title'],
                'approval_status': s_org['approval_status'],
                'extras': []
                }
        for extra in s_org.get('extras', []):
            item = filter_dict(extra, ['value', 'state', 'key'])
            params['extras'].append(item)

        print 'Creating organization: %(name)r' % s_org
        update_counter += 1
        d_org = dest.create_organization(params)['result']
    else:
        d_org = dest.get_organization(organization)['result']
    if (not dicts_equal(s_org, d_org, ['title', 'display_name', 'description', 'state'])
        or not lists_of_dicts_equal(s_org['extras'], d_org['extras'], 'key', ['value', 'state', 'key'])):
        params = {
                'display_name': s_org['display_name'],
                'description': s_org['description'],
                'state': s_org['state'],
                'title': s_org['title'],
                'approval_status': s_org['approval_status'],
                'extras': []
                }
        for extra in s_org.get('extras', []):
            item = filter_dict(extra, ['value', 'state', 'key'])
            params['extras'].append(item)

        d_org.update(params)
        print 'Updating organization: %(name)r' % s_org
        update_counter += 1
        dest.update_organization(d_org)
    return update_counter


def sync_package(package, source, dest):
    # How many changes were done.
    update_counter = 0
    # Create package if it isn't in destination
    s_pack = source.get_package(package)['result']
    if (dest.get_package(package)['success'] == False):
        params = {
                'name': s_pack['name'],
                'title': s_pack['title'],
                'notes': s_pack['notes'],
                'owner_org': s_pack['organization']['name'],
                'ruian_code': s_pack.get('ruian_code'),
                'ruian_type': s_pack.get('ruian_type'),
                'spatial_uri': s_pack.get('spatial_uri'),
                'maintainer': s_pack.get('maintainer'),
                'maintainer_email': s_pack.get('maintainer_email'),
                'license_title': s_pack.get('license_title'),
                'license_id': s_pack.get('license_id'),
                'license_url': s_pack.get('license_url'),
                'license_link': s_pack.get('license_link'),
                'frequency': s_pack.get('frequency'),
                'author': s_pack.get('author'),
                'author_email': s_pack.get('author_email'),
                'temporal_start': s_pack.get('temporal_start'),
                'temporal_end': s_pack.get('temporal_end'),
                'publisher_name': s_pack.get('publisher_name'),
                'publisher_uri': s_pack.get('publisher_uri'),
                'schema': s_pack.get('schema'),
                'theme': s_pack.get('theme'),
                'tags': [{'state': tag['state'], 'display_name': tag['display_name'], 'name': tag['name']} for tag in s_pack.get('tags')],
                'extras': []
                }
        for extra in s_pack.get('extras', []):
            item = filter_dict(extra, ['value', 'key'])
            params['extras'].append(item)

        print 'Creating package: %(name)r' % s_pack
        update_counter += 1
        d_pack = dest.create_package(params)['result']
    else:
        d_pack = dest.get_package(package)['result']

    if (not dicts_equal(s_pack, d_pack, ['title', 'notes', 'ruian_code', 'ruian_type', 'maintainer', 'author', 'publisher_name', 'maintainer_email', 'license_id', 'license_url','temporal_start', 'temporal_end', 'schema', 'license_id', 'theme'])
        or not lists_of_dicts_equal(s_pack.get('extras', {}), d_pack.get('extras', {}), 'key', ['value', 'key'])):
        params = {
                'title': s_pack['title'],
                'notes': s_pack['notes'],
                'owner_org': s_pack['organization']['name'],
                'ruian_code': s_pack.get('ruian_code'),
                'ruian_type': s_pack.get('ruian_type'),
                'spatial_uri': s_pack.get('spatial_uri'),
                'maintainer': s_pack.get('maintainer'),
                'maintainer_email': s_pack.get('maintainer_email'),
                'license_title': s_pack.get('license_title'),
                'license_id': s_pack.get('license_id'),
                'license_url': s_pack.get('license_url'),
                'license_link': s_pack.get('license_link'),
                'frequency': s_pack.get('frequency'),
                'author': s_pack.get('author'),
                'author_email': s_pack.get('author_email'),
                'temporal_start': s_pack.get('temporal_start'),
                'temporal_end': s_pack.get('temporal_end'),
                'publisher_name': s_pack.get('publisher_name'),
                'publisher_uri': s_pack.get('publisher_uri'),
                'schema': s_pack.get('schema'),
                'theme': s_pack.get('theme'),
                'extras': []
                }
        for extra in s_pack.get('extras', []):
            item = filter_dict(extra, ['value', 'key'])
            params['extras'].append(item)

        d_pack.update(params)
        print 'Updating package: %(name)r' % s_pack
        update_counter += 1
        dest.update_package(d_pack)

    # Update resources
    for s_resource in s_pack['resources']:
        d_result = dest.get_resource_by_hash(s_resource['id'])
        try:
            d_resource = d_result['result']['results'][0]
        except IndexError:
            d_resource = []

        # Download resource
        filename = '%s-%s' % (s_resource['id'], os.path.basename(s_resource['url']))

        if 'praha.eu' in s_resource['url']:
            source.download(s_resource['url'], filename)

            # Reupload it
            data = {'package_id': d_pack['id'],
                    'name': s_resource['name'],
                    'format': s_resource['format'],
                    'hash': '%s:%s' % (s_resource['id'], s_resource['last_modified']) ,
                    'position': s_resource['position'],
                    'description': s_resource['description'],
                    'last_modified': s_resource['last_modified'],
                    'url': ''
                    }
            files = [('upload', file("%s/%s" % (source.temp_path, filename)))]
            s_json = False
        else:
            data = {'package_id': d_pack['id'],
                    'name': s_resource['name'],
                    'format': s_resource['format'],
                    'hash': '%s:%s' % (s_resource['id'], s_resource['last_modified']) ,
                    'position': s_resource['position'],
                    'description': s_resource['description'],
                    'last_modified': s_resource['last_modified'],
                    'url': s_resource['url']
                    }
            files = []
            s_json = True

        if d_result['result']['count'] == 0:
            print 'Create resource: %(name)r' % s_resource
            update_counter += 1
            dest.create_resource(data, files, json=s_json)
        elif not dicts_equal(s_resource, d_resource, ['name', 'description', 'position', 'last_modified']):
            s_id, last_modified = parse_hash(d_resource['hash'])
            if last_modified != s_resource['last_modified']:
                print 'Update resource: %(name)r' % s_resource
                data['id'] = d_resource.get('id')
                update_counter += 1
                dest.update_resource(data, files, json=s_json)

        if 'praha.eu' in s_resource['url']:
            # Delete downloaded file
            os.remove("%s/%s" % (source.temp_path, filename))


    # Delete unwanted resources
    s_ids = [v['id'] for v in s_pack['resources']]
    d_hashes = {parse_hash(v['hash'])[0]: v for v in d_pack['resources']}
    for d_hash, res in d_hashes.iteritems():
        if d_hash not in s_ids:
            print 'Delete resource: %(name)r' % res
            update_counter += 1
            dest.delete_resource(res['id'])
    return update_counter


def sync_all(source, dest):
    update_counter = 0

    source_orgs = source.list_organizations()
    for organization in source_orgs:
        print 'Syncing org: %r' % organization 
        update_counter += sync_organization(organization, source, dest)

    source_pckgs = source.list_packages()
    for package in source_pckgs:
        print 'Syncing pkg: %r' % package
        update_counter += sync_package(package, source, dest)

    # Delete packages that shouldn't be there
    for package in dest.list_packages():
        if package not in source_pckgs:
            print 'Deleting pkg: %r' % package
            dest.purge_package(package)
            update_counter += 1
    return update_counter


def print_help():
    print 'Usage: '+argv[0]+' [OPTIONS]'
    print 'Runs CKAN sync script with given options.'
    print ''
    print 'OPTIONS:'
    print '  --help, -h                 Display this help.'
    print ''
    print '  --source, -s               URL of source CKAN.'
    print '  --source-api-key, -S       API key for source CKAN.'
    print ''
    print '  --destination, -d          URL of source CKAN.'
    print '  --destination-api-key, -D  API key for source CKAN.'
    print ''
    print '  --temporary-path, -t       Path to save temporary files.'


if __name__ == '__main__':

    opts, args = gnu_getopt(argv, 'hs:S:d:D:t:', 
            ['help', 'source=', 'source-api-key=', 'destination=',
             'destination-api-key=', 'temporary-path='])

    source_api = ''
    source_api_key = ''
    dest_api = ''
    dest_api_key = ''
    temp_path = '.'

    for o, a in opts:

        if o in ('-s', '--source'):
            source_api = a
        elif o in ('-S', '--source-api-key'):
            source_api_key = a
        elif o in ('-d', '--destination'):
            dest_api = a
        elif o in ('-D', '--destination-api-key'):
            dest_api_key = a
        elif o in ('-t', '--temporary-path'):
            temp_path = a
        elif o in ('-h', '--help'):
            print_help()
            exit()
    if not opts:
        print_help()
        exit(1)


    # Define source and destination
    source = CkanApi(source_api, source_api_key, temp_path)
    dest = CkanApi(dest_api, dest_api_key, temp_path)

    # Empty trash before sync
    source.empty_trash()
    dest.empty_trash()

    # Do the sync
    changes = sync_all(source, dest)

    # Empty trash after sync
    source.empty_trash()
    dest.empty_trash()

    if changes > 0:
        exit(1)
    else:
        exit(0)

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
