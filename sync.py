#!/usr/bin/python -tt

import requests
import json
import os

# Command line arguments follow the GNU conventions.
from getopt import gnu_getopt
from sys import argv, exit


def cmp_filter(dict1, dict2, filter):
    """ Returns dicts cmp() comparing only keys in filter """
    dict1 = {k:dict1[k] for k in dict1 if k in filter}
    dict2 = {k:dict2[k] for k in dict2 if k in filter}
    return cmp(dict1, dict2)

class CkanApi:
    def __init__(self, url, key, temp_path):
        self.url = url
        self.key = key
        self.temp_path = temp_path
        self.headers = {'Authorization': self.key}

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
        return r.json()

    def update_package(self, data):
        r = requests.post(self.url+'action/package_update', json=data, headers=self.headers)
        return r.json()

    def delete_package(self, name):
        params = {'id': name}
        r = requests.post(self.url+'action/package_delete', json=params, headers=self.headers)
        return r.json()

    def create_resource(self, data, files):
        r = requests.post(self.url+'action/resource_create',
                data=data,
                files=files,
                headers=self.headers)
        return r.text

    def update_resource(self, data, files):
        r = requests.post(self.url+'action/resource_update',
                data=data,
                files=files,
                headers=self.headers)
        return r.text

    def delete_resource(self, id):
        params = {'id': id}
        r = requests.post(self.url+'action/resource_delete', json=params, headers=self.headers)
        return r.json()

    def download(self, url, filename):
        r = requests.get(url)
        with open('%s/%s' % (self.temp_path, filename), 'wb') as fd:
            for chunk in r.iter_content(4096):
                fd.write(chunk)

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
                'extras': [{'key': 'source', 'value': s_pack['id']}]
                }
        print 'Creating package: %(name)s' % s_pack
        update_counter += 1
        d_pack = dest.create_package(params)['result']
    else:
        d_pack = dest.get_package(package)['result']

    if (cmp_filter(s_pack, d_pack, ['title', 'notes']) != 0):
        params = {
                'title': s_pack['title'],
                'notes': s_pack['notes'],
                'owner_org': s_pack['organization']['name'],
                }
        d_pack.update(params)
        print 'Updating package: %(name)s' % s_pack
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
        filename = '%s-%s'%(s_resource['id'], os.path.basename(s_resource['url']))
        source.download(s_resource['url'], filename)
        # Reupload it
        data = {'package_id': d_pack['id'],
                'name': s_resource['name'],
                'format': s_resource['format'],
                'hash': s_resource['id'],
                'position': s_resource['position'],
                'description': s_resource['description'],
                'last_modified': s_resource['last_modified'],
                'url': ''
                }
        files = [('upload', file(filename))]

        if (d_result['result']['count'] == 0):
            print 'Create resource: %(name)s' % s_resource
            update_counter += 1
            dest.create_resource(data, files)
        elif (cmp_filter(s_resource, d_resource, ['name', 'description', 'position', 'last_modified']) != 0):
            print 'Update resource: %(name)s' % s_resource
            data['id'] = d_resource.get('id')
            update_counter += 1
            dest.update_resource(data, files)
        # Delete downloaded file
        os.remove(filename)


    # Delete unwanted resources
    s_ids = [v['id'] for v in s_pack['resources']]
    d_hashes = {v['hash']: v for v in d_pack['resources']}
    for d_hash, res in d_hashes.iteritems():
        if d_hash not in s_ids:
            print 'Delete resource: %(name)s' % res
            update_counter += 1
            dest.delete_resource(res['id'])
    return update_counter


def sync_all(source, dest):
    update_counter = 0
    # Delete packages that shouldn't be there
    source_pckgs = source.list_packages()
    for package in source_pckgs:
        print 'Syncing: %s' % package
        update_counter += sync_package(package, source, dest)

    for package in dest.list_packages():
        if package not in source_pckgs:
            print 'Deleting: %s' % package
            update_counter += dest.delete_package(package)
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

    source = CkanApi(source_api, source_api_key, temp_path)
    dest = CkanApi(dest_api, dest_api_key, temp_path)
    if sync_all(source, dest) > 0:
        exit(1)
    else:
        exit(0)

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
