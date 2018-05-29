#!/usr/bin/env python3


import argparse
import configparser
import datetime
import logging
import os
import re
import requests
import sys
import time

from urllib.parse import urlparse


class Organization(dict):
    """Copy of CKAN organization dict containing only non-internal attributes
    and with sorted extras list. That allows simple comparison with other
    instances.
    """
    def __init__(self, org_dict):
        super().__init__()
        keys = [
            'approval_status',
            'description',
            'display_name',
            'name',
            'state',
            'title',
            'type'
        ]
        self.update({k: org_dict.get(k) for k in keys})
        extras = [{k: ed.get(k) for k in ['key', 'value', 'state']}
                    for ed in org_dict.get('extras', [])]
        self.update({'extras': sorted(extras, key=lambda x: x['key'])})

        self._image_url = org_dict.get('image_display_url')

    @property
    def image_name(self):
        filename = os.path.basename(self._image_url)
        orig_name = re.sub(r'^[\d-]+\.\d{6}', '', filename)
        return orig_name


class PackageMetadata(dict):
    """Copy of CKAN package dict containing only non-internal package metadata
    (i.e., no resources).
    """
    def __init__(self, package_dict):
        super().__init__()
        keys = [
            'author',
            'author_email',
            'frequency',
            'license_id',
            'license_link',
            'license_title',
            'license_url',
            'maintainer',
            'maintainer_email',
            'name',
            'notes',
            'publisher_name',
            'publisher_uri',
            'ruian_code',
            'ruian_type',
            'schema',
            'spatial_uri',
            'state',
            'temporal_start',
            'temporal_end',
            'theme',
            'title',
            'url',
            'version',
        ]
        self.update({k: package_dict.get(k) for k in keys})
        extras = [{k: ed.get(k) for k in ['key', 'value']}
                    for ed in package_dict.get('extras', [])]
        tags = [{k: td.get(k) for k in ['display_name', 'state', 'name']}
                    for td in package_dict.get('tags', [])]
        self.update({
            'extras': sorted(extras, key=lambda x: x['key']),
            'tags': sorted(tags, key=lambda x: x['display_name']),
            'owner_org': package_dict['organization']['name'],
        })

        # Strip leading and trailing whitespace from all string values.
        # CKAN 2.8 validates 'author_email' more strictly than previous
        # versions, for example.
        for k, v in self.items():
            if isinstance(v, str):
                self[k] = v.strip()


class Resource(dict):
    """Dict representing CKAN resource.

    Resources have only internal unique IDs (the 'name' field is not
    unique here, in contrast to organizations and packages) and
    can't be always simply compared between CKAN instances, therefore
    the 'hash' field of synced target repository is used for storing
    the internal ID & last revision of this resource in source repo.
    """
    def __init__(self, resource_dict, package_name):
        super().__init__()
        self.update(resource_dict)
        self['package_id'] = package_name
        self.update(self.parse_hash())

    def parse_hash(self):
        try:
            oid, orev = self.get('hash').split(':', 1)
        except ValueError:  # hash is empty
            oid, orev = '', ''
        return {'original_id': oid, 'original_revision': orev}

    def create_hash(self):
        return '%s:%s' % (self['id'], self['revision_id'])

    def create_filename(self):
        return '%s-%s' % (self['id'], os.path.basename(self['url']))

    def same_as_source(self, source_res):
        return (self['original_id'] == source_res['id']) and \
               (self['original_revision'] in (source_res['revision_id'],
                                              source_res['last_modified']))

    def for_upload(self):
        upload_dict = {k: self.get(k) for k in [
            'describedBy',
            'describedByType',
            'description',
            'format',
            'license_link',
            'name',
            'package_id',
            'position',
            'temporal_start',
            'temporal_end',
        ]}
        upload_dict['url'] = '' if self['url_type'] == 'upload' else self['url']
        upload_dict['hash'] = self.create_hash()
        return upload_dict


class CkanApi:
    """CKAN API wrapper.
    """
    def __init__(self, api_url, api_key=None):
        self.api_url = api_url
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': self.api_key,
            'User-Agent': 'ckan-sync',
        })

    def __str__(self):
        return "<CkanApi %s>" % self.api_url

    def api_action(self, action, **kwargs):
        url = '/'.join([self.api_url.strip('/'), 'action', action])
        if ('json' in kwargs) or ('data' in kwargs):
            r = self.session.post(url=url, **kwargs)
            r.raise_for_status()
            if not r.json().get('success'):
                raise Exception('POST request failed', r.text)
        else:
            r = self.session.get(url=url, **kwargs)
        return r.json()

    # Organizations

    def list_organizations(self):
        return self.api_action('organization_list').get('result')

    def get_organization(self, org_name):
        return self.api_action(
            'organization_show', params={'id': org_name}).get('result')

    def create_organization(self, org_dict):
        logging.info('Creating organization %(name)s' % org_dict)
        return self.api_action('organization_create', json=org_dict)

    def patch_organization(self, org_dict, files=None):
        logging.info('Updating organization %(name)s' % org_dict)
        if 'id' not in org_dict.keys():
            org_dict['id'] = org_dict['name']
        datakey = 'data' if files else 'json'
        return self.api_action(
            'organization_patch', **{datakey: org_dict, 'files': files})

    # Packages

    def list_packages(self):
        return self.api_action('package_list').get('result')

    def get_package(self, package_name):
        return self.api_action(
            'package_show', params={'id': package_name}).get('result')

    def create_package(self, package_dict):
        logging.info('Creating package %(name)s' % package_dict)
        if 'owner_org' not in package_dict.keys():
            raise ValueError('Package must contain "owner_org" key')
        return self.api_action('package_create', json=package_dict)

    def patch_package(self, package_dict):
        logging.info('Updating package %(name)s' % package_dict)
        if 'id' not in package_dict.keys():
            package_dict['id'] = package_dict['name']
        return self.api_action('package_patch', json=package_dict)

    def purge_package(self, package_name):
        logging.info('Purging package %s', package_name)
        return self.api_action('dataset_purge', json={'id': package_name})

    # Resources

    def create_resource(self, resource_dict, files=None):
        logging.info('Creating resource %(name)s' % resource_dict)
        datakey = 'data' if files else 'json'
        return self.api_action(
            'resource_create', **{datakey: resource_dict, 'files': files})

    def update_resource(self, resource_dict, files=None):
        assert 'id' in resource_dict.keys()
        logging.info('Updating resource %(id)s' % resource_dict)
        datakey = 'data' if files else 'json'
        return self.api_action(
            'resource_update', **{datakey: resource_dict, 'files': files})

    def delete_resource(self, resource_id):
        logging.info('Deleting resource %s', resource_id)
        return self.api_action('resource_delete', json={'id': resource_id})

    # Revisions

    def get_revision(self, revid):
        return self.api_action(
            'revision_show', params={'id': revid}).get('result')

    def collect_revisions(self, since_time=None, since_id=None):
        # CKAN behavior: if both params given, only since_id is used
        if not (since_time or since_id):
            raise ValueError('Cannot collect revisions'
             ' - missing required param (since_id or since_time)')
        logging.info('Collecting revisions since %s', since_id or since_time)
        revisions = []
        params = {
            'sort': 'time_asc',
            'since_id': since_id,
            'since_time': since_time}
        batch = self.api_action('revision_list', params=params)['result']
        while len(batch) > 0:
            revisions.extend(batch)
            params = {'sort': 'time_asc', 'since_id': batch[-1]}
            batch = self.api_action('revision_list', params=params)['result']
        logging.info('Found %s revisions', len(revisions))
        return revisions

    def collect_changes_from_revisions(self, revision_list):
        '''Collects changes of both organizations and packages (including
        deletions), but needs to check every revision, hence suitable only
        for short term checks/small number of revisions.
        '''
        logging.info('Collecting changes from revision list.')
        orgs = set()
        packages = set()
        for rev in revision_list:
            rev_details = self.get_revision(rev)
            orgs.update(rev_details['groups'])
            packages.update(rev_details['packages'])
        logging.info(
            'Found revisions of these organizations: %s and packages: %s',
            orgs, packages)
        return (orgs, packages)

    def collect_changed_packages(self, since_time):
        '''Collects packages created/updated since given time,
        without deleted ones.
        Returns list of full package dicts as returned by CKAN API.
        '''
        def convert_time(timestamp):
            time_tuple = re.split('[^\d]+', timestamp)
            return datetime.datetime(*map(int, time_tuple))

        def filter_changed_packages(package_list, since_datetime):
            return [pack for pack in package_list
                if (convert_time(pack['metadata_modified']) > since_datetime)]

        logging.info('Collecting packages changed since %s', since_time)
        changed_packages = []
        action = 'current_package_list_with_resources'
        limit, offset = 20, 0
        since_datetime = convert_time(since_time)
        batch = self.api_action(
            action, params={'limit': limit})['result']
        packs = filter_changed_packages(batch, since_datetime)
        changed_packages.extend(packs)
        while len(packs) == limit:
            offset += limit
            batch = self.api_action(
                action, params={'limit': limit, 'offset': offset})['result']
            packs = filter_changed_packages(batch, since_datetime)
            changed_packages.extend(packs)
        logging.info('Found %s changed packages', len(changed_packages))
        return changed_packages


    def empty_trash(self):
        parsed_uri = urlparse(self.api_url)
        trash_uri = '{uri.scheme}://{uri.netloc}/ckan-admin/trash'.format(
            uri=parsed_uri)
        res = requests.get(
            trash_uri,
            params={'purge-packages': 'purge'},
            headers = {'Authorization': self.api_key})


class CkanSync:
    def __init__(self, source, target, since_id=None, since_time=None, temp_path=None):
        self.source = source
        self.target = target
        self.since_id = since_id
        self.since_time = since_time
        self.temp_path = temp_path

    # Synchronization of organizations, packages & resources

    def sync_org(self, org_name):
        def sync_image():
            image_file = self.download_file(s_org._image_url, s_org.image_name)
            files = [('image_upload', open(image_file, 'rb'))]
            self.target.patch_organization({'name': org_name}, files)
            os.remove(image_file)

        logging.info('Syncing organization %s', org_name)
        s_org = Organization(self.source.get_organization(org_name))
        t_org_full = self.target.get_organization(org_name)
        if not t_org_full:
            self.target.create_organization(s_org)
            # image upload must be done in this extra step, there is collision
            # with processing of organization 'extras' otherwise
            sync_image()
        else:
            t_org = Organization(t_org_full)
            if s_org != t_org:
                self.target.patch_organization(s_org)
            if s_org.image_name != t_org.image_name:
                sync_image()


    def sync_package(self, package):
        '''Param package: either full package dict returned by source CKAN,
        or just package name.
        '''
        if type(package) == dict:
            s_pack = package
            package_name = s_pack['name']
        else:
            package_name = package
            s_pack = self.source.get_package(package_name)
        logging.info('Syncing package %s', package_name)
        t_pack = self.target.get_package(package_name)
        if s_pack:
            # metadata
            s_package_meta = PackageMetadata(s_pack)
            if not t_pack:
                self.target.create_package(s_package_meta)
            else:
                if s_package_meta != PackageMetadata(t_pack):
                    if s_package_meta['state'] == 'deleted':
                        self.target.purge_package(package_name)
                        return
                    else:
                        self.target.patch_package(s_package_meta)
            # resources
            s_resources = s_pack.get('resources', [])
            t_resources = t_pack.get('resources', []) if t_pack else []
            self.sync_package_resources(s_resources, t_resources, package_name)
        else:   # package not found in source CKAN
            if t_pack:
                self.target.purge_package(package_name)
            else:
                logging.warning(
                    'Package %s not found in either repo; doing nothing',
                    package_name)

    def sync_package_resources(self, source_reslist, target_reslist, package_name):
        logging.info('Syncing resources of package %s', package_name)

        # 1. prepare dict of target resources, where the keys are their
        #    original IDs in source CKAN
        t_resources_by_oid = dict()
        for target_res in target_reslist:
            res = Resource(target_res, package_name)
            oid = res['original_id']
            if len(oid) == 0:
                # empty hash => resource can't be matched across CKAN instances
                self.target.delete_resource(res['id'])
            else:
                t_resources_by_oid[oid] = res

        # 2. process list of resources present in source repo
        for source_res in source_reslist:
            files = None
            s_res = Resource(source_res, package_name)
            if s_res['id'] not in t_resources_by_oid:  # create
                if s_res['url_type'] == 'upload':
                    downloaded = self.download_file(
                        s_res['url'], s_res.create_filename())
                    files = [('upload', open(downloaded, 'rb'))]
                self.target.create_resource(s_res.for_upload(), files)
            else:
                t_res = t_resources_by_oid.pop(s_res['id'])
                if not t_res.same_as_source(s_res):     # update
                    if s_res['url_type'] == 'upload':
                        downloaded = self.download_file(
                            s_res['url'], s_res.create_filename())
                        files = [('upload', open(downloaded, 'rb'))]
                    res_upload = s_res.for_upload()
                    res_upload['id'] = t_res['id']
                    self.target.update_resource(res_upload, files)
            if files is not None:
                os.remove(downloaded)

        # 3. delete remaining target resources
        for t in t_resources_by_oid.values():
            self.target.delete_resource(t['id'])

    def download_file(self, url, filename):
        location = '%s/%s' % (self.temp_path, filename)
        r = requests.get(url)
        with open(location, 'wb') as fd:
            for chunk in r.iter_content(4096):
                fd.write(chunk)
        return location

    def sync_orgs_and_packages(self, orgs, packages):
        for org in orgs:
            self.sync_org(org)
        for package in packages:
            self.sync_package(package)

    # Full/partial synchronization of CKAN instances

    def sync_full(self):
        logging.info(
            'Started full sync from %s to %s', self.source, self.target)
        source_orgs = self.source.list_organizations()
        source_packages = self.source.list_packages()
        self.sync_orgs_and_packages(source_orgs, source_packages)
        for package in self.target.list_packages():
            if package not in source_packages:
                self.target.purge_package(package)
        logging.info('Full sync completed')

    def sync_packages_only(self):
        logging.info('Syncing packages since %s', self.since_time)
        changed_packages = self.source.collect_changed_packages(self.since_time)
        for package in changed_packages:    # only created or updated ones
            self.sync_package(package)
        to_delete = set(self.target.list_packages()) - set(self.source.list_packages())
        for package in to_delete:
            self.target.purge_package(package)
        logging.info('Packages sync completed')

    def sync(self):
        logging.info('Started sync from %s to %s', self.source, self.target)
        if not (self.since_id or self.since_time):
            return self.sync_full()
        revisions = self.source.collect_revisions(
            since_id=self.since_id, since_time=self.since_time)
        if len(revisions) == 0:
            logging.info('No new revisions of source CKAN found; doing nothing.')
            return
        elif len(revisions) > 200:
            logging.info('Too many revisions; doing full sync instead.')
            return self.sync_full()
        else:
            # collects deleted items too
            orgs, packages = self.source.collect_changes_from_revisions(revisions)
            self.sync_orgs_and_packages(orgs, packages)
            logging.info('Sync completed')

    def sync_loop(self, sleep=60):
        if not self.since_id:
            last_revid = self.source.api_action('revision_list')['result'][0]
            self.sync_full()
        else:
            last_revid = self.since_id

        while True:
            try:
                new_revs = self.source.collect_revisions(since_id=last_revid)
                if len(new_revs) > 0:
                    orgs, packages = self.source.collect_changes_from_revisions(
                        new_revs)
                    self.sync_orgs_and_packages(orgs, packages)
                    last_revid = new_revs[-1]
                time.sleep(sleep)
            except:
                logging.exception('Sync loop failed')
                sys.exit(1)


def main():
    def interval_to_timestamp(time_interval):
        udict = {
            'm': 'minutes',
            'h': 'hours',
            'd': 'days'
        }
        match = re.match(r'^(\d+)([mhd])$', time_interval)
        if not match:
            raise ValueError('Wrong time interval format, see help')
        amount = int(match.group(1))
        units = udict.get(match.group(2))
        delta = datetime.timedelta(**{units: amount})
        since_time = datetime.datetime.utcnow() - delta
        return since_time.isoformat()

    logformat = '%(asctime)-15s %(levelname)s %(message)s'
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=logformat)

    parser = argparse.ArgumentParser(
        description='CKAN synchronization',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('config_file', help='path to config file')
    parser.add_argument('--since-time', '-t',
        help='check for changes in last N minutes/hours/days only '
             'format: N[mhd] '
             'example: -t5m  = sync items changed within last 5 minutes')
    parser.add_argument('--since-id', '-i',
        help='start synchronization since this revision id')

    loop = parser.add_argument_group('loop mode')
    loop.add_argument('--loop', '-l', action='store_true',
        default=False, help='run in loop and periodically check for changes')
    loop.add_argument('--sleep', '-s',
        default=60, help='time interval for periodic checks (in seconds)')

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config_file)

    source = CkanApi(**dict(config.items('source')))
    target = CkanApi(**dict(config.items('target')))
    since_time = interval_to_timestamp(args.since_time) if args.since_time else None
    temp_path = config['sync']['temp_path']
    sync = CkanSync(
        source,
        target,
        since_id=args.since_id,
        since_time=since_time,
        temp_path=temp_path)

    source.empty_trash()
    target.empty_trash()

    if args.loop:
        sync.sync_loop(sleep=int(args.sleep))
    else:
        sync.sync()


if __name__ == '__main__':
    main()


