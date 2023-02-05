import logging
from ipaddress import ip_address
from typing import Optional

import requests
from cinder.volume.drivers.rmb938.truenas.api.objects.dataset import Dataset, DatasetType
from cinder.volume.drivers.rmb938.truenas.api.objects.iscsi import ISCSIGlobal, ISCSIPortal, ISCSIPortalListen, \
    ISCSITarget, ISCSIExtent
from requests.compat import urljoin, quote_plus

LOG = logging.getLogger(__name__)


class TrueNASAPIClient(object):

    def __init__(self, url: str, api_key: str):
        self.__url = urljoin(url, "/api/v2.0/")

        self.__client_session = requests.Session()
        self.__client_session.headers.update({'Authorization': 'Bearer %s' % api_key})
        # TODO: ssl configuration if it's provided

    def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        url = urljoin(self.__url, "pool/dataset/id/%s" % quote_plus(dataset_id))
        resp = self.__client_session.get(url)
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error getting dataset %s: %s" % (dataset_id, resp.text))
            resp.raise_for_status()

        output_data = resp.json()
        if output_data['type'] == 'VOLUME':
            dataset_type = DatasetType.VOLUME
        elif output_data['type'] == 'FILESYSTEM':
            dataset_type = DatasetType.FILESYSTEM
        else:
            raise ValueError('Unknown dataset type %s for dataset id %s' % (output_data['type'], dataset_id))

        size = 0
        if 'quota' in output_data:
            size = int(output_data['quota']['rawvalue'])
        elif 'volsize' in output_data:
            size = int(output_data['volsize']['rawvalue'])

        return Dataset(
            id=output_data['id'],
            type=dataset_type,
            size=size,
            used=int(output_data['used']['rawvalue']),
            origin=output_data['origin']['value']
        )

    def create_zvol(self, name: str, size: int, sparse: bool):
        url = urljoin(self.__url, "pool/dataset")
        zvol_props = {
            "name": name,
            "type": 'VOLUME',
            "volsize": size,
            "sparse": sparse
        }
        resp = self.__client_session.post(url, json=zvol_props)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error creating zvol %s: %s" % (zvol_props, resp.text))
            resp.raise_for_status()

    def expand_zvol(self, dataset_id, size: int):
        url = urljoin(self.__url, "pool/dataset/id/%s" % quote_plus(dataset_id))
        zvol_props = {
            "volsize": size,
        }
        resp = self.__client_session.put(url, json=zvol_props)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error expanding zvol %s: %s" % (zvol_props, resp.text))
            resp.raise_for_status()

    def delete_dataset(self, dataset_id):
        url = urljoin(self.__url, "pool/dataset/id/%s" % quote_plus(dataset_id))
        resp = self.__client_session.delete(url)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error deleting dataset %s: %s" % (dataset_id, resp.text))
            resp.raise_for_status()

    def create_snapshot(self, name: str, dataset: str):
        url = urljoin(self.__url, "zfs/snapshot")
        snapshot_props = {
            "dataset": dataset,
            "name": name
        }
        resp = self.__client_session.post(url, json=snapshot_props)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error creating snapshot %s: %s" % (snapshot_props, resp.text))
            resp.raise_for_status()

    def delete_snapshot(self, snapshot_id: str):
        url = urljoin(self.__url, "zfs/snapshot/id/%s" % quote_plus(snapshot_id))
        resp = self.__client_session.delete(url)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error deleting snapshot %s: %s" % (snapshot_id, resp.text))
            resp.raise_for_status()

    def clone_snapshot(self, snapshot_id: str, dataset_id: str):
        url = urljoin(self.__url, "zfs/snapshot/clone")
        snapshot_props = {
            "snapshot": snapshot_id,
            "dataset_dst": dataset_id
        }
        resp = self.__client_session.post(url, json=snapshot_props)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error cloning snapshot %s: %s" % (snapshot_props, resp.text))
            resp.raise_for_status()

    def get_iscsi_global(self) -> ISCSIGlobal:
        url = urljoin(self.__url, "iscsi/global")
        resp = self.__client_session.get(url)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error getting iscsi global: %s" % resp.text)
            resp.raise_for_status()

        output_data = resp.json()

        return ISCSIGlobal(
            id=output_data['id'],
            basename=output_data['basename']
        )

    def get_iscsi_portal(self, portal_id: str) -> Optional[ISCSIPortal]:
        url = urljoin(self.__url, "iscsi/portal/id/%s" % quote_plus(portal_id))
        resp = self.__client_session.get(url)
        if resp.status_code != 200:
            if resp.status_code == 404:
                return None
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error getting iscsi portal %s: %s" % (portal_id, resp.text))
            resp.raise_for_status()

        output_data = resp.json()

        portal = ISCSIPortal(id=output_data['id'], listen=[])

        for portal_listen in output_data['listen']:
            portal.listen.append(ISCSIPortalListen(
                ip=ip_address(portal_listen['ip']),
                port=portal_listen['port']
            ))

        return portal

    def create_iscsi_target(self, name: str, portal_id: int) -> ISCSITarget:
        url = urljoin(self.__url, "iscsi/target")
        iscsi_target_props = {
            "name": name,
            "alias": None,
            "mode": "ISCSI",
            "groups": [
                {
                    "portal": portal_id,
                    "initiator": None,
                    "auth": None,
                    "authmethod": "NONE"

                }
            ]
        }
        resp = self.__client_session.post(url, json=iscsi_target_props)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error create iscsi target %s: %s" % (iscsi_target_props, resp.text))
            resp.raise_for_status()

        output_data = resp.json()

        return ISCSITarget(id=output_data['id'])

    def delete_iscsi_target(self, target_id: str):
        url = urljoin(self.__url, "iscsi/target/id/%s" % quote_plus(target_id))
        resp = self.__client_session.delete(url)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error deleting iscsi target %s: %s" % (target_id, resp.text))
            resp.raise_for_status()

    def create_iscsi_disk_extent(self, name: str, block_size: int, disk_path: str) -> ISCSIExtent:
        url = urljoin(self.__url, "iscsi/extent")
        iscsi_disk_extent_props = {
            "name": name,
            "type": "DISK",
            "blocksize": block_size,
            "enabled": True,
            "disk": f"zvol/{disk_path}"

        }

        resp = self.__client_session.post(url, json=iscsi_disk_extent_props)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error create iscsi disk extent %s: %s" % (iscsi_disk_extent_props, resp.text))
            resp.raise_for_status()

        output_data = resp.json()

        return ISCSIExtent(id=output_data['id'])

    def delete_iscsi_extent(self, extent_id: str):
        url = urljoin(self.__url, "iscsi/extent/id/%s" % quote_plus(extent_id))
        resp = self.__client_session.delete(url)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error deleting iscsi extent %s: %s" % (extent_id, resp.text))
            resp.raise_for_status()

    def create_iscsi_targetextent(self, target_id: int, extent_id: int):
        url = urljoin(self.__url, "iscsi/targetextent")
        iscsi_target_extent_props = {
            "target": target_id,
            "extent": extent_id,
        }

        resp = self.__client_session.post(url, json=iscsi_target_extent_props)
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error create iscsi targetextent %s: %s" % (iscsi_target_extent_props, resp.text))
            resp.raise_for_status()
