import logging
from typing import Optional

import requests
from cinder.volume.drivers.rmb938.truenas.api.objects.dataset import Dataset, DatasetType
from requests.compat import urljoin, quote_plus

LOG = logging.getLogger(__name__)


class TrueNASAPIClient(object):

    def __init__(self, url: str, api_key: str):
        self.__url = urljoin(url, "/api/v2.0")

        self.__client_session = requests.Session()
        self.__client_session.headers.update({'Authorization': 'Bearer %s' % api_key})
        # TODO: ssl configuration if it's provided

    def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        url = urljoin(self.__url, "/pool/dataset/id/%s" % quote_plus(dataset_id))
        resp = self.__client_session.get(url)
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            # TODO: we probably want to wrap this with a custom exception
            LOG.error("error getting dataset %s: %s" % (dataset_id, resp.text))
            resp.raise_for_status()

        LOG.info("get dataset response: %s: %s" % (dataset_id, resp.text))
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

    def create_zvol(self, name: str, size: int, block_size: int, sparse: bool):
        url = urljoin(self.__url, "/pool/dataset")
        resp = self.__client_session.post(url, json={
            "name": name,
            "type": 'VOLUME',
            "volsize": size,
            "volblocksize": block_size,
            "sparse": sparse
        })
        # TODO: we probably want to wrap this with a custom exception
        resp.raise_for_status()
