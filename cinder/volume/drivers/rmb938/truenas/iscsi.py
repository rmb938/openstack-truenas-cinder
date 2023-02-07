import json
import logging
from typing import Tuple, Any, Optional

import requests.exceptions
from cinder import interface
from cinder.common.constants import ISCSI
from cinder.context import RequestContext
from cinder.exception import VolumeBackendAPIException, VolumeDriverException, SnapshotIsBusy
from cinder.image.glance import GlanceImageService
from cinder.objects.snapshot import Snapshot
from cinder.objects.volume import Volume
from cinder.volume import driver, configuration
from cinder.volume import volume_types
from cinder.volume.drivers.rmb938.truenas.api.client import TrueNASAPIClient
from cinder.volume.drivers.rmb938.truenas.api.objects.dataset import DatasetType
from cinder.volume.drivers.rmb938.truenas.options import truenas_connection_opts
from oslo_config import cfg

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.register_opts(truenas_connection_opts, group=configuration.SHARED_CONF_GROUP)


@interface.volumedriver
class TrueNASISCSIDriver(driver.ISCSIDriver):
    VERSION = "1.0.0"

    def __init__(self, *args, **kwargs):
        """Initialize TrueNASISCSIDriver Class."""

        LOG.info('truenas: Init Cinder Driver')
        super(TrueNASISCSIDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(truenas_connection_opts)
        self.truenas_client: Optional[TrueNASAPIClient] = None

    @classmethod
    def get_driver_options(cls):
        additional_opts = cls._get_oslo_driver_opts(
            'reserved_percentage', 'volume_backend_name')
        return truenas_connection_opts + additional_opts

    def check_for_setup_error(self):
        truenas_dataset_path = self.configuration.truenas_dataset_path
        dataset = self.truenas_client.get_dataset(truenas_dataset_path)
        if dataset is None:
            raise VolumeBackendAPIException('Could not find TrueNAS dataset at path %s' % truenas_dataset_path)

        if dataset.type != DatasetType.FILESYSTEM:
            raise VolumeBackendAPIException('truenas_dataset_path %s is not a filesystem' % truenas_dataset_path)

    def do_setup(self, context: RequestContext):
        truenas_url = self.configuration.truenas_url
        truenas_api_key = self.configuration.truenas_apikey
        self.truenas_client = TrueNASAPIClient(truenas_url, truenas_api_key)

    def _update_volume_stats(self):
        reserved_percentage = self.configuration.reserved_percentage
        backend_name = self.configuration.volume_backend_name
        truenas_dataset_path = self.configuration.truenas_dataset_path

        dataset = self.truenas_client.get_dataset(truenas_dataset_path)
        total_capacity_gb = dataset.size / 1024 / 1024 / 1024
        free_capacity_gb = (dataset.size - dataset.used) / 1024 / 1024 / 1024

        self._stats = {
            'volume_backend_name': backend_name or 'Generic_TrueNAS',
            'vendor_name': 'TrueNAS',
            'driver_version': self.VERSION,
            'storage_protocol': ISCSI,
            'total_capacity_gb': total_capacity_gb,
            'free_capacity_gb': free_capacity_gb,
            'reserved_percentage': reserved_percentage or 0,
            'location_info': '',
            'QoS_support': False,
            'max_over_subscription_ratio': 0.0,  # TODO: calculate this
            'thin_provisioning_support': True,
            'thick_provisioning_support': True,
            # TODO: provisioned_capacity_gb (all volumes provisioned, including non-openstack things)
            'multiattach': False,
            'online_extend_support': True,
        }

    def initialize_connection(self, volume: Volume, connector: dict):
        if volume.provider_id is None:
            # volume has no provider id, so we didn't actually create it
            raise VolumeDriverException(
                "Volume %s does not have provider_id set so we cannot make a connection" % volume.id)

        # TODO: support multi-path somehow, would be multiple ips on the portal, but the iqn is the same?

        iscsi_global = self.truenas_client.get_iscsi_global()

        iscsi_portal = self.truenas_client.get_iscsi_portal(portal_id="1")  # TODO: make this configurable
        if iscsi_portal is None:
            raise VolumeDriverException("Could not find iscsi portal")
        # TODO: make sure the IP returned is not 0.0.0.0

        return {
            'driver_volume_type': 'iscsi',
            'data': {
                'target_discovered': False,
                'target_iqn': f'{iscsi_global.basename}:{volume.name_id}',
                'target_portal': f'{iscsi_portal.listen[0].ip}:{iscsi_portal.listen[0].port}',
                'volume_id': volume.id,
                'discard': False,
            }
        }

    def terminate_connection(self, volume: Volume, connector: dict, **kwargs):
        pass

    def clone_image(self, context: RequestContext, volume: Volume, image_location: Tuple[str, dict[str, str]],
                    image_meta: dict[str, Any], image_service: GlanceImageService):

        # TODO: do a replication task since we don't want a hard dependency
        #  between the volume and the source image

        return {}, False

    def create_snapshot(self, snapshot: Snapshot) -> dict:
        if snapshot.volume.provider_id is None:
            # volume has no provider id, so we didn't actually create it
            raise VolumeDriverException(
                "Volume %s does not exist in the backend so we can't create a snapshot" % snapshot.volume.id)

        truenas_snapshot_id = "%s@%s" % (snapshot.volume.provider_id, snapshot.id)

        self.truenas_client.create_snapshot(name=snapshot.id, dataset=snapshot.volume.provider_id)

        model_update = {
            'provider_id': truenas_snapshot_id
        }

        if not snapshot.admin_metadata:
            model_update['admin_metadata'] = {
                'truenas_snapshot_id': truenas_snapshot_id,
            }
        else:
            model_update['admin_metadata'] = {
                **snapshot.admin_metadata,
                'truenas_snapshot_id': truenas_snapshot_id,
            }

        return model_update

    def create_volume(self, volume: Volume) -> dict:
        truenas_dataset_path = self.configuration.truenas_dataset_path
        truenas_volume_id = "%s/%s" % (truenas_dataset_path, volume.name_id)

        sparse = False  # TODO: make this default a config option
        if volume.volume_type is not None:
            provisioning_type = volume_types.get_volume_type_extra_specs(volume.volume_type.id, 'provisioning:type')
            if provisioning_type:
                if provisioning_type == 'thin':
                    sparse = True
                elif provisioning_type == 'thick':
                    sparse = False
                else:
                    raise VolumeDriverException(
                        "Unknown provisioning type %s for volume %s" % (provisioning_type, volume.id))

        # TODO: the size given here is the minimum
        # when the zvol is created it will round up to the nearest block size, i.e 1 GiB will round up to 1.03 GiB
        # unsure how to report this back to openstack as openstack only uses round number sizing
        self.truenas_client.create_zvol(
            name=truenas_volume_id,
            size=volume.size * 1024 * 1024 * 1024,  # Cinder creates volumes in GiB (1024) to convert to bytes
            sparse=sparse
        )

        model_update = {
            'provider_id': truenas_volume_id,
        }

        if not volume.admin_metadata:
            model_update['admin_metadata'] = {
                'truenas_volume_id': truenas_volume_id
            }
        else:
            model_update['admin_metadata'] = {
                **volume.admin_metadata,
                'truenas_volume_id': truenas_volume_id
            }

        return model_update

    def create_cloned_volume(self, volume: Volume, src_vref: Volume):
        raise NotImplementedError()
        truenas_dataset_path = self.configuration.truenas_dataset_path
        truenas_volume_id = "%s/%s" % (truenas_dataset_path, volume.name_id)

        # TODO: do a replication task since we are fully cloning the volume
        #  not making a snapshot from it
        #  replication tasks can take a long time, so unsure how to do this nicely

        model_update = {
            'provider_id': truenas_volume_id,
        }

        if not volume.admin_metadata:
            model_update['admin_metadata'] = {
                'truenas_volume_id': truenas_volume_id
            }
        else:
            model_update['admin_metadata'] = {
                **volume.admin_metadata,
                'truenas_volume_id': truenas_volume_id
            }

        return model_update

    def create_volume_from_snapshot(self, volume: Volume, snapshot: Snapshot) -> dict:
        truenas_dataset_path = self.configuration.truenas_dataset_path
        truenas_volume_id = "%s/%s" % (truenas_dataset_path, volume.name_id)

        self.truenas_client.clone_snapshot(snapshot.provider_id, truenas_volume_id)

        # set provider id so downstream functions can use it
        volume.provider_id = truenas_volume_id
        model_update = {
            'provider_id': truenas_volume_id,
        }

        if not volume.admin_metadata:
            model_update['admin_metadata'] = {
                'truenas_volume_id': truenas_volume_id,
                'truenas_volume_from_snapshot_id': snapshot.provider_id
            }
        else:
            model_update['admin_metadata'] = {
                **volume.admin_metadata,
                'truenas_volume_id': truenas_volume_id,
                'truenas_volume_from_snapshot_id': snapshot.provider_id
            }

        # check if snapshot is different size
        if snapshot.volume_size != volume.size:
            # it takes a bit for the dataset to show in the api
            # so just loop for a bit until it's not none
            dataset = None
            while dataset is None:
                dataset = self.truenas_client.get_dataset(truenas_volume_id)

            # Extend the volume if it's a different size
            self.extend_volume(volume, volume.size)

        return model_update

    def delete_snapshot(self, snapshot: Snapshot):
        if snapshot.provider_id is None:
            # snapshot has no provider id, so we didn't actually create it
            LOG.info("Snapshot %s has no provider_id during a delete so ignore it" % snapshot.id)
            return

        try:
            self.truenas_client.delete_snapshot(snapshot.provider_id)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                try:
                    # snapshot not found so return safely
                    if 'not found' in e.response.json()['message']:
                        return

                    # snapshot is in use so raise is busy
                    if 'snapshot has dependent clones' in e.response.json()['message']:
                        raise SnapshotIsBusy(snapshot_name=snapshot.name)
                except json.JSONDecodeError:
                    raise e

            raise e

    def delete_volume(self, volume: Volume):
        if volume.provider_id is None:
            # volume has no provider id, so we didn't actually create it
            LOG.info("Volume %s has no provider_id during a delete so ignore it" % volume.id)
            return

        try:
            self.truenas_client.delete_dataset(volume.provider_id)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                try:
                    response_data = e.response.json()
                    # volume not found so return safely
                    if 'null' in response_data:
                        if 'does not exist' in response_data['null'][0]['message']:
                            return
                except json.JSONDecodeError:
                    raise e

            raise e

    def extend_volume(self, volume: Volume, new_size: int):
        if volume.provider_id is None:
            # volume has no provider id, so we didn't actually create it
            raise VolumeDriverException("volume %s does not exist in TrueNAS so we cant expand it" % volume.id)

        # TODO: the size given here is the minimum
        # when the zvol is created it will round up to the nearest block size, i.e 1 GiB will round up to 1.03 GiB
        # unsure how to report this back to openstack as openstack only uses round number sizing
        self.truenas_client.expand_zvol(volume.provider_id, new_size * 1024 * 1024 * 1024)

    def create_export(self, context: RequestContext, volume: Volume, connector: dict) -> dict:
        if volume.provider_id is None:
            # volume has no provider id, so we didn't actually create it
            raise VolumeDriverException(
                "Volume %s does not have provider_id set so we cannot create export" % volume.id)

        iscsi_target = self.truenas_client.create_iscsi_target(
            name=volume.name_id,
            portal_id=1,  # TODO: make this configurable
        )

        iscsi_disk_extent = self.truenas_client.create_iscsi_disk_extent(
            name=volume.name_id,
            block_size=512,  # TODO: make this configurable
            disk_path=volume.provider_id,
        )

        self.truenas_client.create_iscsi_targetextent(
            target_id=iscsi_target.id,
            extent_id=iscsi_disk_extent.id
        )

        model_update = {
            'admin_metadata': {
                **volume.admin_metadata,
                'truenas_iscsi_target_id': f"{iscsi_target.id}",
                'truenas_iscsi_extent_id': f"{iscsi_disk_extent.id}",
            }
        }

        return model_update

    def remove_export(self, context: RequestContext, volume: Volume):
        if volume.provider_id is None:
            # volume has no provider id, so we didn't actually create it
            raise VolumeDriverException("Volume %s does not have provider_id set so we "
                                        "cannot remove export" % volume.id)

        if not volume.admin_metadata:
            raise VolumeDriverException("Volume %s is missing admin_metadata so we don't know "
                                        "how to remove exports" % volume.id)

        if 'truenas_iscsi_target_id' not in volume.admin_metadata:
            raise VolumeDriverException("Volume %s is missing 'truenas_iscsi_target_id' in "
                                        "admin_metadata so we don't know how to remove exports" % volume.id)

        if 'truenas_iscsi_extent_id' not in volume.admin_metadata:
            raise VolumeDriverException("Volume %s is missing 'truenas_iscsi_extent_id' in "
                                        "admin_metadata so we don't know how to remove exports" % volume.id)

        truenas_iscsi_target_id = volume.admin_metadata['truenas_iscsi_target_id']
        truenas_iscsi_extent_id = volume.admin_metadata['truenas_iscsi_extent_id']

        self.__remove_iscsi_target(target_id=truenas_iscsi_target_id)
        self.__remove_iscsi_extent(extent_id=truenas_iscsi_extent_id)

    def __remove_iscsi_target(self, target_id):
        try:
            self.truenas_client.delete_iscsi_target(target_id=target_id)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                try:
                    response_data = e.response.json()
                    # volume not found so return safely
                    if 'null' in response_data:
                        if 'does not exist' in response_data['null'][0]['message']:
                            return
                except json.JSONDecodeError:
                    raise e

            raise e

    def __remove_iscsi_extent(self, extent_id):
        try:
            self.truenas_client.delete_iscsi_extent(extent_id=extent_id)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                try:
                    response_data = e.response.json()
                    # volume not found so return safely
                    if 'null' in response_data:
                        if 'does not exist' in response_data['null'][0]['message']:
                            return
                except json.JSONDecodeError:
                    raise e

            raise e
