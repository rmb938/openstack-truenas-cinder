# OpenStack TrueNAS Cinder Driver

TrueNAS Driver for OpenStack Cinder.

## Requirements

* This driver has been developed and tested on [OpenStack Zed](https://docs.openstack.org/zed/), other versions may not work as expected.

## Features

- [ ] All [Core Functionality](https://docs.openstack.org/cinder/latest/contributor/drivers.html#minimum-features) features.
- [ ] Thin & Thick Volumes
- [ ] Clone Image - efficiently create volumes based on images backed by volumes using ZFS Snapshots
  * Only Raw & Bare images are supported
- [ ] Manage/Unmanage Support - Will be a nice to have, most likely possible to implement
- [ ] Ability to do "full" snapshots when cloning volumes - using ZFS Send/Receive to prevent snapshot dependencies

## Installation

This assumes the [OpenStack Installation Guide](https://docs.openstack.org/install-guide/) was followed.

## Referrences

This driver was inspired by https://github.com/iXsystems/cinder
