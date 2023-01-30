from oslo_config import cfg

truenas_connection_opts = [
    cfg.StrOpt('truenas_url',
               required=True,
               help='URL of TrueNAS system.'),
    cfg.StrOpt('truenas_apikey',
               required=True,
               help='API Key of TrueNAS system.'),
    # cfg.StrOpt('truenas_datastore_pool',
    #            required=True,
    #            help='Pool name on the TrueNAS system.'),
    cfg.StrOpt('truenas_dataset_path',
               required=True,
               help='Full path including pool to the dataset on the TrueNAS system.'),
]
