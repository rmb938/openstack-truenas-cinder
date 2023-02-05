from dataclasses import dataclass
from ipaddress import ip_address


@dataclass
class ISCSIGlobal:
    id: int
    basename: str


@dataclass
class ISCSIPortalListen:
    ip: ip_address
    port: int


@dataclass
class ISCSIPortal:
    id: int
    listen: list[ISCSIPortalListen]


@dataclass
class ISCSITarget:
    id: int


@dataclass
class ISCSIExtent:
    id: int
