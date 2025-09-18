from dataclasses import dataclass

from data_loader_core.credentials import CredentialProvider
from data_loader_core.kv_store import KeyValueStore
from data_loader_core.storage import Storage


@dataclass
class loaderConfig:
    """loader configuration container."""

    credential_provider: CredentialProvider
    kv_store: KeyValueStore
    storage: Storage
