import datetime


class PeerDBMixin:
    @classmethod
    def peerdb_version(cls) -> int:
        return 0

    @classmethod
    def peerdb_is_deleted(cls) -> int:
        return cls.__random__.randint(0, 1)

    @classmethod
    def peerdb_synced_at(cls) -> datetime.datetime:
        return datetime.datetime.now()
