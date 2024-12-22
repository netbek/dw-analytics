from pydantic.main import IncEx
from typing import Any, Optional

import datetime
import pydash


class BaseMixin:
    def model_dump_copy(
        self,
        include: Optional[IncEx] = None,
        exclude: Optional[IncEx] = None,
        by_alias: Optional[bool] = False,
        update: Optional[dict[str, Any]] = None,
    ):
        data = self.model_dump(by_alias=by_alias, include=include, exclude=exclude)

        if update:
            data.update(update)

        return data


class PeerDBFactoryMixin:
    @classmethod
    def peerdb_version(cls) -> int:
        return int(pydash.unique_id())

    @classmethod
    def peerdb_is_deleted(cls) -> int:
        return 0

    @classmethod
    def peerdb_synced_at(cls) -> datetime.datetime:
        return datetime.datetime.now()
