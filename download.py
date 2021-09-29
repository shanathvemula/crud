from datetime import datetime
from typing import Any, Dict, Union

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.models.download import DownloadFile, DownloadRequest
from app.models.user import User
from app.schemas.download import (
    DownloadFileCreate,
    DownloadFileIn,
    DownloadRequestCreate,
    DownloadRequestUpdate,
)


class CRUDDownloadRequest(
    CRUDBase[DownloadRequest, DownloadRequestCreate, DownloadRequestUpdate]
):
    """
    CRUD for :any:`DownloadRequest model<models.download.DownloadRequest>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create(
        self, db: AsyncSession, *, obj_in: DownloadRequestCreate, user: User
    ) -> DownloadRequest:
        """Create a row"""
        db_obj = DownloadRequest(created_by=user, data_types=obj_in.data_types.dict())
        await self.commit_refresh(db, db_obj)
        return db_obj

    async def update(
        self,
        db: AsyncSession,
        *,
        db_obj: DownloadRequest,
        obj_in: Union[DownloadRequestUpdate, Dict[str, Any]],
    ):
        d = db_obj.data_types_completed.copy()
        d.update(obj_in.data_types_completed)
        db_obj.data_types_completed = d
        completed = all(
            [
                db_obj.data_types_completed.get(req_k, False)
                for req_k in [k for k, v in db_obj.data_types.items() if v is True]
            ]
        )
        db_obj.completed = completed

        file_objs = db_obj.files
        for f in obj_in.files:
            file_objs.append(await crud.download_file.create(db, obj_in=f))
        db_obj.files = file_objs

        await self.commit_refresh(db, db_obj)
        return db_obj

    async def get_pending_req(self, db: AsyncSession, *, user: User) -> DownloadRequest:
        q = (
            self.query()
            .filter_by(created_by=user, completed=False)
            .filter(DownloadRequest.expiry_at > datetime.utcnow())
        )
        db_obj = await self.get_q_one(db=db, query=q)
        return db_obj


class CRUDDownloadFile(CRUDBase[DownloadFile, DownloadFileCreate, DownloadFileIn]):
    """
    CRUD for :any:`DownloadFile model<models.download.DownloadFile>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


download_request = CRUDDownloadRequest(DownloadRequest)
download_file = CRUDDownloadFile(DownloadFile)
