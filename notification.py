from datetime import datetime
from typing import Dict, List, Optional, Union

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.db.mixin import EPOCH
from app.models.notification import Notification
from app.schemas.notification import (
    CommentOnCommentMeta,
    CommentOnContentMeta,
    ConnectionReqAcceptMeta,
    LikeOnCommentMeta,
    LikeOnContentMeta,
    NewConnectionReqMeta,
    NotificationCountOut,
    NotificationCreate,
    NotificationMeta,
    NotificationTypeEnum,
    NotificationUpdate,
)

MAX_COUNT = 30
MAX_GROUP_USERS = 3


class CRUDNotification(CRUDBase[Notification, NotificationCreate, NotificationUpdate]):
    """
    CRUD for :any:`Notification model<models.notification.Notification>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def get_list(
        self,
        db: AsyncSession,
        *,
        user_id: str,
        read: Optional[bool] = None,
        since_id: Optional[int] = None,
        last_id: Optional[int] = None,
        count: Optional[int] = 10,
    ) -> List[Notification]:
        count = min(MAX_COUNT, max(1, count))

        query = (
            self.query()
            .where(Notification.user_id == user_id)
            .order_by(Notification.notified_at.desc(), Notification.id.desc())
        )
        if read is not None:
            if read is True:
                query = query.where(Notification.read_at != None)  # noqa
            else:
                query = query.where(Notification.read_at == None)  # noqa

        if since_id:
            query = query.where(Notification.id > since_id)
        if last_id:
            query = query.where(Notification.id < last_id)
        if count:
            query = query.limit(count)
        results = await self.get_q(db, query=query)
        return results

    async def get_count(
        self, db: AsyncSession, *, user_id: str
    ) -> NotificationCountOut:
        query = self.query().where(
            Notification.user_id == user_id, Notification.read_at == None  # noqa: E711
        )
        count = await self.count_q(db, query=query)
        return NotificationCountOut(count=count)

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: Union[NotificationCreate, NotificationUpdate],
        db_obj: Optional[Notification] = None,
    ) -> Notification:
        """Create or Update a row"""
        obj_data = obj_in.dict()

        if db_obj is not None:
            db_data = db_obj.__dict__
            for field in db_data:
                if field in obj_data:
                    setattr(db_obj, field, obj_data[field])
        else:
            db_obj = Notification(**obj_data)

        await self.commit_refresh(db, db_obj)
        return db_obj

    async def multi_update(
        self,
        db: AsyncSession,
        *,
        user_id: str,
        read_at: Optional[datetime] = None,
        ids: Optional[List[int]] = None,
    ) -> int:
        """Helper method to update the read_at status of multiple notifications

        returns the number of rows affected.
        """
        update_stmt_q = update(Notification).where(
            Notification.user_id == user_id, Notification.deleted_at == EPOCH
        )
        if ids:
            # mark only these notifications as read
            update_stmt_q = update_stmt_q.where(Notification.id.in_(ids))

        if read_at is None and not ids:
            # If we try to mark as unread and not pass IDs, we don't update it at all.
            return 0

        update_stmt = update_stmt_q.values(read_at=read_at)

        result = await db.execute(update_stmt)
        await db.commit()
        return result.rowcount

    async def multi_delete(
        self, db: AsyncSession, *, user_id: str, ids: List[int], deleted_at: datetime
    ) -> int:
        """Helper method to delete multiple notifications.

        returns the number of rows affected.
        """
        delete_stmt = (
            update(Notification)
            .where(
                Notification.id.in_(ids),
                Notification.user_id == user_id,
                Notification.deleted_at == EPOCH,
            )
            .values(deleted_at=deleted_at)
        )
        result = await db.execute(delete_stmt)
        await db.commit()
        return result.rowcount

    async def create_new(
        self,
        *,
        db: AsyncSession,
        uid: str,
        notification_type: NotificationTypeEnum,
        meta_in: Union[
            CommentOnCommentMeta,
            CommentOnContentMeta,
            LikeOnCommentMeta,
            LikeOnContentMeta,
            NewConnectionReqMeta,
            ConnectionReqAcceptMeta,
        ],
        # Update the item even if marked as read
        force_update: Optional[bool] = False,
        # Delete the matching item without updating
        delete_only: Optional[bool] = False,
    ) -> Notification:
        meta_obj = self.format_notification_meta(
            meta_in=meta_in.__dict__, notification_type=notification_type
        )

        data_obj = {
            "user_id": uid,
            "type": notification_type,
            "entity_id": None,
            "notified_at": datetime.utcnow(),
        }

        if notification_type in [
            NotificationTypeEnum.COMMENT_ON_CONTENT,
            NotificationTypeEnum.LIKE_ON_CONTENT,
        ]:
            data_obj["entity_id"] = meta_obj.content.id
        elif notification_type in [
            NotificationTypeEnum.COMMENT_ON_COMMENT,
            NotificationTypeEnum.LIKE_ON_COMMENT,
        ]:
            data_obj["entity_id"] = meta_obj.comment.id
        elif notification_type in [
            NotificationTypeEnum.NEW_CONNECTION_REQ,
            NotificationTypeEnum.CONNECTION_REQ_ACCEPT,
        ]:
            data_obj["entity_id"] = meta_obj.connection.id

        if (
            force_update
            and notification_type == NotificationTypeEnum.NEW_CONNECTION_REQ
        ):
            # Do not update notified at when connection request is updated
            data_obj.pop("notified_at")

        query = self.query().filter(
            Notification.user_id == uid,
            Notification.type == notification_type,
            Notification.entity_id == str(data_obj["entity_id"]),
        )
        if not force_update:
            query.filter(Notification.read_at.is_(None))

        query.order_by(Notification.notified_at.desc(), Notification.id.desc())

        db_obj = await self.get_q_one(db, query)

        full_meta_obj = NotificationMeta(**meta_obj.__dict__)

        if not delete_only and db_obj is not None:
            meta_user_exist = None
            if full_meta_obj.users:
                for i, user in enumerate(db_obj.meta["users"]):
                    if user["uid"] == full_meta_obj.users[0].uid:
                        meta_user_exist = (i, user)

                delattr(full_meta_obj, "user")

            if meta_user_exist and full_meta_obj.users:
                # Since meta will be overwritten, this mutation has no side affect.
                db_obj.meta["users"].pop(meta_user_exist[0])
                full_meta_obj.users = (
                    full_meta_obj.users + db_obj.meta["users"][: MAX_GROUP_USERS - 1]
                )
                full_meta_obj.users_count = db_obj.meta["users_count"]
            elif not meta_user_exist and full_meta_obj.users:
                full_meta_obj.users = (
                    full_meta_obj.users + db_obj.meta["users"][: MAX_GROUP_USERS - 1]
                )
                full_meta_obj.users_count = db_obj.meta["users_count"] + 1

            data_obj["meta"] = full_meta_obj

            # Copy db data & update it, to avoid resetting of non-updated fields
            new_data_obj = db_obj.__dict__.copy()
            new_data_obj.update(data_obj)
            notif_obj = NotificationUpdate(**new_data_obj)
        elif not delete_only:
            if full_meta_obj.users:
                full_meta_obj.users_count = 1
                delattr(full_meta_obj, "user")

            data_obj["meta"] = full_meta_obj
            notif_obj = NotificationCreate(**data_obj)

        if delete_only and db_obj is not None:
            return await self.delete(db, id=db_obj.id)
        elif delete_only:
            return None

        return await self.create_or_update(db, obj_in=notif_obj, db_obj=db_obj)

    @staticmethod
    def format_notification_meta(
        *,
        meta_in: Dict,
        notification_type: NotificationTypeEnum,
    ):
        meta_obj = None

        if notification_type == NotificationTypeEnum.COMMENT_ON_CONTENT:
            meta_in["users"] = [meta_in["user"]]
            meta_obj = CommentOnContentMeta(**meta_in)

        if notification_type == NotificationTypeEnum.LIKE_ON_CONTENT:
            meta_in["users"] = [meta_in["user"]]
            meta_obj = LikeOnContentMeta(**meta_in)

        if notification_type == NotificationTypeEnum.COMMENT_ON_COMMENT:
            meta_in["users"] = [meta_in["user"]]
            meta_obj = CommentOnCommentMeta(**meta_in)

        if notification_type == NotificationTypeEnum.LIKE_ON_COMMENT:
            meta_in["users"] = [meta_in["user"]]
            meta_obj = LikeOnCommentMeta(**meta_in)

        if notification_type == NotificationTypeEnum.NEW_CONNECTION_REQ:
            meta_obj = NewConnectionReqMeta(**meta_in)

        if notification_type == NotificationTypeEnum.CONNECTION_REQ_ACCEPT:
            meta_obj = ConnectionReqAcceptMeta(**meta_in)

        return meta_obj


notification = CRUDNotification(Notification)
