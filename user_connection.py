from typing import Any, Optional

import aioredis
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.core.config import settings
from app.crud.base import CRUDBase
from app.db import redis
from app.db.mixin import EPOCH
from app.models.user import User
from app.models.user_connection import UserConnection
from app.schemas.user_connection import (
    ConnectionStatusEnum,
    UserConnectionCount,
    UserConnectionCreate,
    UserConnectionQuery,
    UserConnectionUpdate,
)

MAX_CONN_COUNT = 50


class CRUDUserConnection(
    CRUDBase[UserConnection, UserConnectionCreate, UserConnectionUpdate]
):
    """
    CRUD for :any:`Connections model<models.user_connection.UserConnection>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def get_all(
        self,
        db: AsyncSession,
        request_query: UserConnectionQuery,
        user: User,
        limit: Optional[bool] = True,
    ) -> Any:
        """
        Get all connections for a given user.

        Args:
            request_query: consist of all filtering & pagination params
            limit: setting to False, will return all results without limiting

        Returns:
            Returns a dict consisting list of connection objects, total & pagination
        """

        start = max(0, request_query.start)
        count = min(MAX_CONN_COUNT, max(1, request_query.count))

        # TODO Remove page & size, once frontend is changed to start & count
        if request_query.size is not None:
            count = min(MAX_CONN_COUNT, max(1, request_query.size))

        if request_query.page is not None:
            start = (max(1, request_query.page) - 1) * count

        status = request_query.status

        status_query = [
            or_(
                UserConnection.created_by_id == user.id,
                UserConnection.receiver_id == user.id,
            )
        ]

        if status == ConnectionStatusEnum.SENT:
            status_query = [
                UserConnection.created_by_id == user.id,
                UserConnection.connected.is_(False),
            ]
        elif status == ConnectionStatusEnum.RECEIVED:
            status_query = [
                UserConnection.receiver_id == user.id,
                UserConnection.connected.is_(False),
            ]
        elif status == ConnectionStatusEnum.ACTIVE:
            status_query.append(UserConnection.connected.is_(True))

        query = (
            select(UserConnection)
            .join(
                User,
                or_(
                    User.id == UserConnection.created_by_id,
                    User.id == UserConnection.receiver_id,
                ),
            )
            .where(
                *status_query,
                UserConnection.deleted_at == EPOCH,
                User.id != user.id,
                User.deactivated_at.is_(None),
                User.deleted_at == EPOCH,
            )
        )
        total = await self.count_q(db, query=query)

        query = query.order_by(UserConnection.created_at.desc())
        if limit:
            query = query.offset(start).limit(count)

        db_obj = await self.get_q(db, query)
        conn_res = {"conns": db_obj, "start": start, "count": count, "total": total}
        return conn_res

    async def get(self, db: AsyncSession, id, user: User) -> UserConnection:
        """
        Get a connection object for a given id.

        Args:
            id: id of :any:`Connection<models.user_connection.UserConnection>`

        Returns:
            Returns a connection object
        """

        query = select(UserConnection)
        query = query.filter(
            UserConnection.id == id,
            or_(
                UserConnection.created_by_id == user.id,
                UserConnection.receiver_id == user.id,
            ),
            UserConnection.deleted_at == EPOCH,
        )
        db_obj = await self.get_q(db, query)

        return db_obj.pop() if db_obj else None

    async def get_conn_from_user_ids(
        self, db, *, auth_user_id: int, conn_user_id: int
    ) -> UserConnection:
        """
        Get a connection object given 2 user ids (creator/receiver)

        Args:
            auth_user_id: authenticated(logged) in :any:`user<models.user.User>` id
            conn_user_id: other :any:`user's<models.user.User>` id to whom connected

        Returns:
            Returns a connection object
        """

        query = select(UserConnection)
        query = query.filter(
            or_(
                and_(
                    UserConnection.created_by_id == auth_user_id,
                    UserConnection.receiver_id == conn_user_id,
                ),
                and_(
                    UserConnection.created_by_id == conn_user_id,
                    UserConnection.receiver_id == auth_user_id,
                ),
            ),
            UserConnection.deleted_at == EPOCH,
        )

        db_obj = await self.get_q(db, query)

        return db_obj.pop() if db_obj else None

    async def create(
        self, db: AsyncSession, *, obj_in: UserConnectionCreate, user: User
    ) -> UserConnection:
        receiver = await crud.user.get_by_uid(db, uid=obj_in.uid)
        db_obj = UserConnection(
            receiver=receiver,
            connected=obj_in.connected,
            created_by=user,
        )
        await self.commit_refresh(db, db_obj)
        return db_obj

    async def delete(self, db: AsyncSession, *, id: int, user: User) -> UserConnection:
        db_obj = await self.get(db, id, user)
        db_obj.updated_by = user
        db_obj.delete()
        await self.commit_refresh(db, db_obj)
        return db_obj

    async def get_conn_count(
        self,
        db: AsyncSession,
        r: aioredis.Redis,
        *,
        user: User,
        status: Optional[ConnectionStatusEnum] = None,
    ) -> UserConnectionCount:
        """
        Get connection counts for a given user, from redis.
        If count is not available in redis, call the updater.

        Args:
            status: filter by connection status

        Returns:
            Returns a dict of all connection status & its counts
        """

        conn_status_key = redis.get_user_conn_key(user.id, "count")
        conn_status_count = await r.hgetall(conn_status_key, encoding="utf-8")

        if not conn_status_count:
            conn_status_count = await crud.userconnection.update_conn_count(
                db, r, user=user
            )

        conn_count_obj = dict()
        for conn_status in ConnectionStatusEnum:
            if not status or conn_status == status:
                # update_conn_count method (above) returns Enum as key
                # while Redis returns String as key
                # Hence, check Enum as fallback to String.
                conn_count_obj[conn_status] = int(
                    conn_status_count.get(
                        conn_status.name, conn_status_count.get(conn_status, 0)
                    )
                )

        return conn_count_obj

    async def update_conn_count(
        self,
        db: AsyncSession,
        r: aioredis.Redis,
        *,
        user: User,
    ):
        """
        Fetches user connection counts from db & updates redis.

        Returns:
            Returns a dict of all connection status & its counts
        """

        cache_ttl = settings.CONN_COUNT_CACHE_TTL  # TTL in seconds

        conn_status_q = select(
            case(
                (
                    UserConnection.created_by_id == user.id,
                    UserConnection.receiver_id,
                ),
                (
                    UserConnection.receiver_id == user.id,
                    UserConnection.created_by_id,
                ),
            ).label("conn_user"),
            case(
                (
                    UserConnection.connected.is_(True),
                    ConnectionStatusEnum.ACTIVE,
                ),
                (
                    UserConnection.created_by_id == user.id,
                    ConnectionStatusEnum.SENT,
                ),
                (
                    UserConnection.receiver_id == user.id,
                    ConnectionStatusEnum.RECEIVED,
                ),
            ).label("status"),
        ).where(
            or_(
                UserConnection.created_by_id == user.id,
                UserConnection.receiver_id == user.id,
            ),
            UserConnection.deleted_at == EPOCH,
        )

        conn_count_q = (
            select(
                conn_status_q.c.status.label("status"),
                func.count(conn_status_q.c.status).label("total"),
            )
            .join(User, User.id == conn_status_q.c.conn_user)
            .where(User.deactivated_at.is_(None), User.deleted_at == EPOCH)
            .group_by(conn_status_q.c.status)
        )

        results = await db.execute(conn_count_q)
        results = results.all()

        conn_count_obj = dict()
        conn_status_key = redis.get_user_conn_key(user.id, "count")
        for item in results:
            conn_status = ConnectionStatusEnum(item.status)
            conn_count_obj[conn_status] = item.total

            await r.hset(
                conn_status_key,
                conn_status.name,
                conn_count_obj[conn_status],
            )
            await r.expire(conn_status_key, cache_ttl)

        return conn_count_obj

    async def expire_conn_count(
        self,
        db: AsyncSession,
        r: aioredis.Redis,
        *,
        user_id: int,
    ):
        """
        Clears connection count stored in redis.
        """

        conn_status_key = redis.get_user_conn_key(user_id, "count")
        await r.unlink(conn_status_key)
        return


userconnection = CRUDUserConnection(UserConnection)
