from copy import copy
from typing import Dict, List, Optional, Union

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.core.roles_permissions import UserRolesEnum
from app.crud.base import CRUDBase
from app.db.mixin import EPOCH
from app.models.role_permission import Role
from app.models.room import Room
from app.models.room_category import RoomCategory
from app.models.room_category_assoc import RoomCategoryAssoc
from app.models.user import User, UserRoom
from app.schemas.room import (
    RoomCategorySetOut,
    RoomCreate,
    RoomCreateIn,
    RoomListOut,
    RoomOut,
    RoomUpdateIn,
)
from app.schemas.room_category import RoomCategoryOut
from app.schemas.user_profile import UserBasicDetailsOut
from app.schemas.user_room import RoomUsersOut

MAX_COUNT = 50


class CRUDRoom(CRUDBase[Room, RoomCreate, RoomCreateIn]):
    """
    CRUD for :any:`Room model<models.room.Room>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def update(
        self,
        db: AsyncSession,
        *,
        db_obj: Room,
        obj_in: RoomUpdateIn,
        user: Optional[User] = None,
    ) -> Room:
        """Update an existing row"""
        fields = [
            "name",
            "description",
            "short_description",
            "categories",
            "banner_image",
            "display_photo",
        ]
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.dict(exclude_unset=True)

        for field in fields:
            if field in update_data:
                setattr(db_obj, field, update_data[field])
        db_obj.updated_by = user

        await self.commit_refresh(db, db_obj)
        return db_obj

    async def get_all(
        self,
        db: AsyncSession,
        *,
        rcid: Optional[int] = None,
        rcids: Optional[List[int]] = None,
        rids: Optional[List[int]] = None,
        user: Optional[User] = None,
        followed: Optional[bool] = None,
        recommended: Optional[bool] = None,
        start: Optional[int] = None,
        count: Optional[int] = None,
    ) -> Union[List[RoomCategorySetOut], RoomListOut]:
        if start is not None and count is not None:
            start = max(0, start)
            count = min(MAX_COUNT, max(1, count))

        rooms_q = (
            select(Room)  # noqa
            .join(RoomCategoryAssoc, RoomCategoryAssoc.room_id == Room.id)  # noqa
            .join(
                RoomCategory,
                RoomCategory.id == RoomCategoryAssoc.category_id,  # noqa
            )
            .filter(Room.deleted_at == EPOCH)  # noqa
            .filter(RoomCategory.deleted_at == EPOCH)
            .group_by(Room.id)
            .order_by(Room.name)
        )

        if rcid:
            rooms_q = rooms_q.filter(RoomCategory.id == rcid)

        if (followed or recommended) and user:
            # Initializing with 0, to cover users who followed no rooms.
            # Translates to sql 'rooms in (0)', when there are no rooms.
            user_rooms = [0]
            user_room_categories = {}
            user_rooms_copy = copy(user.rooms)
            for user_room in user_rooms_copy:
                user_rooms.append(user_room.room.id)
                for category in user_room.room.categories:
                    user_room_categories[category.id] = ""

            if recommended:
                rooms_q = rooms_q.where(Room.id.notin_(user_rooms))
                rcids = [] if rcids is None else rcids
                rcids.extend(user_room_categories.keys())

            if followed:
                rids = [] if rids is None else rids
                rids.extend(user_rooms)

        if rids:
            rooms_q = rooms_q.where(Room.id.in_(rids))

        if rcids:
            rooms_q = rooms_q.where(RoomCategory.id.in_(rcids))

        total = await crud.room.count_q(db, query=rooms_q)

        if start is not None and count is not None:
            rooms_q = rooms_q.offset(start).limit(count)

        rooms = await self.get_q(db, query=rooms_q)

        rooms_list = []
        for room_obj in rooms:
            room_cat = [RoomCategoryOut.from_orm(c) for c in room_obj.categories]
            room_d = room_obj.__dict__
            room_d["categories"] = room_cat
            room_users = await crud.room.get_all_users(
                db, id=room_obj.id, total_only=True
            )
            room_d["total_users"] = room_users.total

            room_out = RoomOut(**room_d)
            rooms_list.append(room_out)

        rooms_out = RoomListOut(rooms=rooms_list, start=start, count=count, total=total)
        return rooms_out

    @staticmethod
    async def get_all_users(
        db: AsyncSession,
        *,
        id: int,
        start: Optional[int] = 0,
        count: Optional[int] = 10,
        total_only: Optional[bool] = False,
        ids_only: Optional[bool] = False,
    ) -> Union[RoomUsersOut, Dict]:
        start = max(0, start)
        count = min(50, max(1, count))

        users_q = select(User)
        if ids_only:
            users_q = select(User.id)

        users_q = (
            users_q.distinct(User.id)
            .join(UserRoom, UserRoom.user_id == User.id)
            .join(Room, UserRoom.room_id == Room.id)
            .join(Role, UserRoom.role_id == Role.id)
            .filter(Room.deleted_at == EPOCH)  # noqa
            .filter(User.deleted_at == EPOCH)  # noqa
            .filter(User.deactivated_at == None)  # noqa
            .filter(Room.id == id)  # noqa
        )

        total = await crud.user.count_q(db, query=users_q)

        if total_only:
            ru = RoomUsersOut(total=total)
        elif ids_only:
            users_q = users_q.offset(start).limit(count)
            users = await crud.user.get_q(db, query=users_q)
            ru = {"users": users, "total": total}
        else:
            users_q = users_q.offset(start).limit(count)
            users = await crud.user.get_q(db, query=users_q)
            users_pr = []
            for user in users:
                up = UserBasicDetailsOut()
                up.load(user, user.profile)
                users_pr.append(up)

            ru = RoomUsersOut(total=total, users=users_pr)
        return ru

    @staticmethod
    async def get_user_room(
        db: AsyncSession, *, user: User, room: Room, role: UserRolesEnum
    ) -> Optional[UserRoom]:
        user_rooms = copy(user.rooms)
        role_obj = await crud.role.get_by_name(db, name=role)

        for user_room in user_rooms:
            if user_room.room.id == room.id and user_room.role.id == role_obj.id:
                return user_room

        return None

    @staticmethod
    async def add_user_to_room(
        db: AsyncSession,
        *,
        user: User,
        room: Room,
        roles: Optional[List[UserRolesEnum]] = None,
    ) -> Optional[List[UserRoom]]:
        roles = [UserRolesEnum.ROOM_USER] if roles is None else roles
        try:
            user_rooms = []
            for role in roles:
                role_obj = await crud.role.get_by_name(db, name=role)
                if not role_obj:
                    continue

                user_room = UserRoom(user=user, room=room, role=role_obj)  # noqa
                db.add(user_room)
                user_rooms.append(user_room)
            await db.commit()
            return user_rooms
        except Exception as e:
            print(e)

        return None

    @staticmethod
    async def delete_user_from_room(db: AsyncSession, *, user: User, room: Room = None):
        user_rooms = copy(user.rooms)

        deleted_user_rooms = []
        try:
            for user_room in user_rooms:
                if room is None or user_room.room.id == room.id:
                    await db.delete(user_room)
                    deleted_user_rooms.append(user_room)

            await db.commit()
            return deleted_user_rooms
        except Exception as e:
            print(e)

        return False

    @staticmethod
    async def get_user_room_roles(db: AsyncSession, *, user: User, room: Room):
        user_rooms = copy(user.rooms)

        roles = []
        for user_room in user_rooms:
            if user_room.room.id == room.id:
                roles.append(user_room.role)

        return roles

    @staticmethod
    async def get_rooms_from_user(db: AsyncSession, *, user: User):
        user_rooms = copy(user.rooms)

        rooms = {}
        for user_room in user_rooms:
            rooms[user_room.room.id] = user_room.room

        return list(rooms.values())


room = CRUDRoom(Room)
