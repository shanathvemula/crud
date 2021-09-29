import random
from typing import Optional

import aioredis
import base58
from aioredis_bloom import BloomFilter
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.constants import USER_NOT_FOUND
from app.core.roles_permissions import UserRolesEnum
from app.crud.base import CRUDBase
from app.db import redis
from app.models.profile import Profile
from app.models.user import QuestStageEnum, User, UserLink, UserRoom
from app.schemas.user import UserCreate, UserDeleteIn, UserRoomCreate, UserUpdate
from app.schemas.user_connection import ConnectionStatusEnum
from app.schemas.user_link import UserLinkCreate
from app.schemas.user_profile import UserMicroProfileOut, UserProfileIn, UserProfileOut

REFERRAL_MIN = 58 ** 4  # 11 Million
REFERRAL_MAX = 58 ** 6  # 38 Billion


class CRUDUser(CRUDBase[User, UserCreate, UserUpdate]):
    """
    CRUD for :any:`Users model<models.user.User>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def get_by_email(self, db: AsyncSession, *, email: str) -> Optional[User]:
        stmt = self.query().filter_by(email=email)
        result = await db.execute(stmt)
        return result.scalar()

    async def get_by_uid(self, db: AsyncSession, *, uid: str) -> Optional[User]:
        stmt = self.query().filter_by(firebase_uid=uid)
        result = await db.execute(stmt)
        return result.scalar()

    async def get_by_referral_code(
        self, db: AsyncSession, *, referral_code: str
    ) -> Optional[User]:
        stmt = self.query().filter_by(referral_code=referral_code)
        result = await db.execute(stmt)
        return result.scalar()

    @staticmethod
    async def generate_referral_code(bloom: BloomFilter) -> str:
        """
        Generates a unique referral code for the user.
        Uses bloom filter data structure through Redis for the uniqueness.

        Returns:
            Returns the generated referral code
        """
        unique = False
        while not unique:
            rand_num = random.randint(REFERRAL_MIN, REFERRAL_MAX)
            referral_code = base58.b58encode_int(rand_num)
            unique = not await bloom.contains(referral_code)
        return referral_code.decode("utf8")

    @staticmethod
    async def set_referral_code(bloom: BloomFilter, referral_code: str):
        await bloom.add(referral_code)

    async def create(
        self, db: AsyncSession, bloom: BloomFilter, *, user_profile_in: UserProfileIn
    ) -> Optional[User]:
        if user_profile_in.referral_code:
            referred_user = await self.get_by_referral_code(
                db, referral_code=user_profile_in.referral_code
            )
        else:
            referred_user = None
        referral_code = await self.generate_referral_code(bloom)
        user_obj = User(
            firebase_uid=user_profile_in.firebase_uid,
            email=user_profile_in.email,
            user_name=user_profile_in.user_name,
            email_verified=user_profile_in.email_verified,
            provider=user_profile_in.provider,
            quest_stage=QuestStageEnum.STAGE_1,
            referred_by=referred_user,
            referral_code=referral_code,
        )
        Profile(
            display_name=user_profile_in.display_name,
            first_name=user_profile_in.first_name,
            last_name=user_profile_in.last_name,
            photo_url=user_profile_in.photo_url,
            user=user_obj,
            public=user_profile_in.public,
            phone_number=user_profile_in.phone_number,
            phone_number_verified=user_profile_in.phone_number_verified,
            address=user_profile_in.address,
        )
        user_obj = await self.commit_refresh(db, user_obj)
        await self.set_referral_code(bloom, user_obj.referral_code)
        return user_obj

    async def delete(
        self, db: AsyncSession, *, uid: str, delete_in: UserDeleteIn
    ) -> User:
        user = await self.get_by_uid(db, uid=uid)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        await self.update(db, db_obj=user, obj_in=delete_in)
        user.profile.delete()
        user.delete()
        user = await self.commit_refresh(db, user)
        return user

    @staticmethod
    async def get_conn_status(
        r: aioredis.Redis, *, auth_user_id: int, conn_user_id: int
    ) -> Optional[ConnectionStatusEnum]:
        status = None
        if await r.sismember(
            redis.get_user_conn_key(auth_user_id, ConnectionStatusEnum.ACTIVE),
            conn_user_id,
        ):
            status = ConnectionStatusEnum.ACTIVE
        elif await r.sismember(
            redis.get_user_conn_key(auth_user_id, ConnectionStatusEnum.SENT),
            conn_user_id,
        ):
            status = ConnectionStatusEnum.SENT
        elif await r.sismember(
            redis.get_user_conn_key(auth_user_id, ConnectionStatusEnum.RECEIVED),
            conn_user_id,
        ):
            status = ConnectionStatusEnum.RECEIVED

        return status

    async def add_role_to_user(
        self, db: AsyncSession, *, user: User, role_name: UserRolesEnum
    ) -> User:
        role_obj = await crud.role.get_by_name(db, name=role_name)
        user.roles.append(role_obj)
        await self.commit_refresh(db, user)
        return user

    async def get_micro_details(
        self, db: AsyncSession, uid: str
    ) -> UserMicroProfileOut:
        """
        Get full user details for micro service APIs.

        Args:
            uid: :any:`User<models.user.User>` firebase_uid
        Returns:
            Returns user details object
        """

        if uid is None:
            raise HTTPException(status_code=400, detail=USER_NOT_FOUND)

        user = await crud.user.get_by_uid(db, uid=uid)
        if user is None:
            raise HTTPException(status_code=404, detail=USER_NOT_FOUND)

        # Fill in the basic user details
        user_profile = UserProfileOut()
        user_profile.load_from_user(user)
        user_profile.load_from_profile(user.profile)
        result = UserMicroProfileOut(basic=user_profile)

        # Get all the detailed info for the user
        educations_q = crud.education.get_multi(created_by=user)
        user_educations = await crud.education.get_q(
            db, query=educations_q.order_by(text("start_date desc"))
        )
        experiences_q = crud.experience.get_multi(created_by=user)
        user_experiences = await crud.experience.get_q(
            db, query=experiences_q.order_by(text("start_date desc"))
        )
        user_languages_q = crud.userlanguage.get_multi(created_by=user)
        user_languages = await crud.userlanguage.get_q(db, query=user_languages_q)
        user_interests_q = crud.userinterest.get_multi(created_by=user)
        user_interests = await crud.userinterest.get_q(db, query=user_interests_q)
        user_certificates_q = crud.usercertificate.get_multi(created_by=user)
        user_certificates = await crud.usercertificate.get_q(
            db, query=user_certificates_q.order_by(text("issued_date desc"))
        )
        user_skills_q = crud.userskill.get_multi(created_by=user)
        user_skills = await crud.userskill.get_q(db, query=user_skills_q)
        user_projects_q = crud.project.get_multi(created_by=user)
        user_projects = await crud.project.get_q(db, query=user_projects_q)
        user_rooms = await crud.room.get_rooms_from_user(db, user=user)

        # Fill in the detailed info for the user
        result.education = user_educations
        result.experience = user_experiences
        result.interests = user_interests
        result.skills = user_skills
        result.languages = user_languages
        result.certificates = user_certificates
        result.projects = user_projects
        result.rooms = user_rooms

        return result


class CRUDUserLink(CRUDBase[UserLink, UserLinkCreate, UserLinkCreate]):
    """
    CRUD for :any:`UserLink model<models.user.UserLink>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


class CRUDUserRoom(CRUDBase[UserRoom, UserRoomCreate, UserRoomCreate]):
    """
    CRUD for :any:`UserRoom model<models.user.User.rooms>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


user = CRUDUser(User)
user_link = CRUDUserLink(UserLink)
user_room = CRUDUserRoom(UserRoom)
