import json
from typing import List, Optional

import aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.db import redis
from app.models.user import User
from app.models.user_interest import UserInterest
from app.schemas.interest import InterestCreate
from app.schemas.user_interest import UserInterestCreate, UserInterestUpdate


class CRUDUserInterest(CRUDBase[UserInterest, UserInterestCreate, UserInterestUpdate]):
    """
    CRUD for :any:`UserInterest model<models.user_interest.UserInterest>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create_or_update(
        self,
        db: AsyncSession,
        r: aioredis.Redis,
        *,
        obj_in: UserInterestCreate,
        user: User,
        db_obj: Optional[UserInterest] = None,
    ) -> UserInterest:
        """Create a new User_Interest or update existing one

        also all supporting interest etc.
        """
        fields = ["start_date"]
        if db_obj is None:
            # this isn't an update.
            # create a new user interest object
            db_obj = UserInterest(start_date=obj_in.start_date)
            db_obj.created_by = user
        else:
            update_data = obj_in.dict(exclude_unset=True)
            for field in fields:
                if field in update_data:
                    setattr(db_obj, field, update_data[field])
            db_obj.updated_by = user

        if obj_in.interest is not None:
            interest_in = InterestCreate(name=obj_in.interest.name)
            interest = await crud.interest.get_or_create(
                db, obj_in=interest_in, user=user
            )
            db_obj.interest = interest

        await self.commit_refresh(db, db_obj)
        await self.r_cache_update(db, r, user=user)
        return db_obj

    @staticmethod
    async def r_refresh_cache(
        r: aioredis.Redis, *, uis: List[UserInterest], user: User
    ) -> UserInterest:
        """
        Format & write User Interests to Redis
        """
        user_interests = []
        for ui in uis:
            user_interests.append(
                {
                    "id": ui.id,
                    "interest": {"id": ui.interest.id, "name": ui.interest.name},
                    "start_date": (
                        ui.start_date.strftime("%Y-%m-%d") if ui.start_date else None
                    ),
                }
            )

        await redis.put_to_cache(
            r,
            key=redis.get_uid_key(user.firebase_uid),
            field="interests",
            value=json.dumps(user_interests),
        )

        return user_interests

    @staticmethod
    async def r_cache_update(
        db: AsyncSession,
        r: aioredis.Redis,
        *,
        user: User,
    ):
        """
        Update User Interests From DB to Redis
        """
        userinterests_q = crud.userinterest.get_multi(created_by=user)
        ui_objs = await crud.userinterest.get_q(db, query=userinterests_q)
        user_interests = await userinterest.r_refresh_cache(r, uis=ui_objs, user=user)
        return user_interests

    @staticmethod
    async def r_get_q(
        db: AsyncSession,
        r: aioredis.Redis,
        *,
        user: User,
    ) -> UserInterest:
        """
        Fetch User Interests from cache & update if doesn't exist
        """
        uis_from_cache = await redis.get_from_cache(
            r,
            key=redis.get_uid_key(user.firebase_uid),
            field="interests",
        )

        if uis_from_cache:
            user_interests = json.loads(uis_from_cache)
        else:
            user_interests = await userinterest.r_cache_update(db, r, user=user)

        return user_interests


userinterest = CRUDUserInterest(UserInterest)
