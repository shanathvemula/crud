from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.profile import Profile
from app.schemas.profile import ProfileCreate, ProfileIn


class CRUDProfile(CRUDBase[Profile, ProfileCreate, ProfileIn]):
    """
    CRUD for :any:`Profile model<models.profile.Profile>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create(self, db: AsyncSession, *, profile_cr: ProfileCreate) -> Profile:
        db_obj = Profile(
            display_name=profile_cr.display_name,
            first_name=profile_cr.first_name,
            last_name=profile_cr.last_name,
            photo_url=profile_cr.photo_url,
            user_id=profile_cr.user_id,
            public=profile_cr.public,
            phone_number=profile_cr.phone_number,
            country_code=profile_cr.country_code,
            phone_number_verified=profile_cr.phone_number_verified,
            address=profile_cr.address,
        )
        db_obj = await self.commit_refresh(db, db_obj)
        return db_obj

    async def update(
        self, db: AsyncSession, *, db_obj: Profile, profile_in: ProfileIn
    ) -> Profile:
        """Update an existing user's profile"""
        obj_data = db_obj.__dict__
        if isinstance(profile_in, dict):
            update_data = profile_in
        else:
            update_data = profile_in.dict(exclude_unset=True)
        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])
        db_obj = await self.commit_refresh(db, db_obj)
        return db_obj


profile = CRUDProfile(Profile)
