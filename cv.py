import logging

import aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.models.cv import CVUpload
from app.models.user import User
from app.schemas import ProfileIn
from app.schemas.cv import CV, CVUploadCreate


class CRUDCVUpload(CRUDBase[CVUpload, CVUploadCreate, CVUploadCreate]):
    """
    CRUD for :any:`CV model<models.cv.CVUpload>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create(
        self, db: AsyncSession, *, obj_in: CVUploadCreate, user: User
    ) -> CVUpload:
        """Insert a row in cv_uploads table"""
        db_obj = self.model(**obj_in.dict())
        db_obj.created_by = user
        await self.commit_refresh(db, db_obj)
        return db_obj

    async def save_parsed_cv(
        self,
        db: AsyncSession,
        r: aioredis.Redis,
        *,
        parsed_cv: CV,
        user: User,
    ):
        details = parsed_cv.user_details
        profile_in = ProfileIn(
            first_name=details.first_name,
            last_name=details.last_name,
            phone_number=details.phone,
            country_code=details.country_code,
            address=details.address,
            gender=details.gender,  # TODO add after adding column
        )
        try:
            await crud.profile.update(db, db_obj=user.profile, profile_in=profile_in)
        except Exception:
            logging.error("user profile update failed.")

        if parsed_cv.educations:
            for edu in parsed_cv.educations:
                try:
                    await crud.education.create_or_update(db, obj_in=edu, user=user)
                except Exception:
                    logging.error("user education create failed.")

        if parsed_cv.experiences:
            for exp in parsed_cv.experiences:
                try:
                    await crud.experience.create_or_update(db, obj_in=exp, user=user)
                except Exception:
                    logging.error("user experience create failed.")

        if parsed_cv.skills:
            for skill in parsed_cv.skills:
                try:
                    await crud.userskill.create_or_update(db, obj_in=skill, user=user)
                except Exception:
                    logging.error("user skill create failed.")

        if parsed_cv.interests:
            for interest in parsed_cv.interests:
                try:
                    await crud.userinterest.create_or_update(
                        db, r, obj_in=interest, user=user
                    )
                except Exception:
                    logging.error("user interests create failed.")

        if parsed_cv.languages:
            for lang in parsed_cv.languages:
                try:
                    await crud.userlanguage.create_or_update(db, obj_in=lang, user=user)
                except Exception:
                    logging.error("user language create failed.")

    async def get_basic_details(self, db: AsyncSession, *, user: User) -> CVUpload:
        query = select(
            CVUpload.url,
            CVUpload.created_by_id,
            CVUpload.created_at,
            CVUpload.updated_by_id,
            CVUpload.updated_at,
        ).where(CVUpload.created_by_id == user.id)

        results = await db.execute(query)
        results = results.all()
        return results


cv_upload = CRUDCVUpload(CVUpload)
