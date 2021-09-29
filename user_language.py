from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.models.user import User
from app.models.user_language import UserLanguage
from app.schemas.language import LanguageCode, LanguageID
from app.schemas.user_language import UserLanguageCreate, UserLanguageUpdate


class CRUDUserLanguage(CRUDBase[UserLanguage, UserLanguageCreate, UserLanguageUpdate]):
    """
    CRUD for :any:`UserLanguage model<models.user_language.UserLanguage>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: UserLanguageCreate,
        user: User,
        db_obj: Optional[UserLanguage] = None,
    ) -> UserLanguage:
        """Create a new User Language or update the existing one"""
        fields = ["level"]
        if db_obj is None:
            # this isn't an update.
            # create a new UserLanguage object
            db_obj = UserLanguage(level=obj_in.level)
            db_obj.created_by = user
        else:
            update_data = obj_in.dict(exclude_unset=True)
            for field in fields:
                if field in update_data:
                    setattr(db_obj, field, update_data[field])

            db_obj.updated_by = user

        if isinstance(obj_in.language, LanguageID) and (
            (db_obj.language and db_obj.language.id != obj_in.language.id)
            or db_obj.language is None
        ):
            language = await crud.language.get(db, id=obj_in.language.id)
            db_obj.language = language
        elif isinstance(obj_in.language, LanguageCode) and (
            (db_obj.language and db_obj.language.code != obj_in.language.code)
            or db_obj.language is None
        ):
            language_q = crud.language.get_multi(code=obj_in.language.code)
            languages = await crud.language.get_q(db, query=language_q)
            if languages:
                language = languages[0]
                db_obj.language = language

        if db_obj.language is not None:
            await self.commit_refresh(db, db_obj)
            return db_obj

        raise Exception


userlanguage = CRUDUserLanguage(UserLanguage)
