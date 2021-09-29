from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.models.user import User
from app.models.user_skill import UserSkill
from app.schemas.skill import SkillCreate
from app.schemas.user_skill import UserSkillCreate, UserSkillUpdate


class CRUDUserSkill(CRUDBase[UserSkill, UserSkillCreate, UserSkillUpdate]):
    """
    CRUD for :any:`UserSkill model<models.user_skill.UserSkill>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: UserSkillCreate,
        user: User,
        db_obj: Optional[UserSkill] = None,
    ) -> UserSkill:
        """Create a new User Skill or update an existing one"""
        fields = ["level", "learning_year"]

        if db_obj is None:
            # this isn't an update.
            # create a new User Skill object
            db_obj = UserSkill(level=obj_in.level, learning_year=obj_in.learning_year)
            db_obj.created_by = user
        else:
            update_data = obj_in.dict(exclude_unset=True)
            for field in fields:
                if field in update_data:
                    setattr(db_obj, field, update_data[field])
            db_obj.updated_by = user

        if obj_in.skill:
            skill_in = SkillCreate(name=obj_in.skill.name)
            skill = await crud.skill.get_or_create(db, obj_in=skill_in, user=user)
            db_obj.skill = skill

        await self.commit_refresh(db, db_obj)
        return db_obj


userskill = CRUDUserSkill(UserSkill)
