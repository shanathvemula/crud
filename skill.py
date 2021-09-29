from app.crud.base import CRUDBase
from app.models.skill import Skill
from app.schemas.skill import SkillCreate, SkillUpdate


class CRUDSkill(CRUDBase[Skill, SkillCreate, SkillUpdate]):
    """
    CRUD for :any:`Skill model<models.skill.Skill>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


skill = CRUDSkill(Skill)
