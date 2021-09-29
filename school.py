from app.crud.base import CRUDBase
from app.models.school import School
from app.schemas.school import SchoolCreate, SchoolIn


class CRUDSchool(CRUDBase[School, SchoolCreate, SchoolIn]):
    """
    CRUD for :any:`School model<models.school.School>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


school = CRUDSchool(School)
