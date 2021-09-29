from app.crud.base import CRUDBase
from app.models.degree import Degree
from app.schemas.degree import DegreeCreate, DegreeIn


class CRUDDegree(CRUDBase[Degree, DegreeCreate, DegreeIn]):
    """
    CRUD for :any:`Degree model<models.degree.Degree>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


degree = CRUDDegree(Degree)
