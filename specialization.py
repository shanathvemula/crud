from app.crud.base import CRUDBase
from app.models.specialization import Specialization
from app.schemas.specialization import SpecializationCreate, SpecializationIn


class CRUDSpecialization(
    CRUDBase[Specialization, SpecializationCreate, SpecializationIn]
):
    """
    CRUD for :any:`Specialization model<models.specialization.Specialization>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


specialization = CRUDSpecialization(Specialization)
