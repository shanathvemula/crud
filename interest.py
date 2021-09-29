from app.crud.base import CRUDBase
from app.models.interest import Interest
from app.schemas.interest import InterestCreate, InterestUpdate


class CRUDInterest(CRUDBase[Interest, InterestCreate, InterestUpdate]):
    """
    CRUD for :any:`Interest model<models.interest.Interest>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


interest = CRUDInterest(Interest)
