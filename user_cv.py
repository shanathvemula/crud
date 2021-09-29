from app.crud.base import CRUDBase
from app.models.user_cv import UserCV
from app.schemas.user_cv import UserCVCreate, UserCVUpdate


class CRUDUserCV(CRUDBase[UserCV, UserCVCreate, UserCVUpdate]):
    """
    CRUD for :any:`UserCV model<models.user_cv.UserCV>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


user_cv = CRUDUserCV(UserCV)
