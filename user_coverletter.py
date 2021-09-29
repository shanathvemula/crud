from app.crud.base import CRUDBase
from app.models.user_coverletter import UserCoverLetter
from app.schemas.user_coverletter import UserCoverLetterCreate, UserCoverLetterIn


class CRUDUserCoverLetter(
    CRUDBase[UserCoverLetter, UserCoverLetterCreate, UserCoverLetterIn]
):
    """
    CRUD for :any:`UserCoverLetter model<models.user_coverletter.UserCoverLetter>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


user_coverletter = CRUDUserCoverLetter(UserCoverLetter)
