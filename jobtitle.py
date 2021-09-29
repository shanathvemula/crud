from app.crud.base import CRUDBase
from app.models.jobtitle import JobTitle
from app.schemas.jobtitle import JobTitleCreate, JobTitleIn


class CRUDJobTitle(CRUDBase[JobTitle, JobTitleCreate, JobTitleIn]):
    """
    CRUD for :any:`JobTitle model<models.jobtitle.JobTitle>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


jobtitle = CRUDJobTitle(JobTitle)
