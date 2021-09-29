from app.crud.base import CRUDBase
from app.models.company import Company
from app.schemas.company import CompanyCreate, CompanyIn


class CRUDCompany(CRUDBase[Company, CompanyCreate, CompanyIn]):
    """
    CRUD for :any:`Company model<models.company.Company>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


company = CRUDCompany(Company)
