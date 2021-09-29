from app.crud.base import CRUDBase
from app.models.organization import Organization
from app.schemas.organization import OrganizationCreate, OrganizationUpdate


class CRUDOrganization(CRUDBase[Organization, OrganizationCreate, OrganizationUpdate]):
    """
    CRUD for :any:`Organization model<models.organization.Organization>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


organization = CRUDOrganization(Organization)
