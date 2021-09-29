from app.crud.base import CRUDBase
from app.models.certificate import Certificate
from app.schemas.certificate import CertificateUpdate, OrgCertificateCreate


class CRUDCertificate(CRUDBase[Certificate, OrgCertificateCreate, CertificateUpdate]):
    """
    CRUD for :any:`Certificate model<models.certificate.Certificate>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


certificate = CRUDCertificate(Certificate)
