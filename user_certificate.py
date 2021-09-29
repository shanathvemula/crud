from typing import Optional

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.models.user import User
from app.models.user_certificate import UserCertificate
from app.schemas.certificate import CertificateID, OrgCertificateCreate
from app.schemas.organization import OrganizationCreate
from app.schemas.user_certificate import UserCertificateCreate, UserCertificateUpdate


class CRUDUserCertificate(
    CRUDBase[UserCertificate, UserCertificateCreate, UserCertificateUpdate]
):
    """
    CRUD for :any:`UserCertificate model<models.user_certificate.UserCertificate>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: UserCertificateCreate,
        user: User,
        db_obj: Optional[UserCertificate] = None,
    ) -> UserCertificate:
        """Create a new user certificate or update existing education

        also all supporting organization, certificate, etc.
        """
        fields = [
            "credential_id",
            "credential_url",
            "can_expire",
            "issued_date",
            "expiry_date",
        ]
        if db_obj is None:
            # this isn't an update.
            # create a new user certificate object
            db_obj = UserCertificate(
                credential_id=obj_in.credential_id,
                credential_url=obj_in.credential_url,
                can_expire=obj_in.can_expire,
                issued_date=obj_in.issued_date,
                expiry_date=obj_in.expiry_date,
            )
            db_obj.created_by = user
        else:
            update_data = obj_in.dict(exclude_unset=True)
            for field in fields:
                if field in update_data:
                    setattr(db_obj, field, update_data[field])
            db_obj.updated_by = user

        if obj_in.certificate is not None:
            if isinstance(obj_in.certificate, CertificateID):
                # obj_in.certificate just has the certificate ID, so use it directly.
                org_certificate = await crud.certificate.get(
                    db, id=obj_in.certificate.id
                )
                if org_certificate is None:
                    # Certificate ID does not exist
                    raise HTTPException(
                        status_code=404, detail="Certificate does not exist."
                    )
            else:
                # Have to get or create the org and certificate from DB
                organization = OrganizationCreate(
                    name=obj_in.certificate.organization.name
                )
                organization = await crud.organization.get_or_create(
                    db, obj_in=organization, user=user
                )

                org_certificate_cr = OrgCertificateCreate(
                    name=obj_in.certificate.name, organization_id=organization.id
                )
                org_certificate = await crud.certificate.get_or_create(
                    db, obj_in=org_certificate_cr, user=user
                )
            db_obj.certificate = org_certificate

        await self.commit_refresh(db, db_obj)

        return db_obj


usercertificate = CRUDUserCertificate(UserCertificate)
