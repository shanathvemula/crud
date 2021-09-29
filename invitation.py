import hashlib
from typing import List, Optional

from pydantic import EmailStr
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.crud.base import CRUDBase
from app.models.invitation import Invitation
from app.schemas.invitation import InvitationBasicOut, InvitationCreate, InvitationIn

MAX_INV_COUNT = 100


class CRUDInvitation(CRUDBase[Invitation, InvitationCreate, InvitationIn]):
    """
    CRUD for :any:`Invitation model<models.invitation.Invitation>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def validate_invitation(
        self, db: AsyncSession, email: EmailStr, code: str
    ) -> bool:
        q = self.get_multi(email=email, code=code, used=False)
        invitation_obj = await self.get_q_one(db, query=q)
        if invitation_obj is None:
            return False
        return True

    async def create_invitation(
        self, db: AsyncSession, email: EmailStr, name: str
    ) -> Optional[Invitation]:
        # Check if user already has an unused invitation
        q = self.get_multi(email=email, used=False)
        existing_invitation = await self.get_q_one(db, query=q)
        if existing_invitation:
            return None

        code = hashlib.md5(
            "{}${}${}".format(name, email, settings.INVITE_CODE_SALT).encode("utf-8")
        ).hexdigest()
        invitation_cr = InvitationCreate(name=name, email=email, code=code)
        invitation_obj = await self.create(db, obj_in=invitation_cr)
        return invitation_obj

    async def get_all(
        self,
        db: AsyncSession,
        *,
        last_id: int = None,
        count: int = 10,
    ) -> List[InvitationBasicOut]:
        count = min(MAX_INV_COUNT, max(1, count))

        query = self.query()
        query = query.order_by(Invitation.created_at.desc())

        if last_id is not None:
            last_id = max(1, last_id)
            query = query.where(Invitation.id < last_id)
        if count:
            query = query.limit(count)

        invitations = await self.get_q(db, query)
        return invitations


invitation = CRUDInvitation(Invitation)
