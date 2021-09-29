from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.action import UserCommentAction, UserContentAction
from app.models.comment import Comment
from app.models.content import Content
from app.models.user import User
from app.schemas.action import UserCommentActionCreate, UserContentActionCreate


class CRUDContentAction(
    CRUDBase[UserContentAction, UserContentActionCreate, UserContentActionCreate]
):
    """
    CRUD for :any:`ContentAction model<models.action.UserContentAction>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def get_or_create_by_content_user(
        self, db: AsyncSession, *, content: Content, user: User
    ) -> Optional[UserContentAction]:
        q = self.query().filter(
            self.model.content_id == content.id, self.model.created_by_id == user.id
        )

        # to protect content from being changed by any other lines,
        # specifically the next line.
        if content in db.dirty:
            db.expunge(content)
        result = await self.get_q(db, query=q)
        if result:
            return result[0]
        else:
            uca_cr = UserContentActionCreate(content_id=content.id)
            uca_obj = await self.create(db, obj_in=uca_cr, user=user)
            return uca_obj


class CRUDCommentAction(
    CRUDBase[UserCommentAction, UserCommentActionCreate, UserCommentActionCreate]
):
    """
    CRUD for :any:`CommentAction model<models.action.UserCommentAction>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def get_or_create_by_comment_user(
        self, db: AsyncSession, *, comment: Comment, user: User
    ) -> Optional[UserCommentAction]:
        q = self.query().filter(
            self.model.comment_id == comment.id, self.model.created_by_id == user.id
        )
        result = await self.get_q(db, query=q)
        if result:
            return result[0]
        else:
            uca_cr = UserCommentActionCreate(comment_id=comment.id)
            uca_obj = await self.create(db, obj_in=uca_cr, user=user)
            return uca_obj


content_action = CRUDContentAction(UserContentAction)
comment_action = CRUDCommentAction(UserCommentAction)
