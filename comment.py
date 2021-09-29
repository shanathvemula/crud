from datetime import datetime
from typing import Dict, List, Optional, Union

from sqlalchemy import distinct, func, select, true, update
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.db.mixin import EPOCH
from app.models.action import UserCommentAction
from app.models.comment import Comment
from app.models.user import User
from app.schemas.comment import (
    CommentCreate,
    CommentL1Out,
    CommentL2Out,
    CommentL3Out,
    CommentListOut,
    CommentUpdate,
)
from app.schemas.content import ContentDeletedUserOut, ContentInBase, ContentUserOut

MAX_COMM_COUNT = 25
MAX_SUB_COMM_COUNT = 10


class CRUDComment(CRUDBase[Comment, CommentCreate, CommentUpdate]):
    """
    CRUD for :any:`Comments model<models.comment.Comment>`

    Comments are stored like tree.
    Each row is either a comment or a sub-comment of a comment,
    identified with parent_id.
    Sub-Comments support infinite level,
    however is restricted to 3 levels per request.

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def get_all(
        self,
        db: AsyncSession,
        *,
        content_id: int = None,
        comment_id: int = None,
        last_id: int = None,
        count: int = 10,
        check_comments: bool = False,
        total_only: bool = False,
        user: Optional[User] = None,
    ) -> Dict:
        """
        Get all comments on a single level.

        * top-level if no comment_id is given
        * sub-level if a comment_id is given

        Args:
            content_id: id of :any:`Content<models.content.Content>`
            comment_id: id of :any:`Comment<models.comment.Comment>`
            last_id: last id of :any:`Comment<models.comment.Comment>`
            count: number of results to return
            check_comments: returns total as 1,
                if a given comment_id has sub-comments
            total_only: returns only total for a level

        Returns:
            Returns a dict of sub set of comments & total comments count
        """
        count = min(MAX_COMM_COUNT, max(1, count))

        query = self.query()
        query = query.filter(Comment.content_id == content_id)

        if not total_only:
            query = query.filter(Comment.parent_id == comment_id)

        comments = None
        if check_comments:
            query = query.limit(1)

        total = await self.count_q(db, query=query)

        if not total_only and not check_comments:
            query = query.order_by(Comment.created_at.desc())
            if last_id:
                query = query.where(Comment.id < last_id)
            if count:
                query = query.limit(count)

            comments = await self.get_q(db, query)

        res = {"comments": comments, "total": total}
        return res

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: Union[CommentCreate, CommentUpdate],
        db_obj: Optional[Comment] = None,
        user: User,
    ) -> Comment:

        if db_obj is None:
            db_obj = Comment(
                body=obj_in.body,
                content_id=obj_in.content_id,
                parent_id=obj_in.parent_id,
                created_by=user,
            )
        else:
            obj_data = obj_in.dict()
            db_data = db_obj.__dict__
            for field in db_data:
                if field in obj_data:
                    setattr(db_obj, field, obj_data[field])
            db_obj.updated_by = user

        await self.commit_refresh(db, db_obj)
        return db_obj

    async def delete(self, db: AsyncSession, *, id: int, user: User) -> Comment:
        """
        Delete a comment & its tree (3 levels).

        Args:
            id: id of :any:`Comment model<models.comment.Comment>`

        Returns:
            Returns the deleted comment object
        """
        # Fetch comments from all levels of the tree
        l1_comments = (
            select(Comment.id)
            .where(
                Comment.deleted_at == EPOCH,
                Comment.id == id,
            )
            .alias("l1")
        )

        l2_comments = (
            select(Comment.id)
            .where(Comment.deleted_at == EPOCH, Comment.parent_id == l1_comments.c.id)
            .lateral("l2")
        )

        l3_comments = (
            select(Comment.id)
            .where(Comment.deleted_at == EPOCH, Comment.parent_id == l2_comments.c.id)
            .lateral("l3")
        )

        # Join comments of all levels in the tree.
        # Convert all level comment id columns to rows & get unique of ids
        query = select(
            distinct(
                func.unnest(
                    array((l1_comments.c.id, l2_comments.c.id, l3_comments.c.id))
                )
            ).label("id")
        ).select_from(
            l1_comments.outerjoin(l2_comments, true()).outerjoin(l3_comments, true())
        )

        comment_ids = await self.get_q(db, query=query)

        # Update all comments from above ids as deleted
        delete_stmt = (
            update(Comment)
            .where(
                Comment.id.in_(comment_ids),
            )
            .values(
                {Comment.deleted_at: datetime.utcnow(), Comment.updated_by_id: user.id}
            )
        )
        await db.execute(delete_stmt)
        await db.commit()

    @staticmethod
    async def fetch_comments_data(
        db: AsyncSession,
        *,
        comments: List[Comment],
        user: Optional[User] = None,
    ) -> (Dict, Dict):
        """
        Fetch :any:`User<models.user.User>` data &
        :any:`Actions<models.action.UserCommentAction>`
        data for given list of comments

        Args:
            comments: list of comments objects
        Returns:
            Returns a tuple consisting User objects & Action objects
        """

        user_objs = {}
        comment_action_objs = {}
        for comment in comments:
            user_objs[comment.l1_created_by_id] = None
            user_objs[comment.l2_created_by_id] = None
            user_objs[comment.l3_created_by_id] = None
            comment_action_objs[comment.l1_id] = None
            comment_action_objs[comment.l2_id] = None
            comment_action_objs[comment.l3_id] = None

        users_q = select(User).where(User.id.in_(user_objs.keys()))
        results = await db.execute(users_q)
        users = results.scalars().all()

        user_objs = {user.id: user for user in users}

        if user:
            comment_actions_q = crud.comment_action.query().filter(
                UserCommentAction.comment_id.in_(comment_action_objs.keys()),
                UserCommentAction.created_by_id == user.id,
            )
            comment_actions = await crud.comment_action.get_q(
                db, query=comment_actions_q
            )
            comment_action_objs = {
                comment_action.comment_id: comment_action
                for comment_action in comment_actions
            }
        return user_objs, comment_action_objs

    @staticmethod
    async def format_single_comment(
        comment: Comment,
        *,
        level: int = 1,
        index: Dict = {},
        permalink: str,
        users: Dict = {},
        comment_actions: Dict = {},
        comments_out: List = [],
        schema: Union[CommentL1Out, CommentL2Out, CommentL3Out],
    ) -> (Dict, List):
        """
        Construct a single comment structure.

        Args:
            level: :any:`Comment<models.comment.Comment>` level in the comments tree
            index: an indexer with :any:`Comment<models.comment.Comment>` id as key,
                    list index & child index as values
            permalink: permalink of :any:`Content<models.content.Content>` object
            users: User data objects
            comment_actions: Action data objects
            comments_out: comments list of previously formatted comments in the tree
            schema: the specific :any:`comment schema out<schemas.comment>` to use

        Returns:
            Returns a tuple consisting an updated indexer & list of comment objects
        """

        comment_id = getattr(comment, "l{}_id".format(level))

        if comment_id not in index:
            parent_id = getattr(comment, "l{}_parent_id".format(level))
            body = getattr(comment, "l{}_body".format(level))
            created_by_id = getattr(comment, "l{}_created_by_id".format(level))
            created_at = getattr(comment, "l{}_created_at".format(level))
            updated_at = getattr(comment, "l{}_updated_at".format(level))
            comments_total = getattr(comment, "l{}_total".format(level + 1), None)

            user_obj = users[created_by_id]

            if user_obj.deleted_at != EPOCH:
                comment_user = ContentDeletedUserOut(deleted=True)
            else:
                comment_user = ContentUserOut()
                comment_user.load(user_obj, user_obj.profile)

            comment_action = None
            if comment_id in comment_actions:
                comment_action = comment_actions[comment_id]

            comment_out = schema(
                id=comment_id,
                body=body,
                parent_id=parent_id,
                content_link=permalink,
                created_by=comment_user,
                created_at=created_at,
                updated_at=updated_at,
                user_action=comment_action,
                comments_total=comments_total,
                comments=[],
            )
            index[comment_id] = {
                "list_id": len(comments_out),
                "child_index": {},
            }
            comments_out.append(comment_out)

        return index, comments_out

    @staticmethod
    async def format_nested_comments(
        db: AsyncSession,
        *,
        comments: List[Comment],
        permalink: str,
        user: Optional[User] = None,
    ) -> CommentListOut:
        """
        Construct a complete nested comments structure.

        Each level has an indexer (dict) & comments (list).

        * index key is id of the comment.
        * index value (dict) contains,
            * list_id (int) - the index of that comment in the comments list.
            * child_index (dict) - index of the next level.

        Args:
            permalink: permalink of :any:`Content<models.content.Content>` object
            comments: list of comment objects

        Returns:
            Returns comment objects in a nested structure with sub-comment objects
        """

        users, comment_actions = await crud.comment.fetch_comments_data(
            db, comments=comments, user=user
        )

        l1_index = {}
        l1_comments = []
        for comment in comments:
            if permalink is None:
                content_link = ContentInBase().generate_permalink(
                    comment.content.permalink, comment.content.id
                )
            else:
                content_link = permalink

            if comment.l1_id:
                (l1_index, l1_comments,) = await crud.comment.format_single_comment(
                    comment,
                    level=1,
                    index=l1_index,
                    permalink=content_link,
                    users=users,
                    comment_actions=comment_actions,
                    comments_out=l1_comments,
                    schema=CommentL1Out,
                )
                l1_index_obj = l1_index[comment.l1_id]

            if comment.l2_id:
                l2_index = l1_index_obj["child_index"]
                l2_comments = l1_comments[l1_index_obj["list_id"]].comments

                (l2_index, l2_comments,) = await crud.comment.format_single_comment(
                    comment,
                    level=2,
                    index=l2_index,
                    permalink=content_link,
                    users=users,
                    comment_actions=comment_actions,
                    comments_out=l2_comments,
                    schema=CommentL2Out,
                )
                l2_index_obj = l2_index[comment.l2_id]

            if comment.l3_id:
                l3_index = l2_index_obj["child_index"]
                l3_comments = l2_comments[l2_index_obj["list_id"]].comments

                await crud.comment.format_single_comment(
                    comment,
                    level=3,
                    index=l3_index,
                    permalink=content_link,
                    users=users,
                    comment_actions=comment_actions,
                    comments_out=l3_comments,
                    schema=CommentL3Out,
                )

        l1_total = comments[0].l1_total if comments else 0
        master_comments_out = CommentListOut(
            comments=l1_comments, comments_total=l1_total
        )
        return master_comments_out

    @staticmethod
    async def get_multi_levels(
        db: AsyncSession,
        *,
        content_id: int = None,
        comment_id: int = None,
        include_cid=False,
        last_id: int = None,
        count: int = 10,
        sub_count: int = 3,
        user: Optional[User] = None,
    ) -> Comment:
        """
        Fetch all comments for content or comment, upto 3 levels.

        Args:
            content_id: id of :any:`Content<models.content.Content>`
            comment_id: id of :any:`Comment<models.comment.Comment>`
            include_cid: setting to ``True``, will include this comment in the result
            last_id: last id of :any:`Comment<models.comment.Comment>`
            count: number of results to return
            sub_count: number of results to return for sub-comments

        Returns:
            Returns a list of comments across levels in a flat structure.
        """

        count = min(MAX_COMM_COUNT, max(1, count))
        sub_count = min(MAX_SUB_COMM_COUNT, max(1, sub_count))

        l1_comments_filters = (
            (Comment.parent_id == comment_id),
            ((Comment.id < last_id) if last_id else true()),
        )
        if include_cid and comment_id:
            l1_comments_filters = (Comment.id == comment_id,)

        l1_comments = (
            select(
                Comment.id.label("l1_id"),
                Comment.content_id.label("l1_content_id"),
                Comment.parent_id.label("l1_parent_id"),
                Comment.body.label("l1_body"),
                Comment.created_by_id.label("l1_created_by_id"),
                Comment.created_at.label("l1_created_at"),
                Comment.updated_at.label("l1_updated_at"),
            )
            .where(
                Comment.deleted_at == EPOCH,
                Comment.content_id == content_id,
                *l1_comments_filters,
            )
            .order_by(Comment.created_at.desc())
            .limit(count)
            .alias("l1")
        )

        l2_comments = (
            select(
                Comment.id.label("l2_id"),
                Comment.parent_id.label("l2_parent_id"),
                Comment.body.label("l2_body"),
                Comment.created_by_id.label("l2_created_by_id"),
                Comment.created_at.label("l2_created_at"),
                Comment.updated_at.label("l2_updated_at"),
            )
            .where(
                Comment.deleted_at == EPOCH, Comment.parent_id == l1_comments.c.l1_id
            )
            .order_by(Comment.created_at.desc())
            .limit(sub_count)
            .lateral("l2")
        )

        l3_comments = (
            select(
                Comment.id.label("l3_id"),
                Comment.parent_id.label("l3_parent_id"),
                Comment.body.label("l3_body"),
                Comment.created_by_id.label("l3_created_by_id"),
                Comment.created_at.label("l3_created_at"),
                Comment.updated_at.label("l3_updated_at"),
            )
            .where(
                Comment.deleted_at == EPOCH, Comment.parent_id == l2_comments.c.l2_id
            )
            .order_by(Comment.created_at.desc())
            .limit(sub_count)
            .lateral("l3")
        )

        l1_comments_total = (
            select(func.count(Comment.id).label("l1_total"))
            .where(
                Comment.deleted_at == EPOCH,
                Comment.content_id == content_id,
                Comment.parent_id == comment_id,
            )
            .lateral("l1_count")
        )

        l2_comments_total = (
            select(func.count(Comment.id).label("l2_total"))
            .where(
                Comment.deleted_at == EPOCH, Comment.parent_id == l1_comments.c.l1_id
            )
            .lateral("l2_count")
        )

        l3_comments_total = (
            select(func.count(Comment.id).label("l3_total"))
            .where(
                Comment.deleted_at == EPOCH, Comment.parent_id == l2_comments.c.l2_id
            )
            .lateral("l3_count")
        )

        query = select(
            l1_comments_total,
            l1_comments,
            l2_comments_total,
            l2_comments,
            l3_comments_total,
            l3_comments,
        ).select_from(
            l1_comments.outerjoin(l2_comments, true())
            .outerjoin(l3_comments, true())
            .outerjoin(l1_comments_total, true())
            .outerjoin(l2_comments_total, true())
            .outerjoin(l3_comments_total, true())
        )

        results = await db.execute(query)
        results = results.all()
        return results


comment = CRUDComment(Comment)
