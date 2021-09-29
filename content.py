import re
from typing import List, Optional, Union

import base58
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Query

from app import crud
from app.crud.base import CRUDBase
from app.db.mixin import EPOCH
from app.models.action import UserContentAction
from app.models.comment import Comment
from app.models.content import Content, ContentTypeEnum, content_tags_table
from app.models.content_tag import ContentTag
from app.models.user import User
from app.models.user_connection import UserConnection
from app.schemas.content import ContentCreate, ContentInBase, ContentUpdate
from app.schemas.content_tag import ContentTagIn

MAX_COUNT = 100


class NotFoundError(Exception):
    def __init__(self, detail):
        self.detail = detail

    def __str__(self):
        return str(self.detail)


class CRUDContent(CRUDBase[Content, ContentCreate, ContentUpdate]):
    """
    CRUD for :any:`Contents model<models.content.Content>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    @staticmethod
    def generate_link(text, num_chars=100):
        """
        Generates a string from given text by,

        * replacing all spaces with ``-``
        * removing all chars except alphanumerics & ``-``
        * convert all chars to lowercase
        * limiting to first few chars (`num_chars`)

        Args:
            text: input string to generate from
            num_chars: number of chars to limit the generated string to
        """

        text = re.sub(r"\s+", "-", text)
        text = re.sub(r"[^\w\-]", "", text)
        text = re.sub(r"[\-_]+", "-", text)
        return text.lower()[:num_chars].strip("-")

    @staticmethod
    def parse_permalink(permalink_str) -> dict:
        """
        Parse the given permalink to extract db permalink & id of the content.

        Args:
            permalink_str: raw permalink string

        Returns:
             Returns a dict consisting of db permalink, content id & info on input.
        """

        link_parts = permalink_str.rsplit("-", 1)

        if len(link_parts) == 2:
            base58_id = link_parts.pop()
            permalink = "-".join(link_parts)
            try:
                content_id = base58.b58decode_int(base58_id.encode("utf-8"))
            except ValueError:
                content_id = None

            return {
                "raw_permalink": permalink_str,
                "raw_id": base58_id,
                "parsed_permalink": permalink,
                "parsed_id": content_id,
            }

        return None

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: Union[ContentCreate, ContentUpdate],
        content_type: ContentTypeEnum,
        db_obj: Optional[Content] = None,
        user: User,
    ) -> Content:
        """Create a row"""
        obj_data = obj_in.dict()

        if db_obj is not None:
            obj_data["room"] = db_obj.room
        else:
            room_obj = await crud.room.get(db, id=obj_data["room"]["id"])
            if room_obj is None:
                raise NotFoundError(
                    detail="Room id {} not found.".format(obj_data["room"]["id"])
                )
            obj_data["room"] = room_obj

        if obj_data.get("tags") is not None:
            tag_objs = {}
            for tag in obj_data["tags"]:
                if tag["name"] not in tag_objs:
                    tag_objs[tag["name"]] = await crud.contenttag.get_or_create(
                        db, obj_in=ContentTagIn(**tag), user=user
                    )

            obj_data["tags"] = [tag_obj for tag, tag_obj in tag_objs.items()]
        else:
            if db_obj is not None:
                obj_data["tags"] = db_obj.tags
            else:
                obj_data["tags"] = []

        if obj_data.get("images") is not None:
            images_obj = []
            for image in obj_data["images"]:
                image_obj = await crud.image.get(db, id=image["id"])
                if image_obj is None:
                    raise NotFoundError(
                        detail="Image id {} not found.".format(image["id"])
                    )
                images_obj.append(image_obj)
            obj_data["images"] = images_obj
        else:
            if db_obj is not None:
                obj_data["images"] = db_obj.images
            else:
                obj_data["images"] = []

        if "title" in obj_data and obj_data["title"] is not None:
            link_text = obj_data["title"]
        elif "body" in obj_data and obj_data["body"] is not None:
            link_text = obj_data["body"]
        elif "images" in obj_data and obj_data["images"]:
            link_text = obj_data["images"][0].title
            # Should it be like twitter, with permalink for each image?
        else:
            raise NotFoundError(detail="Title/Body not found")

        if obj_data["permalink"] is not None and len(obj_data["permalink"]):
            permalink_obj = ContentInBase(type=content_type).parse_permalink(
                obj_data["permalink"]
            )
            if (
                db_obj is not None
                and permalink_obj is not None
                and permalink_obj["parsed_id"] == db_obj.id
            ):
                obj_data["permalink"] = self.generate_link(
                    permalink_obj["parsed_permalink"]
                )
            else:
                obj_data["permalink"] = self.generate_link(obj_data["permalink"])
        else:
            if db_obj is not None:
                obj_data["permalink"] = db_obj.permalink
            else:
                obj_data["permalink"] = self.generate_link(link_text)

        obj_data["type"] = content_type

        if db_obj is None:
            db_obj = Content(**obj_data)
            db_obj.created_by = user
        else:
            db_data = db_obj.__dict__
            for field in db_data:
                if field in obj_data:
                    setattr(db_obj, field, obj_data[field])
            db_obj.updated_by = user

        await self.commit_refresh(db, db_obj)
        return db_obj

    @staticmethod
    def get_id_from_permalink(permalink) -> Optional[int]:
        """
        Parse the given permalink to extract id of the content.

        Args:
            permalink: raw permalink string

        Returns:
             Returns the content id.
        """

        permalink_obj = ContentInBase().parse_permalink(permalink)
        return permalink_obj["parsed_id"]

    def get_all_q(
        self,
        db: AsyncSession,
        *,
        content_type: Optional[List[ContentTypeEnum]] = None,
        room_id: Optional[List[int]] = None,
        q: Optional[str] = None,
        created_by_id: Optional[str] = None,
        comment_created_by_id: Optional[str] = None,
        connections: Optional[bool] = False,
        format: Optional[List[str]] = None,
        tag: Optional[List[str]] = None,
        saved: Optional[bool] = None,
        since_permalink: Optional[str] = None,
        last_permalink: Optional[str] = None,
        count: Optional[int] = 10,
        user: User,
    ) -> Query:
        """
        Get all contents matching the search criteria & filters with pagination.

        Args:
            room_id: filter based on :any:`Room<models.room.Room>` ids
            q: filter based on search query
            created_by_id: filter based on content posted by
                :any:`User<models.user.User>` firebase_uid
            comment_created_by_id: filter based on
                :any:`Comment<models.comment.Comment>` posted by
                :any:`User<models.user.User>` firebase_uid
            connections: filter based on content posted by users within
                :any:`Connections<models.user_connection.UserConnection>`
            tag: filter based on content posted under
                :any:`Tags<models.content_tag.ContentTag>`
            saved: filter based on contents
                :any:`saved<models.action.UserContentAction>` by User
            since_permalink: for pagination of new posts since this permalink
            last_permalink: for pagination of posts before this permalink
            count: number of results to return

        Returns:
             Returns the content id.
        """

        count = min(MAX_COUNT, max(1, count))
        query = self.query().order_by(Content.created_at.desc())
        if content_type:
            query = query.where(Content.type.in_(content_type))
        if room_id:
            query = query.where(Content.room_id.in_(room_id))
        if q:
            query = query.where(Content.content_tsv.op("@@")(func.plainto_tsquery(q)))
        if created_by_id:
            query = query.join(User, User.id == Content.created_by_id)
            query = query.where(User.firebase_uid == created_by_id)
        if comment_created_by_id:
            query = query.join(Comment, Comment.content_id == Content.id)
            query = query.join(User, User.id == Comment.created_by_id)
            query = query.where(User.firebase_uid == comment_created_by_id)
            query = query.group_by(Content.id)
        if connections:
            uc_query_1 = select(UserConnection.created_by_id).where(
                UserConnection.deleted_at == EPOCH,
                UserConnection.connected == True,  # noqa
                UserConnection.receiver_id == user.id,
            )  # [2,3]
            uc_query_2 = select(UserConnection.receiver_id).where(
                UserConnection.deleted_at == EPOCH,
                UserConnection.connected == True,  # noqa
                UserConnection.created_by_id == user.id,
            )  # [4,5,6,7]
            uc_query = uc_query_1.union(uc_query_2)  # [2,3,4,5,6,7]
            user_active_q = select(User.id).where(
                User.id.in_(uc_query),  # [2,3,4,5,6,7]
                User.deactivated_at == None,  # noqa # [2,3,5,6,7]
                User.deleted_at == EPOCH,
            )
            query = query.where(Content.created_by_id.in_(user_active_q))
        if format:
            query = query.where(Content.format.in_(format))
        if tag:
            query = query.join(
                content_tags_table, content_tags_table.c.content_id == Content.id
            ).join(ContentTag, ContentTag.id == content_tags_table.c.tag_id)
            query = query.where(ContentTag.name.in_(tag))
        if saved:
            # TODO Use UserContentAction to check for currently saved
            query = query.join(
                UserContentAction, UserContentAction.content_id == Content.id
            )
            query = query.where(
                UserContentAction.created_by_id == user.id,
                UserContentAction.saved_at != None,  # noqa
            )
        if since_permalink:
            since_id = self.get_id_from_permalink(since_permalink)
            query = query.where(Content.id > since_id)
        if last_permalink:
            last_id = self.get_id_from_permalink(last_permalink)
            query = query.where(Content.id < last_id)
        if count:
            query = query.limit(count)

        print("-" * 30)
        print(query)
        return query

    async def get_by_permalink(
        self, db: AsyncSession, *, permalink: str
    ) -> Optional[Content]:
        permalink_obj = self.parse_permalink(permalink)
        content_obj = await self.get(db, id=permalink_obj["parsed_id"])
        if content_obj is None:
            return None
        if content_obj.permalink != permalink_obj["parsed_permalink"]:
            return None
        return content_obj

    async def update_user_data(
        self, db: AsyncSession, *, contents: List = []
    ) -> Optional[Content]:
        """
        Add user object on the given list of content objects using created_at_id.

        Args:
            contents: list of content objects

        Returns:
             Returns a list of contents objects with added user object.
        """

        user_objs = {}
        for c in contents:
            user_objs[c.created_by_id] = None
        users_q = select(User).filter(User.id.in_(user_objs.keys()))
        users = await crud.user.get_q(db, query=users_q)
        user_objs = {user.id: user for user in users}

        for c in contents:
            c.created_by = user_objs[c.created_by_id]

        return contents


content = CRUDContent(Content)
