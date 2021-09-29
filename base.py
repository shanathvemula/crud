from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Query

from app.db.base_class import Base
from app.db.mixin import EPOCH, SoftDeleteMixin, UserMixin
from app.models.user import User

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)

LIMIT = 100


class MoreThanOneError(Exception):
    pass


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    def __init__(self, model: Type[ModelType]):
        """CRUD Object with default methods to Create, Read, Update and Delete (CRUD).

        **Parameters**
        * `model`: A SQLAlchemy model class
        * `schema`: A Pydantic model (schema) class
        """
        self.model = model
        if issubclass(model, SoftDeleteMixin):
            self.__soft_del = True
        else:
            self.__soft_del = False
        if issubclass(model, UserMixin):
            self.__user_mixin = True
        else:
            self.__user_mixin = False

    def query(self) -> Query:
        """Just a query object for the model."""
        query = select(self.model)
        if self.__soft_del:
            # sqlalchemy overloads == and !=, so can't use is or is not None
            # noinspection PyComparisonWithNone
            query = query.filter(self.model.deleted_at == EPOCH)  # noqa: E711
        return query

    async def get_all(
        self, db: AsyncSession, user: Optional[User] = None
    ) -> List[ModelType]:
        query = self.query()
        if user and self.__user_mixin:
            query = query.filter(self.model.created_by_id == user.id)
        return await self.get_q(db, query)

    async def get_q(self, db: AsyncSession, query: Query) -> List[ModelType]:
        results = await db.execute(query)
        objects = results.scalars().all()
        return objects

    async def get_q_one(self, db: AsyncSession, query: Query) -> List[ModelType]:
        results = await db.execute(query)
        object = results.scalars().first()
        return object

    async def count_q(self, db: AsyncSession, query: Query) -> int:
        q = select(func.count()).select_from(query.subquery())
        result = await db.execute(q)
        count = result.scalars().one()
        return count

    async def get(self, db: AsyncSession, id: Any) -> Optional[ModelType]:
        """Get a single ID based on the primary key.

        Returns None, if the ID doesn't exist.
        """
        stmt = self.query().filter(self.model.id == id)
        result = await db.execute(stmt)
        return result.scalar()

    def get_multi(self, **kw) -> Query:
        """Returns a query which satisfies all the filters"""
        return self.query().filter_by(**kw)

    def get_multi_limit(self, *, skip: int = 0, limit: int = LIMIT, **kw) -> Query:
        """Returns a query which satisfies all filters and limits results.
        :param db: The DB session object
        :param skip: skips this many items for paginating.
        :param limit: return this many items in the list.
        """
        return self.get_multi(**kw).offset(skip).limit(limit)

    def get_multi_since(self, *, since_id: int = 0, limit: int = LIMIT, **kw) -> Query:
        """Returns a query which satisfies all filters and limits results.
        :param db: The DB session object
        :param since_id: The ID from which to fetch results.
        :param limit: return this many items in the list.
        """
        return self.get_multi(**kw).filter(self.model.id > since_id).limit(limit)

    async def get_by_name(self, db: AsyncSession, *, name: str) -> Optional[ModelType]:
        stmt = self.query().filter(self.model.name == name)
        result = await db.execute(stmt)
        return result.scalar()

    async def get_by_names(
        self, db: AsyncSession, *, names: List[str]
    ) -> List[ModelType]:
        stmt = self.query().filter(self.model.name.in_(names))
        result = await self.get_q(db, stmt)
        return result

    async def commit_refresh(self, db: AsyncSession, obj: ModelType) -> ModelType:
        db.add(obj)
        await db.commit()
        await db.refresh(obj)
        return obj

    async def create(
        self, db: AsyncSession, *, obj_in: CreateSchemaType, user: Optional[User] = None
    ) -> ModelType:
        """Create a row"""
        db_obj = self.model(**obj_in.dict())
        if self.__user_mixin:
            db_obj.created_by = user
        await self.commit_refresh(db, db_obj)
        return db_obj

    async def get_or_create(
        self,
        db: AsyncSession,
        *,
        obj_in: Union[CreateSchemaType, Dict[str, Any]],
        user: Optional[User] = None,
    ) -> ModelType:
        if isinstance(obj_in, dict):
            data = obj_in
        else:
            data = obj_in.dict()
        if data.get("id"):
            # use the ID to get the object
            obj = await self.get(db, data.get("id"))
            return obj

        if "id" in data:
            # if id is None, remove it to ensure the filter query works
            data.pop("id")

        q = self.get_multi(**data)
        q_count = await self.count_q(db, q)

        if q_count == 1:
            # Got back exactly 1 result. Return it.
            results = await self.get_q(db, q)
            if results:
                return results[0]
        elif q_count > 1:
            # got ambiguous number of results. throw an error.
            raise MoreThanOneError
        elif q_count == 0:
            # object doesn't exist. create and return
            # obj_in_data = jsonable_encoder(obj_in)
            if isinstance(obj_in, dict):
                db_obj = self.model(**obj_in)
            else:
                db_obj = self.model(**obj_in.dict())
            if self.__user_mixin:
                db_obj.created_by = user
            await self.commit_refresh(db, db_obj)
            return db_obj

    async def update(
        self,
        db: AsyncSession,
        *,
        db_obj: ModelType,
        obj_in: Union[UpdateSchemaType, Dict[str, Any]],
        user: Optional[User] = None,
    ) -> ModelType:
        """update an existing row"""
        obj_data = jsonable_encoder(db_obj)
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.dict(exclude_unset=True)
        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])
        if self.__user_mixin:
            db_obj.updated_by = user

        await self.commit_refresh(db, db_obj)
        return db_obj

    async def delete(self, db: AsyncSession, *, id: int) -> ModelType:
        """Perform a Soft/Hard Delete of a row."""
        db_obj = await self.get(db, id)
        if isinstance(db_obj, SoftDeleteMixin):
            db_obj.delete()  # soft delete
            await self.commit_refresh(db, db_obj)
        else:
            db.delete(db_obj)  # hard delete
            await db.commit()
        return db_obj

    async def purge(self, db: AsyncSession, *, id: int) -> ModelType:
        """Permanently remove a row from the table."""
        obj = await self.get(db, id=id)
        db.delete(obj)
        await db.commit()
        return obj
