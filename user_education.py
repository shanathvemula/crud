from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.models.user import User
from app.models.user_education import UserEducation
from app.schemas.degree import DegreeID
from app.schemas.school import SchoolCreate, SchoolID
from app.schemas.specialization import SpecializationCreate, SpecializationID
from app.schemas.user_education import UserEducationCreate, UserEducationUpdate


class CRUDUserEducation(
    CRUDBase[UserEducation, UserEducationCreate, UserEducationUpdate]
):
    """
    CRUD for :any:`UserEducation model<models.user_education.UserEducation>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: UserEducationUpdate,
        user: User,
        db_obj: Optional[UserEducation] = None,
    ) -> UserEducation:
        """Create a new education or update existing education

        also all create related degree, specialization, school, etc.
        """
        fields = [
            "grade",
            "description",
            "currently_studying",
            "start_date",
            "end_date",
        ]
        if db_obj is None:
            # this isn't an update.
            # create a new project object
            db_obj = UserEducation(
                grade=obj_in.grade,
                description=obj_in.description,
                currently_studying=obj_in.currently_studying,
                start_date=obj_in.start_date,
                end_date=obj_in.end_date,
            )
            db_obj.created_by = user
        else:
            update_data = obj_in.dict(exclude_unset=True)
            for field in fields:
                if field in update_data:
                    setattr(db_obj, field, update_data[field])
            db_obj.updated_by = user

        print("obj_in.degree >>>>>>>>>>>>>>>>>>>", obj_in.degree)
        if obj_in.degree:
            if isinstance(obj_in.degree, DegreeID):
                # Get the degree from the ID
                degree = await crud.degree.get(db, id=obj_in.degree.id)
            else:
                degree = await crud.degree.get_or_create(
                    db, obj_in=obj_in.degree, user=user
                )
            if degree is not None:
                db_obj.degree = degree

        if obj_in.specialization:
            if isinstance(obj_in.specialization, SpecializationID):
                # get specialization from ID
                specialization = await crud.specialization.get(
                    db, id=obj_in.specialization.id
                )
            else:
                specialization_in = SpecializationCreate(
                    name=obj_in.specialization.name
                )
                specialization = await crud.specialization.get_or_create(
                    db, obj_in=specialization_in, user=user
                )
            print("-----====>", specialization)
            if specialization is not None:
                db_obj.specialization = specialization

        if obj_in.school:
            if isinstance(obj_in.school, SchoolID):
                # get the school from the ID
                school = await crud.school.get(db, id=obj_in.school.id)
            else:
                school_in = SchoolCreate(name=obj_in.school.name)
                school = await crud.school.get_or_create(
                    db, obj_in=school_in, user=user
                )
            if school is not None:
                db_obj.school = school

        await self.commit_refresh(db, db_obj)
        print("db_obj --->", db_obj)
        print("db_obj id --->", db_obj.id)

        return db_obj


education = CRUDUserEducation(UserEducation)
