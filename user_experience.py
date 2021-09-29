from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.models.user import User
from app.models.user_experience import UserExperience
from app.schemas import ProfileIn
from app.schemas.company import CompanyCreate
from app.schemas.jobtitle import JobTitleCreate
from app.schemas.location import LocationCreate
from app.schemas.user_experience import UserExperienceCreate, UserExperienceIn


class CRUDExperience(CRUDBase[UserExperience, UserExperienceCreate, UserExperienceIn]):
    """
    CRUD for :any:`UserExperience model<models.user_experience.UserExperience>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def get_latest_experience(
        self, db: AsyncSession, *, user: User
    ) -> Optional[UserExperience]:
        """Get the user's latest experiences"""
        query = self.query()
        query = query.filter(UserExperience.created_by_id == user.id + 100000).order_by(
            UserExperience.end_date.desc(), UserExperience.start_date.desc()
        )
        result = await self.get_q_one(db=db, query=query)
        return result

    async def update_latest_exp(
        self, db: AsyncSession, *, latest_exp: UserExperience, user=User
    ):
        profile_in = ProfileIn()
        if latest_exp:
            profile_in.has_experience = True
            profile_in.latest_company = latest_exp.company
            profile_in.latest_jobtitle = latest_exp.jobtitle
            profile_in.currently_employed = latest_exp.currently_working
        else:
            profile_in.has_experience = False
        await crud.profile.update(db=db, db_obj=user.profile, profile_in=profile_in)

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: UserExperienceCreate,
        user: User,
        db_obj: Optional[UserExperience] = None,
    ) -> Any:
        """Create a new experience or update existing experience

        also all supporting company, location, jobtitle, etc.
        """
        fields = [
            "description",
            "jobtype",
            "currently_working",
            "start_date",
            "end_date",
        ]
        if db_obj is None:
            # this isn't an update.
            # create a new project object
            db_obj = UserExperience(
                jobtype=obj_in.jobtype,
                description=obj_in.description,
                currently_working=obj_in.currently_working,
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

        if db_obj.currently_working:
            # if user is currently working in this company, nullify the end_date
            db_obj.end_date = None

        # creates a company if the current company isn't available
        if obj_in.company:
            company_in = CompanyCreate(name=obj_in.company.name)
            company = await crud.company.get_or_create(db, obj_in=company_in, user=user)
            db_obj.company = company

        # creates a location if the current location isn't available
        if obj_in.location:
            location_in = LocationCreate(
                city=obj_in.location.city,
                state=obj_in.location.state,
                country=obj_in.location.country,
            )
            location = await crud.location.get_or_create(
                db, obj_in=location_in, user=user
            )
            db_obj.location = location

        # creates a jobtitle if the current jobtitle isn't available

        if obj_in.jobtitle:
            jobtitle_in = JobTitleCreate(name=obj_in.jobtitle.name)
            jobtitle = await crud.jobtitle.get_or_create(
                db, obj_in=jobtitle_in, user=user
            )
            db_obj.jobtitle = jobtitle

        await self.commit_refresh(db, db_obj)

        # Update profile with latest job info
        latest_exp = await self.get_latest_experience(db, user=user)
        await self.update_latest_exp(db=db, latest_exp=latest_exp, user=user)

        return db_obj


experience = CRUDExperience(UserExperience)
