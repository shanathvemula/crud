from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.crud.base import CRUDBase
from app.models.project import Project, ProjectLink
from app.models.user import User
from app.schemas.project import ProjectCreate, ProjectLinkCreate, ProjectUpdate


class CRUDProjectLink(CRUDBase[ProjectLink, ProjectLinkCreate, ProjectLinkCreate]):
    """
    CRUD for :any:`ProjectLink model<models.project.ProjectLink>`

    Requires,

    * Model
    * CreateSchema
    """

    # No update for project links, delete and add new
    pass


class CRUDProject(CRUDBase[Project, ProjectCreate, ProjectUpdate]):
    """
    CRUD for :any:`Project model<models.project.Project>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    async def create_or_update(
        self,
        db: AsyncSession,
        *,
        obj_in: ProjectCreate,
        user: User,
        db_obj: Optional[Project] = None,
    ) -> Project:
        """Create a new project or update existing project

        also all supporting project links, skills, companies, etc.
        """
        fields = ["name", "description"]
        if db_obj is None:
            # this isn't an update.
            # create a new project object
            db_obj = Project(name=obj_in.name, description=obj_in.description)
            db_obj.created_by = user
        else:
            update_data = obj_in.dict(exclude_unset=True)
            for field in fields:
                if field in update_data:
                    setattr(db_obj, field, update_data[field])
            db_obj.updated_by = user

        # set the cover image
        if obj_in.cover_image:
            db_obj.cover_image = await crud.image.get(db, id=obj_in.cover_image.id)

        # creates a company if the current company isn't available
        if obj_in.company:
            company = await crud.company.get_or_create(
                db, obj_in=obj_in.company, user=user
            )
            db_obj.company = company

        # then creates a list of skills
        db_obj.skills.clear()
        if obj_in.skills:
            for skill in obj_in.skills:
                skill_obj = await crud.skill.get_or_create(db, obj_in=skill, user=user)
                db_obj.skills.append(skill_obj)

        # then creates the list of projectlinks
        project_links = []
        if obj_in.links:
            for link in obj_in.links:
                link_obj = await crud.projectlink.create(db, obj_in=link)
                project_links.append(link_obj)
        db_obj.links = project_links

        await self.commit_refresh(db, db_obj)
        return db_obj


projectlink = CRUDProjectLink(ProjectLink)
project = CRUDProject(Project)
