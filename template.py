from typing import Optional

from app.crud.base import CRUDBase
from app.models.template import Section, Template, TemplateSection
from app.schemas.template import (
    SectionCreate,
    SectionIn,
    SectionOut,
    TemplateCreate,
    TemplateIn,
    TemplateOut,
)


class CRUDTemplate(CRUDBase[Template, TemplateCreate, TemplateIn]):
    """
    CRUD for :any:`Template model<models.template.Template>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    def get_template(self, template: Template) -> Optional[TemplateOut]:
        """Generate a TemplateOut object given a template model object"""
        t_out = TemplateOut(
            id=template.id,
            name=template.name,
            type=template.type,
            sections=[],
            image=template.image,
        )
        for ts in template.sections:
            s_out = SectionOut(
                id=ts.section.id,
                name=ts.section.name,
                type=ts.section.type,
                content=ts.section.content,
            )
            t_out.sections.append(s_out)
        return t_out


class CRUDSection(CRUDBase[Section, SectionCreate, SectionIn]):
    """
    CRUD for :any:`Section model<models.template.Section>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


class CRUDTemplateSection(CRUDBase[TemplateSection, TemplateSection, TemplateSection]):
    """
    CRUD for :any:`TemplateSection model<models.template.TemplateSection>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


template = CRUDTemplate(Template)
section = CRUDSection(Section)
template_section = CRUDTemplateSection(TemplateSection)
