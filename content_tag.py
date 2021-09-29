from app.crud.base import CRUDBase
from app.models.content_tag import ContentTag
from app.schemas.content_tag import ContentTagCreate, ContentTagIn


class CRUDContentTag(CRUDBase[ContentTag, ContentTagCreate, ContentTagIn]):
    """
    CRUD for :any:`ContentTag model<models.content_tag.ContentTag>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


contenttag = CRUDContentTag(ContentTag)
