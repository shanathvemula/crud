from app.crud.base import CRUDBase
from app.models.image import Image
from app.schemas.image import ImageCreate, ImageIn


class CRUDImage(CRUDBase[Image, ImageCreate, ImageIn]):
    """
    CRUD for :any:`Image model<models.image.Image>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


image = CRUDImage(Image)
