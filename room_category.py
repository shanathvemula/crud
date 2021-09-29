from app.crud.base import CRUDBase
from app.models.room_category import RoomCategory
from app.schemas.room_category import RoomCategoryCreate, RoomCategoryIn


class CRUDRoomCategory(CRUDBase[RoomCategory, RoomCategoryCreate, RoomCategoryIn]):
    """
    CRUD for :any:`RoomCategory model<models.room_category.RoomCategory>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


roomcategory = CRUDRoomCategory(RoomCategory)
