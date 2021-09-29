from typing import Optional

from sqlalchemy.orm import Query, Session

from app.crud.base import CRUDBase
from app.models.location import Location
from app.schemas.location import LocationCreate, LocationUpdate


class CRUDLocation(CRUDBase[Location, LocationCreate, LocationUpdate]):
    """
    CRUD for :any:`Location model<models.location.Location>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    def get_by_location(
        self,
        db: Session,
        *,
        city: Optional[str] = None,
        state: Optional[str] = None,
        country: Optional[str] = None,
    ) -> Query:
        q = self.query()
        if city:
            q = q.filter(Location.city == city)
        if state:
            q = q.filter(Location.state == state)
        if country:
            q = q.filter(Location.country == country)
        return q


location = CRUDLocation(Location)
