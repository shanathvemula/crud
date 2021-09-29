from app.crud.base import CRUDBase
from app.models.role_permission import Permission, Role
from app.schemas.role_permission import (
    PermissionCreate,
    PermissionUpdate,
    RoleCreate,
    RoleUpdate,
)


class CRUDRole(CRUDBase[Role, RoleCreate, RoleUpdate]):
    """
    CRUD for :any:`Role model<models.role_permission.Role>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


role = CRUDRole(Role)


class CRUDPermission(CRUDBase[Permission, PermissionCreate, PermissionUpdate]):
    """
    CRUD for :any:`Permission model<models.role_permission.Permission>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


permission = CRUDPermission(Permission)
