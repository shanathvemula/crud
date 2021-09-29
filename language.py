from app.crud.base import CRUDBase
from app.models.language import Language
from app.schemas.language import LanguageCreate, LanguageIn


class CRUDLanguage(CRUDBase[Language, LanguageCreate, LanguageIn]):
    """
    CRUD for :any:`Language model<models.language.Language>`

    Requires,

    * Model
    * CreateSchema
    * UpdateSchema
    """

    pass


language = CRUDLanguage(Language)
