"""
Create Read Update Delete methods

The modules and classes defined in this package form the core
data manipulation logic of the application.

They interact with the models layer and do the right operations like inserts, updates,
get a row, filter for rows, deletes, etc.

All CRUD classes inherit from the :any:`CRUDBase` class, which take three type parameters
Model class, Create Schema and Update Schema.

Based on this base class, all CRUD classes get a list of inherited methods that
allow us to do basic querying & filtering, insertion, updates and deletes without code repetition.
"""

from .action import comment_action, content_action
from .certificate import certificate
from .comment import comment
from .company import company
from .content import content
from .content_tag import contenttag
from .cv import cv_upload
from .degree import degree
from .download import download_file, download_request
from .image import image
from .interest import interest
from .invitation import invitation
from .jobtitle import jobtitle
from .language import language
from .location import location
from .notification import notification
from .organization import organization
from .profile import profile
from .project import project, projectlink
from .role_permission import permission, role
from .room import room
from .room_category import roomcategory
from .school import school
from .skill import skill
from .specialization import specialization
from .template import section, template, template_section
from .user import user, user_link, user_room
from .user_certificate import usercertificate
from .user_connection import userconnection
from .user_coverletter import user_coverletter
from .user_cv import user_cv
from .user_education import education
from .user_experience import experience
from .user_interest import userinterest
from .user_language import userlanguage
from .user_skill import userskill
