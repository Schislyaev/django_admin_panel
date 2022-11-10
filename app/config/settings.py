from pathlib import Path
import os
from os.path import join, abspath

from dotenv import load_dotenv
from split_settings.tools import include

load_dotenv()


# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get('SECRET_KEY')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get('DEBUG', False) == 'True'

ALLOWED_HOSTS = ['127.0.0.1']


# Application definition

include(
    'components/applications.py',
)


# Database
# https://docs.djangoproject.com/en/3.2/ref/settings/#databases

include(
    'components/database.py',
)


# Password validation
# https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators

include(
    'components/passwords_validation.py',
)


# Internationalization
# https://docs.djangoproject.com/en/3.2/topics/i18n/

include(
    'components/internationalization.py',
)

# Logging

# include(
#     'components/logging.py',
# )


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.2/howto/static-files/

STATIC_ROOT = os.path.join(BASE_DIR, 'static')      # root("static","static")
STATIC_URL = "/static/"

STATICFILES_FINDERS = [
    "django.contrib.staticfiles.finders.FileSystemFinder",
    "django.contrib.staticfiles.finders.AppDirectoriesFinder",
]

# Default primary key field type
# https://docs.djangoproject.com/en/3.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'