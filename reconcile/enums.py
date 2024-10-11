from django.db import models
from django.utils.translation import gettext_lazy as _


class Status(models.TextChoices):
    PROCESSING = "PROCESSING", _("PROCESSING")
    SUCCESS = "SUCCESS", _("SUCCESS")
    FAILED = "FAILED", _("FAILED")
