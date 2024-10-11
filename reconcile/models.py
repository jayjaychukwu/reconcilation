import uuid

from django.db import models

from .enums import Status


class ReconcilationRecord(models.Model):
    source_file = models.FileField(upload_to="csv/source_files/")
    target_file = models.FileField(upload_to="csv/target_files/")
    task_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True, db_index=True)
    status = models.CharField(max_length=100, choices=Status.choices, default=Status.PROCESSING)
    missing_data_in_source_file = models.JSONField(null=True, blank=True)
    missing_data_in_target_file = models.JSONField(null=True, blank=True)
    discrepancies = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
