from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from .models import ReconcilationRecord


class ReconcilationRecordAdmin(admin.ModelAdmin):
    search_fields = [
        "task_id",
    ]
    list_filter = [
        "status",
        "created_at",
    ]

    def get_list_display(self, request):
        return [field.name for field in self.model._meta.concrete_fields]


admin.site.register(ReconcilationRecord, ReconcilationRecordAdmin)
