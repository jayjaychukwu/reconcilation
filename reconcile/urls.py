from django.urls import path

from .views import CSVUploadAPIView, ReconcilationAPIView, ReconcilationReportAPIView

urlpatterns = [
    path("", CSVUploadAPIView.as_view(), name="file_upload"),
    path("<str:task_id>/", ReconcilationAPIView.as_view(), name="csv_reconcilation"),
    path(
        "report/<str:task_id>/<str:file_format>/",
        ReconcilationReportAPIView.as_view(),
        name="reconciliation_report",
    ),
]
