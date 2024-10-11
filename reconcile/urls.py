from django.urls import path

from .views import CSVUploadAPIView, ReconcilationAPIView

urlpatterns = [
    path("", CSVUploadAPIView.as_view(), name="file_upload"),
    path("<str:task_id>/", ReconcilationAPIView.as_view(), name="csv_reconcilation"),
]
