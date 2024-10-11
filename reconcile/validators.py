from django.core.files.uploadedfile import UploadedFile
from rest_framework.exceptions import ValidationError


def validate_csv_file(file: UploadedFile) -> None:
    """
    Custom validator to ensure that the uploaded file is a CSV file.
    """
    if not file.name.endswith(".csv"):
        raise ValidationError("The uploaded file must be a CSV file.")

    if file.content_type != "text/csv":
        raise ValidationError("Invalid file type. The file must be of type 'text/csv'.")
