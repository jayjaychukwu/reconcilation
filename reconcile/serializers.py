from rest_framework import serializers

from .models import ReconcilationRecord
from .validators import validate_csv_file


class CSVFileSerializer(serializers.Serializer):
    source_file = serializers.FileField(validators=[validate_csv_file])
    target_file = serializers.FileField(validators=[validate_csv_file])


class ReconcilationRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReconcilationRecord
        fields = [
            "missing_data_in_source_file",
            "missing_data_in_target_file",
            "discrepancies",
            "status",
            "task_id",
        ]
