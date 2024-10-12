from rest_framework import status
from rest_framework.generics import GenericAPIView
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response

from .enums import Status
from .serializers import CSVFileSerializer, ReconcilationRecordSerializer
from .services import OutputFormattingService, ReconcilationService
from .tasks import trigger_reconcilation


class CSVUploadAPIView(GenericAPIView):
    serializer_class = CSVFileSerializer
    parser_classes = (MultiPartParser,)

    def post(self, request, *args, **kwargs):
        """
        Upload two CSV files (source and target) and obtain a unique ID.
        """
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid():
            return Response(data=serializer.errors, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

        source_file = request.FILES["source_file"]
        target_file = request.FILES["target_file"]

        try:
            record = ReconcilationService.create_record(
                source_file=source_file,
                target_file=target_file,
            )
            trigger_reconcilation.delay(record.task_id)
        except ValueError as err:
            return Response(
                data={
                    "error": str(err),
                    "message": "an error occurred",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            {
                "id": record.task_id,
                "message": "please use this ID to get your reconcilation result",
            },
            status=status.HTTP_202_ACCEPTED,
        )


class ReconcilationAPIView(GenericAPIView):
    serializer_class = ReconcilationRecordSerializer

    def get(self, request, task_id: str):
        """
        Get the result of a task process
        """

        record = ReconcilationService.get_reconcilation_result(task_id=task_id)

        if record.status == Status.PROCESSING:
            message = "This task is still processing"
        elif record.status == Status.SUCCESS:
            message = "This task was successful"
        else:
            message = "There was an issue processing this task, please reupload"

        record_serializer = self.serializer_class(record)
        data = {
            "message": message,
            "data": record_serializer.data,
        }

        return Response(
            data,
            status=status.HTTP_200_OK,
        )


class ReconcilationReportAPIView(GenericAPIView):

    def get(self, request, task_id: str, file_format: str):
        try:
            response = OutputFormattingService(
                file_format=file_format,
                task_id=task_id,
            ).generate_file_format_response()
        except ValueError as err:
            return Response(data={"message": str(err)}, status=status.HTTP_400_BAD_REQUEST)

        return response
