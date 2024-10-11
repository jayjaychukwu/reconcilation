from typing import Any, Dict

from celery import shared_task

from .models import ReconcilationRecord, Status
from .services import ReconcilationService


def generate_result(message: str, status: bool = True, error: str | None = None) -> Dict[str, Any]:
    data = {
        "status": "sucess" if status else "failed",
        "message": message,
    }

    if error is not None:
        data["error"] = error

    return data


@shared_task
def trigger_reconcilation(task_id: str) -> Dict[str, Any]:
    try:
        record = ReconcilationRecord.objects.get(task_id=task_id)
    except ReconcilationRecord.DoesNotExist:
        return generate_result(status=False, message=f"record with task_id, {task_id}, does not exist")

    if record.status != Status.PROCESSING:
        return generate_result(message="this task has already been processed")

    try:
        reconcilation_service = ReconcilationService(source_file=record.source_file, target_file=record.target_file)
        reconcilation_service.reconcile_and_save_data()
    except ValueError as err:
        return generate_result(status=False, message=f"an error occurred", error=str(err))

    return generate_result(message="task processed and saved successfully")
