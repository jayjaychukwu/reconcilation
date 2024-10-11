from typing import Any, Dict

import pandas as pd
from django.core.files.uploadedfile import UploadedFile

from .models import ReconcilationRecord
from .tasks import trigger_reconcilation


class ReconcilationService:
    def __init__(self, record: ReconcilationRecord):
        self.record = record

    @classmethod
    def get_reconcilation_result(cls, task_id: str) -> ReconcilationRecord:
        try:
            record = ReconcilationRecord.objects.get(task_id=task_id)
        except ReconcilationRecord.DoesNotExist:
            raise ValueError("this task ID does not exist")

        return record

    @classmethod
    def create_record(cls, source_file: UploadedFile, target_file: UploadedFile) -> ReconcilationRecord:
        reconcilation_record = ReconcilationRecord.objects.create(
            source_file=source_file,
            target_file=target_file,
        )

        return reconcilation_record

    @classmethod
    def create_record_trigger_task(cls, source_file: UploadedFile, target_file: UploadedFile) -> ReconcilationRecord:
        record = cls.create_record(source_file=source_file, target_file=target_file)
        trigger_reconcilation.delay(record.task_id)
        return record

    def reconcile_and_save_data(self):
        source_df = self.normalize_data(pd.read_csv(self.record.source_file))
        target_df = self.normalize_data(pd.read_csv(self.record.target_file))

        reconciled_data = self.reconcile_data(normalized_source_df=source_df, normalized_target_df=target_df)

    def normalize_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize data to handle case sensitivity, spaces, and date formats.

        Args:
            dataframe (pd.DataFrame): The data frame to normalize.

        Returns:
            pd.DataFrame: The normalized data frame.
        """

        # Strip leading/trailing spaces and convert strings to lowercase
        dataframe = dataframe.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)

        # Normalize the 'Date' column to a consistent date format, if it exists
        if "date" in dataframe.columns:
            dataframe["date"] = pd.to_datetime(dataframe["date"], errors="coerce")

        return dataframe

    def reconcile_data(self, normalized_source_df: pd.DataFrame, normalized_target_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Reconcile the source and target CSVs.

        Args:
            normalized_source_df (pd.DataFrame): Source data frame.
            normalized_target_df (pd.DataFrame): Target data frame.

        Returns:
            Dict[str, Any]:{
                missing_in_target (pd.DataFrame): Records missing in the target data.
                missing_in_source (pd.DataFrame): Records missing in the source data.
                discrepancies (pd.DataFrame): Records that are present in both but have discrepancies.
            }
        """

        # Find missing records in the target (records present in source but not in target)
        missing_in_target = pd.merge(
            normalized_source_df, normalized_target_df, how="left", on=["id", "name"], indicator=True
        ).query('_merge == "left_only"')

        # Find missing records in the source (records present in target but not in source)
        missing_in_source = pd.merge(
            normalized_target_df, normalized_source_df, how="left", on=["id", "name"], indicator=True
        ).query('_merge == "left_only"')

        # Find common records with discrepancies in specific fields (e.g., Date and Amount)
        # Merge on 'ID' and 'Name', compare 'Date' and 'Amount' columns
        common_columns = ["id", "name"]
        discrepancies = pd.merge(
            normalized_source_df, normalized_target_df, on=common_columns, suffixes=("_source", "_target")
        )

        # Now compare 'date' and 'amount' columns for discrepancies
        discrepancies = discrepancies[
            (discrepancies["date_source"] != discrepancies["date_target"])
            | (discrepancies["amount_source"] != discrepancies["amount_target"])
        ][common_columns + ["date_source", "date_target", "amount_source", "amount_target"]]

        return {
            "missing_in_target": missing_in_target.to_dict(orient="records"),
            "missing_in_source": missing_in_source.to_dict(orient="records"),
            "discrepancies": discrepancies.to_dict(orient="records"),
        }
