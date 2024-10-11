from typing import Any, Dict, List

import pandas as pd
from django.core.files.uploadedfile import UploadedFile

from .models import ReconcilationRecord, Status


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
    def update_record(cls, data: Dict[str, Any], record: ReconcilationRecord) -> ReconcilationRecord:
        """
        Update the record with the data

        Args:
            data (Dict[str, Any]): the data to update
            record (ReconcilationRecord): the record to be updated

        Returns:
            ReconcilationRecord: The updated reconcilation record
        """
        for key, value in data.items():
            setattr(record, key, value)

        record.save()
        return record

    def reconcile_and_save_data(self):
        source_df = self.normalize_data(pd.read_csv(self.record.source_file))
        target_df = self.normalize_data(pd.read_csv(self.record.target_file))

        reconciled_data = self.reconcile_data(normalized_source_df=source_df, normalized_target_df=target_df)
        reconciled_data["status"] = Status.SUCCESS

        self.update_record(data=reconciled_data, record=self.record)

    def normalize_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize data to handle case sensitivity, spaces, and date formats.

        Args:
            dataframe (pd.DataFrame): The data frame to normalize.

        Returns:
            pd.DataFrame: The normalized data frame.
        """

        # Strip leading/trailing spaces and convert strings to lowercase
        dataframe = dataframe.map(lambda x: x.strip().lower() if isinstance(x, str) else x)

        # Normalize column names to lowercase for both DataFrames
        dataframe.columns = [col.lower().strip() for col in dataframe.columns]

        # Ensure 'id', 'name', 'date', and 'amount' columns are present
        required_columns = ["id", "name", "date", "amount"]
        for col in required_columns:
            if col not in dataframe.columns:
                raise KeyError(f"Column '{col}' is missing from one of the DataFrames.")

        # Normalize the 'Date' column to a consistent date format, if it exists
        if "date" in dataframe.columns:
            dataframe["date"] = pd.to_datetime(dataframe["date"], errors="coerce")

        return dataframe

    # def reconcile_data(self, normalized_source_df: pd.DataFrame, normalized_target_df: pd.DataFrame) -> Dict[str, Any]:
    #     """
    #     Reconcile the source and target CSVs.

    #     Args:
    #         normalized_source_df (pd.DataFrame): Source data frame.
    #         normalized_target_df (pd.DataFrame): Target data frame.

    #     Returns:
    #         Dict[str, Any]:{
    #             missing_in_target (pd.DataFrame): Records missing in the target data.
    #             missing_in_source (pd.DataFrame): Records missing in the source data.
    #             discrepancies (pd.DataFrame): Records that are present in both but have discrepancies.
    #         }
    #     """

    #     # Find missing records in the target (records present in source but not in target)
    #     missing_in_target = pd.merge(
    #         normalized_source_df, normalized_target_df, how="left", on=["id", "name"], indicator=True
    #     ).query('_merge == "left_only"')

    #     # Find missing records in the source (records present in target but not in source)
    #     missing_in_source = pd.merge(
    #         normalized_target_df, normalized_source_df, how="left", on=["id", "name"], indicator=True
    #     ).query('_merge == "left_only"')

    #     # Rename date_x, amount_x to date, amount in missing_in_target and missing_in_source
    #     missing_in_target = missing_in_target.rename(columns={"date_x": "date", "amount_x": "amount"})
    #     missing_in_source = missing_in_source.rename(columns={"date_x": "date", "amount_x": "amount"})

    #     # Convert 'date' columns in both missing_in_target and missing_in_source to JSON serializable format
    #     missing_in_target["date"] = missing_in_target["date"].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    #     missing_in_source["date"] = missing_in_source["date"].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

    #     # Select only the necessary columns (id, name, date, amount)
    #     missing_in_target = missing_in_target[["id", "name", "date", "amount"]]
    #     missing_in_source = missing_in_source[["id", "name", "date", "amount"]]

    #     # Find common records with discrepancies in specific fields (e.g., Date and Amount)
    #     common_columns = ["id", "name"]
    #     discrepancies = pd.merge(
    #         normalized_source_df, normalized_target_df, on=common_columns, suffixes=("_source", "_target")
    #     )

    #     # Now compare 'date' and 'amount' columns for discrepancies
    #     discrepancies = discrepancies[
    #         (discrepancies["date_source"] != discrepancies["date_target"])
    #         | (discrepancies["amount_source"] != discrepancies["amount_target"])
    #     ][common_columns + ["date_source", "date_target", "amount_source", "amount_target"]]

    #     discrepancies = discrepancies[
    #         (discrepancies["amount_source"] != discrepancies["amount_target"])
    #         | (discrepancies["date_source"] != discrepancies["date_target"])
    #     ]

    #     # Convert 'date' columns in discrepancies to JSON serializable format
    #     discrepancies["date_source"] = discrepancies["date_source"].apply(
    #         lambda x: x.isoformat() if pd.notnull(x) else None
    #     )
    #     discrepancies["date_target"] = discrepancies["date_target"].apply(
    #         lambda x: x.isoformat() if pd.notnull(x) else None
    #     )

    #     # Convert the discrepancies into a format that retains only the required fields for the output
    #     discrepancies = discrepancies.rename(
    #         columns={
    #             "date_source": "date",
    #             "amount_source": "amount",
    #             "date_target": "target_date",
    #             "amount_target": "target_amount",
    #         }
    #     )

    #     return {
    #         "missing_data_in_target_file": missing_in_target.to_dict(orient="records"),
    #         "missing_data_in_source_file": missing_in_source.to_dict(orient="records"),
    #         "discrepancies": discrepancies[
    #             common_columns + ["date", "target_date", "amount", "target_amount"]
    #         ].to_dict(orient="records"),
    #     }

    def reconcile_data(self, normalized_source_df: pd.DataFrame, normalized_target_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Reconcile the source and target CSVs.

        Args:
            normalized_source_df (pd.DataFrame): Source data frame.
            normalized_target_df (pd.DataFrame): Target data frame.

        Returns:
            Dict[str, Any]: Reconciliation results.
        """
        missing_in_target = self.find_missing_in_target(normalized_source_df, normalized_target_df)
        missing_in_source = self.find_missing_in_target(normalized_target_df, normalized_source_df)
        discrepancies = self.find_discrepancies(normalized_source_df, normalized_target_df)

        return {
            "missing_data_in_target_file": missing_in_target,
            "missing_data_in_source_file": missing_in_source,
            "discrepancies": discrepancies,
        }

    def find_missing_in_target(self, target_df: pd.DataFrame, source_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Find records in target that are missing in source.

        Args:
            target_df (pd.DataFrame): Target data frame.
            source_df (pd.DataFrame): Source data frame.

        Returns:
            List[Dict[str, Any]]: Records missing in source.
        """
        missing_in_source = pd.merge(target_df, source_df, how="left", on=["id", "name"], indicator=True).query(
            '_merge == "left_only"'
        )

        # Rename and format date and amount columns
        return self.format_missing_data(missing_in_source)

    def format_missing_data(self, missing_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Format missing data for output.

        Args:
            missing_df (pd.DataFrame): Data frame with missing records.

        Returns:
            List[Dict[str, Any]]: Formatted missing records.
        """
        # Rename and convert dates to JSON serializable format
        missing_df = missing_df.rename(columns={"date_x": "date", "amount_x": "amount"})
        missing_df["date"] = missing_df["date"].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        # Select only the necessary columns
        return missing_df[["id", "name", "date", "amount"]].to_dict(orient="records")

    def find_discrepancies(self, source_df: pd.DataFrame, target_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Find discrepancies between source and target data.

        Args:
            source_df (pd.DataFrame): Source data frame.
            target_df (pd.DataFrame): Target data frame.

        Returns:
            List[Dict[str, Any]]: Records with discrepancies.
        """
        common_columns = ["id"]
        discrepancies = pd.merge(source_df, target_df, on=common_columns, suffixes=("_source", "_target"))

        mask = (
            (discrepancies["name_source"] != discrepancies["name_target"])
            | (discrepancies["date_source"] != discrepancies["date_target"])
            | (discrepancies["amount_source"] != discrepancies["amount_target"])
        )

        # Apply the mask to filter the DataFrame
        filtered_discrepancies = discrepancies[mask].copy()

        # Convert dates and return the formatted discrepancies
        return self.format_discrepancies(filtered_discrepancies)

    def format_discrepancies(self, discrepancies_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Format discrepancies for output.

        Args:
            discrepancies_df (pd.DataFrame): Data frame with discrepancies.

        Returns:
            List[Dict[str, Any]]: Formatted discrepancies.
        """
        discrepancies_df["date_source"] = discrepancies_df["date_source"].apply(
            lambda x: x.isoformat() if pd.notnull(x) else None
        )
        discrepancies_df["date_target"] = discrepancies_df["date_target"].apply(
            lambda x: x.isoformat() if pd.notnull(x) else None
        )

        # Rename columns for clarity
        discrepancies_df = discrepancies_df.rename(
            columns={
                "date_source": "date",
                "amount_source": "amount",
                "date_target": "target_date",
                "amount_target": "target_amount",
                "name_source": "name",
                "name_target": "target_name",
            }
        )

        # Return only the records with discrepancies
        discrepancies = discrepancies_df[
            ["id", "name", "target_name", "date", "target_date", "amount", "target_amount"]
        ].to_dict(orient="records")

        keys = ["name", "date", "amount"]
        for discrepancy in discrepancies:
            for key in keys:
                target_key = f"target_{key}"
                if discrepancy[key] == discrepancy[target_key]:
                    del discrepancy[key]
                    del discrepancy[target_key]

        return discrepancies
