import pandas as pd
import numpy as np
import requests
from datetime import datetime

# -----------------------------
# GOOGLE SHEET LINKS
# -----------------------------

participant_list_id = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"
resignation_sheet_id = "18oOQZaVBgZDQSFLKz5JtpExsjDMydJPHG8LN1gfBsU4"

participant_url = f"https://docs.google.com/spreadsheets/d/{participant_list_id}/export?format=csv"
resignation_url = f"https://docs.google.com/spreadsheets/d/{resignation_sheet_id}/export?format=csv"

print("Downloading datasets...")

participant_df = pd.read_csv(participant_url)
resignation_df = pd.read_csv(resignation_url)

print("Datasets downloaded")


# -----------------------------
# STANDARDIZE ID COLUMN
# -----------------------------

participant_df["ID Number"] = (
    participant_df["ID number/Non SA Passport"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.strip()
)

resignation_df["ID Number"] = (
    resignation_df["Participant ID Number"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.strip()
)


# -----------------------------
# DATE FORMATTING
# -----------------------------

participant_df["Participant Start Date"] = pd.to_datetime(
    participant_df["Participant Start Date"], errors="coerce"
)

participant_df["Participant End Date"] = pd.to_datetime(
    participant_df["Participant End Date"], errors="coerce"
)

resignation_df["Resignation Date"] = pd.to_datetime(
    resignation_df["Resignation Date\n\n(actual IP date)"], errors="coerce"
)


# -----------------------------
# MERGE DATA
# -----------------------------

merged = participant_df.merge(
    resignation_df,
    on="ID Number",
    how="left"
)

print("Datasets merged")


# -----------------------------
# CALCULATE ACTUAL STAY MONTHS
# -----------------------------

today = pd.Timestamp.today()


def calculate_stay(row):

    start = row["Participant Start Date"]
    end = row["Participant End Date"]
    resign = row["Resignation Date"]

    if pd.notnull(resign):
        final_end = resign
    elif pd.notnull(end):
        final_end = end
    else:
        final_end = today

    if pd.isnull(start):
        return np.nan

    if final_end < start:
        final_end = start

    return (final_end.year - start.year) * 12 + (final_end.month - start.month)


merged["Actual Stay(Months)"] = merged.apply(calculate_stay, axis=1)


# -----------------------------
# ATTRITION RATE CALCULATION
# -----------------------------

total_participants = len(resignation_df)

resigned = resignation_df[
    resignation_df["Active Status"].str.lower() == "resigned"
]

attrition_rate = (len(resigned) / total_participants) * 100 if total_participants else 0

merged["Attrition Rate"] = round(attrition_rate, 2)


# -----------------------------
# RENAME FIELDS
# -----------------------------

merged.rename(
    columns={
        "Organization's Name": "Organisation's name",
        "Status of UI - 19\n\n(TLT Admin Field)": "Status of UI-19(TLT Admin Field)",
        "Reason for Resignation": "Reason for resignation",
        "Participant Age-Group\n\n[Automated Field- No Input Required]": "Participant Age Group",
        "Gender": "Gender",
        "Exit survey shared with participant?": "Participant completed the exit survey?"
    },
    inplace=True
)


# -----------------------------
# SELECT REQUIRED COLUMNS
# -----------------------------

final_columns = [
    "Gender",
    "ID Number",
    "Attrition Rate",
    "Status of UI-19(TLT Admin Field)",
    "Participant completed the exit survey?",
    "Reason for resignation",
    "Participant Age Group",
    "Resignation Date",
    "Actual Stay(Months)",
    "Organisation's name"
]

final_df = merged[final_columns]


# -----------------------------
# EXPORT CSV
# -----------------------------

output_file = "processed_attrition_dataset.csv"

final_df.to_csv(output_file, index=False)

print("CSV created successfully")
