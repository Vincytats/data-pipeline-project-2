import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)

# -----------------------------
# GOOGLE SHEET IDS
# -----------------------------

participant_sheet = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"
resignation_sheet = "18oOQZaVBgZDQSFLKz5JtpExsjDMydJPHG8LN1gfBsU4"

participant_url = f"https://docs.google.com/spreadsheets/d/{participant_sheet}/export?format=csv"
resignation_url = f"https://docs.google.com/spreadsheets/d/{resignation_sheet}/export?format=csv"


# -----------------------------
# DOWNLOAD DATA
# -----------------------------

logging.info("Downloading datasets")

participant_df = pd.read_csv(participant_url)
resignation_df = pd.read_csv(resignation_url)

logging.info("Datasets downloaded")


# -----------------------------
# NORMALIZE COLUMN NAMES
# -----------------------------

def normalize_columns(df):

    df.columns = (
        df.columns
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )

    return df


participant_df = normalize_columns(participant_df)
resignation_df = normalize_columns(resignation_df)

logging.info("Columns normalized")


# -----------------------------
# COLUMN DETECTION FUNCTION
# -----------------------------

def find_column(df, keywords):

    for col in df.columns:

        name = col.lower()

        if all(word in name for word in keywords):
            return col

    raise ValueError(f"Column containing {keywords} not found")


# -----------------------------
# DETECT IMPORTANT COLUMNS
# -----------------------------

participant_id_col = find_column(participant_df, ["id"])
resignation_id_col = find_column(resignation_df, ["id"])

start_date_col = find_column(participant_df, ["start", "date"])
end_date_col = find_column(participant_df, ["end", "date"])

resignation_date_col = find_column(resignation_df, ["resignation", "date"])

logging.info("Important columns detected")


# -----------------------------
# CLEAN ID NUMBERS
# -----------------------------

def clean_id(series):

    return (
        series.astype(str)
        .str.replace(".0", "", regex=False)
        .str.replace(" ", "")
        .str.strip()
    )


participant_df["ID Number"] = clean_id(participant_df[participant_id_col])
resignation_df["ID Number"] = clean_id(resignation_df[resignation_id_col])


# -----------------------------
# DUPLICATE ID DETECTION
# -----------------------------

duplicates = participant_df[participant_df["ID Number"].duplicated()]

if len(duplicates) > 0:

    logging.warning(f"Duplicate IDs detected: {len(duplicates)}")

    participant_df = participant_df.drop_duplicates(subset=["ID Number"])

else:

    logging.info("No duplicate IDs detected")


# -----------------------------
# DATE CONVERSION
# -----------------------------

participant_df["Participant Start Date"] = pd.to_datetime(
    participant_df[start_date_col], errors="coerce"
)

participant_df["Participant End Date"] = pd.to_datetime(
    participant_df[end_date_col], errors="coerce"
)

resignation_df["Resignation Date"] = pd.to_datetime(
    resignation_df[resignation_date_col], errors="coerce"
)


# -----------------------------
# MERGE DATA
# -----------------------------

merged = participant_df.merge(
    resignation_df,
    on="ID Number",
    how="left",
    suffixes=("", "_resignation")
)

logging.info("Datasets merged")


# -----------------------------
# ACTUAL STAY MONTHS
# -----------------------------

today = pd.Timestamp.today()

end_date = merged["Resignation Date"].fillna(
    merged["Participant End Date"]
).fillna(today)

start_date = merged["Participant Start Date"]

end_date = np.where(end_date < start_date, start_date, end_date)

merged["Actual Stay(Months)"] = (
    (pd.to_datetime(end_date).dt.year - start_date.dt.year) * 12 +
    (pd.to_datetime(end_date).dt.month - start_date.dt.month)
)


# -----------------------------
# ATTRITION RATE
# -----------------------------

status_col = find_column(resignation_df, ["status"])

total = len(resignation_df)

resigned = resignation_df[
    resignation_df[status_col].astype(str).str.lower() == "resigned"
]

attrition_rate = (len(resigned) / total) * 100 if total else 0

merged["Attrition Rate"] = round(attrition_rate, 2)


# -----------------------------
# DETECT REMAINING COLUMNS
# -----------------------------

gender_col = find_column(merged, ["gender"])
age_col = find_column(merged, ["age"])
reason_col = find_column(merged, ["reason"])
survey_col = find_column(merged, ["survey"])
uif_col = find_column(merged, ["ui"])

org_col = None

for col in merged.columns:

    if "organisation" in col.lower() or "organization" in col.lower():

        org_col = col

        break

if org_col is None:

    raise ValueError("Organisation column not found")


# -----------------------------
# FINAL DATASET
# -----------------------------

final_df = pd.DataFrame({

    "Gender": merged[gender_col],
    "ID Number": merged["ID Number"],
    "Attrition Rate": merged["Attrition Rate"],
    "Status of UI-19(TLT Admin Field)": merged[uif_col],
    "Participant completed the exit survey?": merged[survey_col],
    "Reason for resignation": merged[reason_col],
    "Participant Age Group": merged[age_col],
    "Resignation Date": merged["Resignation Date"],
    "Actual Stay(Months)": merged["Actual Stay(Months)"],
    "Organisation's name": merged[org_col]

})


# -----------------------------
# EXPORT CSV
# -----------------------------

output_file = "processed_attrition_dataset.csv"

final_df.to_csv(output_file, index=False)

logging.info("CSV created successfully")
