import pandas as pd
import numpy as np
import logging
import requests
import os

logging.basicConfig(level=logging.INFO)

print("PIPELINE VERSION FINAL RUNNING")

# ==============================
# GOOGLE SHEETS
# ==============================

participant_sheet = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"
resignation_sheet = "18oOQZaVBgZDQSFLKz5JtpExsjDMydJPHG8LN1gfBsU4"

participant_url = f"https://docs.google.com/spreadsheets/d/{participant_sheet}/export?format=csv"
resignation_url = f"https://docs.google.com/spreadsheets/d/{resignation_sheet}/export?format=csv"

OUTPUT_FILE = "Resignation Profiling Dataset.csv"

# ==============================
# DOWNLOAD DATA
# ==============================

logging.info("Downloading datasets")

participant_df = pd.read_csv(participant_url)
resignation_df = pd.read_csv(resignation_url)

logging.info("Datasets downloaded")

# ==============================
# NORMALIZE COLUMNS
# ==============================

def normalize_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )
    return df

participant_df = normalize_columns(participant_df)
resignation_df = normalize_columns(resignation_df)

print("PARTICIPANT COLUMNS:", list(participant_df.columns))
print("RESIGNATION COLUMNS:", list(resignation_df.columns))

# ==============================
# FIND COLUMN FUNCTION
# ==============================

def find_column(df, keywords):
    for col in df.columns:
        c = col.lower()
        if all(word in c for word in keywords):
            return col
    raise ValueError(f"Column containing {keywords} not found in {list(df.columns)}")

# ==============================
# DETECT IMPORTANT COLUMNS
# ==============================

participant_id_col = find_column(participant_df, ["id"])
resignation_id_col = find_column(resignation_df, ["id"])

start_date_col = find_column(participant_df, ["start", "date"])
end_date_col = find_column(participant_df, ["end", "date"])

resignation_date_col = find_column(resignation_df, ["resignation", "date"])
status_col = find_column(resignation_df, ["status"])

# ==============================
# CLEAN IDS
# ==============================

def clean_id(series):
    return (
        series.astype(str)
        .str.replace(".0", "", regex=False)
        .str.replace(" ", "")
        .str.strip()
    )

participant_df["ID Number"] = clean_id(participant_df[participant_id_col])
resignation_df["ID Number"] = clean_id(resignation_df[resignation_id_col])

participant_df = participant_df.drop_duplicates(subset=["ID Number"])

# ==============================
# DATE CONVERSION
# ==============================

participant_df["Participant Start Date"] = pd.to_datetime(
    participant_df[start_date_col], errors="coerce"
)

participant_df["Participant End Date"] = pd.to_datetime(
    participant_df[end_date_col], errors="coerce"
)

resignation_df["Resignation Date"] = pd.to_datetime(
    resignation_df[resignation_date_col], errors="coerce"
)

# ==============================
# KEEP ONLY NEEDED RESIGNATION COLUMNS
# ==============================

reason_col = None
survey_col = None

for col in resignation_df.columns:
    c = col.lower()
    if "reason" in c and reason_col is None:
        reason_col = col
    if "survey" in c and survey_col is None:
        survey_col = col

keep_cols = ["ID Number", "Resignation Date", status_col]

if reason_col:
    keep_cols.append(reason_col)

if survey_col:
    keep_cols.append(survey_col)

resignation_df = resignation_df[keep_cols]

# ==============================
# MERGE
# ==============================

merged = participant_df.merge(
    resignation_df,
    on="ID Number",
    how="left"
)

print("MERGED COLUMNS:", list(merged.columns))

# ==============================
# STAY MONTHS
# ==============================

today = pd.Timestamp.today()

start_date = pd.to_datetime(
    merged["Participant Start Date"],
    errors="coerce"
)

end_date = merged["Resignation Date"].fillna(
    merged["Participant End Date"]
).fillna(today)

end_date = pd.to_datetime(end_date, errors="coerce")

end_date = pd.Series(
    np.where(end_date < start_date, start_date, end_date),
    index=merged.index
)

merged["Actual Stay(Months)"] = (
    (end_date.dt.year - start_date.dt.year) * 12 +
    (end_date.dt.month - start_date.dt.month)
)

# ==============================
# ATTRITION RATE
# ==============================

total = len(resignation_df)

resigned = resignation_df[
    resignation_df[status_col].astype(str).str.lower() == "resigned"
]

attrition_rate = (len(resigned) / total) * 100 if total else 0

merged["Attrition Rate"] = round(attrition_rate, 2)

# ==============================
# DETECT OTHER COLUMNS
# ==============================

gender_col = find_column(merged, ["gender"])
age_col = find_column(merged, ["age"])
uif_col = find_column(merged, ["ui"])

org_col = None
for col in merged.columns:
    c = col.lower()
    if "organisation" in c or "organization" in c:
        org_col = col
        break

if not reason_col:
    merged["Reason for resignation"] = None
    reason_col = "Reason for resignation"

if not survey_col:
    merged["Participant completed the exit survey?"] = None
    survey_col = "Participant completed the exit survey?"

# ==============================
# FINAL DATASET
# ==============================

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
    "Organisation's name": merged[org_col] if org_col else None
})

final_df.to_csv(OUTPUT_FILE, index=False)

logging.info("CSV created successfully")

# ==============================
# GET ACCESS TOKEN
# ==============================

def get_access_token():
    url = f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}/oauth2/v2.0/token"

    data = {
        "client_id": os.environ["AZURE_CLIENT_ID"],
        "client_secret": os.environ["AZURE_CLIENT_SECRET"],
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default"
    }

    r = requests.post(url, data=data)
    r.raise_for_status()

    return r.json()["access_token"]

# ==============================
# UPLOAD TO SHAREPOINT
# ==============================

def upload_to_sharepoint(file_path):

    logging.info("Uploading resignation dataset to SharePoint...")

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    site = requests.get(
        "https://graph.microsoft.com/v1.0/sites/thelearningtrust.sharepoint.com:/sites/TheLearningTrust",
        headers=headers
    )
    site.raise_for_status()
    site_id = site.json()["id"]

    drive = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive",
        headers=headers
    )
    drive.raise_for_status()
    drive_id = drive.json()["id"]

    file_name = os.path.basename(file_path)

    upload_url = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
        f"/root:/Consolidated data/{file_name}:/content"
    )

    with open(file_path, "rb") as f:
        res = requests.put(
            upload_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "text/csv"
            },
            data=f
        )

    res.raise_for_status()

    logging.info("✅ Resignation dataset uploaded successfully")

# ==============================
# UPLOAD PARTICIPANT LIST AS-IS
# ==============================

def upload_participant_list():

    logging.info("Creating Participant List file...")

    participant_raw = pd.read_csv(participant_url, dtype=str)

    participant_raw.columns = (
        participant_raw.columns.astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )

    participant_raw = participant_raw.fillna("")

    participant_file = "Participant List.xlsx"

    participant_raw.to_excel(participant_file, index=False)

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    site = requests.get(
        "https://graph.microsoft.com/v1.0/sites/thelearningtrust.sharepoint.com:/sites/TheLearningTrust",
        headers=headers
    )
    site.raise_for_status()
    site_id = site.json()["id"]

    drive = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive",
        headers=headers
    )
    drive.raise_for_status()
    drive_id = drive.json()["id"]

    upload_url = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
        f"/root:/Consolidated data/Participant List.xlsx:/content"
    )

    with open(participant_file, "rb") as f:
        res = requests.put(
            upload_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            },
            data=f
        )

    res.raise_for_status()

    logging.info("✅ Participant List uploaded successfully")

# ==============================
# RUN
# ==============================

upload_to_sharepoint(OUTPUT_FILE)

upload_participant_list()

logging.info("🎉 PIPELINE COMPLETE")
