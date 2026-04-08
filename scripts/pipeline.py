import pandas as pd
import numpy as np
import logging
import requests
import os
from io import StringIO

logging.basicConfig(level=logging.INFO)

print("PIPELINE VERSION FINAL RUNNING")

# ==============================
# GOOGLE SHEETS
# ==============================

participant_sheet = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"
resignation_sheet = "18oOQZaVBgZDQSFLKz5JtpExsjDMydJPHG8LN1gfBsU4"

participant_url = f"https://docs.google.com/spreadsheets/d/{participant_sheet}/export?format=csv"
resignation_url = f"https://docs.google.com/spreadsheets/d/{resignation_sheet}/export?format=csv"

OUTPUT_FILE = "processed_attrition_dataset.csv"

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
        df.columns
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )
    return df

participant_df = normalize_columns(participant_df)
resignation_df = normalize_columns(resignation_df)

logging.info("Columns normalized")

# ==============================
# FIND COLUMN FUNCTION
# ==============================

def find_column(df, keywords):
    for col in df.columns:
        name = col.lower()
        if all(word in name for word in keywords):
            return col
    raise ValueError(f"Column containing {keywords} not found")

# ==============================
# DETECT COLUMNS
# ==============================

participant_id_col = find_column(participant_df, ["id"])
resignation_id_col = find_column(resignation_df, ["id"])

start_date_col = find_column(participant_df, ["start", "date"])
end_date_col = find_column(participant_df, ["end", "date"])
resignation_date_col = find_column(resignation_df, ["resignation", "date"])

logging.info("Important columns detected")

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

# ==============================
# REMOVE DUPLICATES
# ==============================

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
# MERGE
# ==============================

merged = participant_df.merge(
    resignation_df,
    on="ID Number",
    how="left"
)

logging.info("Datasets merged")

# ==============================
# STAY MONTHS
# ==============================

today = pd.Timestamp.today()

end_date = merged["Resignation Date"].fillna(
    merged["Participant End Date"]
).fillna(today)

start_date = merged["Participant Start Date"]

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

status_col = find_column(resignation_df, ["status"])

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
reason_col = find_column(merged, ["reason"])
survey_col = find_column(merged, ["survey"])
uif_col = find_column(merged, ["ui"])

org_col = None
for col in merged.columns:
    if "organisation" in col.lower() or "organization" in col.lower():
        org_col = col
        break

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
    "Organisation's name": merged[org_col]
})

# ==============================
# SAVE CSV
# ==============================

final_df.to_csv(OUTPUT_FILE, index=False)

logging.info("CSV created successfully")

# ==============================
# AUTH TOKEN
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
    token_data = r.json()

    print("TOKEN RESPONSE:", token_data)

    if "access_token" not in token_data:
        raise Exception(f"Token error: {token_data}")

    return token_data["access_token"]

# ==============================
# UPLOAD TO SHAREPOINT
# ==============================

def upload_to_sharepoint(file_path):
    logging.info("Uploading to SharePoint (correct folder)...")

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }

    # GET SITE
    site_url = "https://graph.microsoft.com/v1.0/sites/thelearningtrust.sharepoint.com:/sites/TheLearningTrust"
    site = requests.get(site_url, headers=headers).json()

    print("SITE:", site)

    site_id = site["id"]

    # GET DRIVE
    drive = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive",
        headers=headers
    ).json()

    print("DRIVE:", drive)

    drive_id = drive["id"]

    file_name = os.path.basename(file_path)

    # 🔥 IMPORTANT
    folder_path = "Consolidated%20data"

    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}/{file_name}:/content"

    print("UPLOAD URL:", upload_url)

    with open(file_path, "rb") as f:
        res = requests.put(upload_url, headers=headers, data=f)

    print("UPLOAD RESPONSE:", res.text)

    if res.status_code not in [200, 201]:
        raise Exception(f"Upload failed: {res.text}")

    logging.info("✅ Upload successful")

# ==============================
# RUN
# ==============================

upload_to_sharepoint(OUTPUT_FILE)

logging.info("🎉 PIPELINE COMPLETE")
