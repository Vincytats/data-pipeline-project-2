import os
import requests
import pandas as pd

# =====================================
# CONFIG
# =====================================

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

SHAREPOINT_SITE = "thelearningtrust.sharepoint.com"
SITE_PATH = "/sites/TheLearningTrust"

WORKBOOK_NAME = "SEF 4 Midline Stats Analysis.xlsx"

MONTHLY_REPORT_ID = "1itWYfUTkxA3g1qJThcaQ9XmgkOzOrngMgqjNSess20A"
MONTHLY_SHEET = "Monthly reporting"


# =====================================
# MICROSOFT GRAPH AUTH
# =====================================

def get_access_token():

    url = (
        f"https://login.microsoftonline.com/"
        f"{TENANT_ID}/oauth2/v2.0/token"
    )

    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }

    r = requests.post(url, data=data)
    r.raise_for_status()

    return r.json()["access_token"]


def get_drive_id():

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    site_url = (
        f"https://graph.microsoft.com/v1.0/sites/"
        f"{SHAREPOINT_SITE}:{SITE_PATH}"
    )

    site_res = requests.get(site_url, headers=headers)
    site_res.raise_for_status()

    site_id = site_res.json()["id"]

    drive_url = (
        f"https://graph.microsoft.com/v1.0/sites/"
        f"{site_id}/drives"
    )

    drive_res = requests.get(drive_url, headers=headers)
    drive_res.raise_for_status()

    drives = drive_res.json()["value"]

    for drive in drives:

        if drive["name"] in ["Documents", "Shared Documents"]:
            return drive["id"]

    raise Exception("Documents library not found")


# =====================================
# DOWNLOAD WORKBOOK
# =====================================

def download_from_sharepoint(filename):

    token = get_access_token()
    drive_id = get_drive_id()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    search_url = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
        f"/root:/Consolidated data:/children"
    )

    res = requests.get(search_url, headers=headers)
    res.raise_for_status()

    file_id = None

    for item in res.json()["value"]:
        print("FOUND:", item["name"])

        if item["name"].strip() == filename.strip():
            file_id = item["id"]
            break

    if not file_id:
        raise Exception(f"{filename} not found")

    download_url = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
        f"/items/{file_id}/content"
    )

    r = requests.get(download_url, headers=headers)
    r.raise_for_status()

    with open(filename, "wb") as f:
        f.write(r.content)

    print(f"✅ Downloaded {filename}")
# =====================================
# UPLOAD WORKBOOK
# =====================================

def upload_to_sharepoint(filename):

    token = get_access_token()
    drive_id = get_drive_id()

    upload_url = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
        f"/root:/Consolidated data/{filename}:/content"
    )

    with open(filename, "rb") as f:

        res = requests.put(
            upload_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            },
            data=f
        )

    res.raise_for_status()

    print("✅ Uploaded workbook")


# =====================================
# LOAD MAPPINGS
# =====================================

def load_mappings(workbook_name):

    mapping_df = pd.read_excel(
        workbook_name,
        sheet_name="variables to be changed",
        header=None
    )

    ip_mapping = {}

    for _, row in mapping_df.iloc[3:50].iterrows():

        source = row[2]
        target = row[3]

        if pd.notna(source) and pd.notna(target):

            ip_mapping[
                str(source).strip()
            ] = str(target).strip()

    indicator_mapping = {}

    for _, row in mapping_df.iloc[3:50].iterrows():

        source = row[8]
        target = row[9]

        if pd.notna(source) and pd.notna(target):

            indicator_mapping[
                str(source).strip()
            ] = str(target).strip()

    return ip_mapping, indicator_mapping


# =====================================
# MONTHLY REPORT
# =====================================

def build_outputs(ip_mapping, indicator_mapping):

    monthly_url = (
        f"https://docs.google.com/spreadsheets/d/"
        f"{MONTHLY_REPORT_ID}/export?format=xlsx"
    )

    raw = pd.read_excel(
        monthly_url,
        sheet_name=MONTHLY_SHEET,
        header=None
    )

    headers = raw.iloc[1].fillna("").astype(str)

    df = raw.iloc[2:].copy()

    df.columns = headers

    df = df.fillna(0)

    ip_col = df.columns[1]

    indicator_cols = []

    for col in df.columns[2:]:

        col_name = str(col).strip()

        if not col_name:
            continue

        if "comment" in col_name.lower():
            continue

        indicator_cols.append(col)

    for col in indicator_cols:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        ).fillna(0)

    outputs = (
        df
        .groupby(ip_col)[indicator_cols]
        .sum()
        .reset_index()
    )

    outputs.rename(
        columns={ip_col: "IP Name"},
        inplace=True
    )

    outputs["IP Name"] = outputs["IP Name"].map(
        lambda x: ip_mapping.get(
            str(x).strip(),
            str(x).strip()
        )
    )

    outputs.rename(
        columns=indicator_mapping,
        inplace=True
    )

    outputs["Criteria"] = "Outputs"

    return outputs


# =====================================
# UPDATE WORKBOOK
# =====================================

def update_workbook(workbook_name, outputs):

    sheet1 = pd.read_excel(
        workbook_name,
        sheet_name="Sheet1"
    )

    outputs_mask = (
        sheet1["Criteria"]
        .astype(str)
        .str.strip()
        .eq("Outputs")
    )

    for _, row in outputs.iterrows():

        ip_name = row["IP Name"]

        target_rows = sheet1[
            outputs_mask &
            (
                sheet1["IP Name"]
                .astype(str)
                .str.strip()
                .eq(ip_name)
            )
        ].index

        if len(target_rows) == 0:
            continue

        idx = target_rows[0]

        for col in outputs.columns:

            if col in ["IP Name", "Criteria"]:
                continue

            if col in sheet1.columns:

                sheet1.loc[idx, col] = row[col]

    with pd.ExcelWriter(
        workbook_name,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:

        sheet1.to_excel(
            writer,
            sheet_name="Sheet1",
            index=False
        )

    print("✅ Workbook updated")


# =====================================
# MAIN
# =====================================

def run_pipeline():

    print("🚀 Starting SEF4 Pipeline")

    download_from_sharepoint(WORKBOOK_NAME)

    ip_mapping, indicator_mapping = load_mappings(
        WORKBOOK_NAME
    )

    outputs = build_outputs(
        ip_mapping,
        indicator_mapping
    )

    update_workbook(
        WORKBOOK_NAME,
        outputs
    )

    upload_to_sharepoint(
        WORKBOOK_NAME
    )

    print("🎉 SEF4 Pipeline Complete")


if __name__ == "__main__":
    run_pipeline()
