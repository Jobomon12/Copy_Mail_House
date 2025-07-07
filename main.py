# ============================================================ SETUP ============================================================
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
import time
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed

# === PATHS ===
ROOT_DIR = r"Y:\FIRST_LIVE\Documents\Mail Merges"
CSV_PATH = r"C:\Users\manh.nguyen\Downloads\Simulate\Melhouse\Success_Copied.csv"
BASE_PATH = r"C:\Users\manh.nguyen\Downloads\Simulate\Melhouse"

# === MAPPINGS ===
FOLDER_NAME_MAP = {
    "Arrears_on_hold": "Arrears on Hold",
    "Best Offer Letter": "Best Offer Letters",
    "Concession Ineligibility Letters": "Concession Ineligibility Letter",
    "Deconsolidation Letter": "Deconsolidation Letter",
    "Direct Debit": "Direct Debit Confirmation",
    "EBR Ineligibility Letters": "EBR Ineligibility Letters",
    "EGAS Forms": "EGAS Forms",
    "Fact Sheets": "Fact Sheets",
    "Failed Direct Debit": "Failed Direct debits",
    "Fixed": "Fixed",
    "Life Support - DR Letters": "Life Support - DR",
    "Life Support - MC Letters": "Life Support - MC  Letters",
    "Life Support - MC Reminder Letters": "Life Support- MC Reminder Letters",
    "Life Support Expiry Letters": "Life Support Expiry Letters",
    "Meter Exchange Letters": "Meter Exchange Letter",
    "Occupier Disconnection Letter": "Occupier Disconnection POSTCARD",
    "Occupier Welcome Pack": "Occupier Welcomepack",
    "Payment Plan Letters": "Payment plan Letter",
    "Quality Check": "Quality Check",
    "Retention": "Retention Letters",
    "Self Read Rejected Letters": "Self Read Rejection"
}

# ============================================================ FUNCTIONS ============================================================

def scan_directory(path, excluded_folder, cutoff_datetime):
    file_list = []
    excluded_folder = excluded_folder.lower()

    def _scan(current_path):
        try:
            with os.scandir(current_path) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if entry.name.lower() != excluded_folder:
                                _scan(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            stat = entry.stat()
                            ctime = datetime.fromtimestamp(stat.st_ctime)
                            if ctime >= cutoff_datetime:
                                file_list.append((entry.path, ctime))
                    except Exception as e:
                        print(f"⚠️ Error accessing {entry.path}: {e}")
        except Exception as e:
            print(f"❌ Error scanning {current_path}: {e}")

    _scan(path)
    return file_list


def create_destination_dir(base_path):
    today = datetime.today()
    year = today.strftime("%Y")
    month_folder = today.strftime("%m %B")
    day_folder = today.strftime("%d-%m-%Y")
    destination_dir = os.path.join(base_path, year, month_folder, day_folder)

    # Base folders
    subfolders = [f"Invoices {i if i > 1 else ''}" for i in range(1, 6)]

    # Additional folders
    extra_folders = [
        "Adhoc & Instant invoices",
        "Network Tariff Change Letter",
        "Rebate Benefit end letter",
        "Rebate Benefit end letter -Email",
        "VIC Govt Grants",
        "QLD Govt Grants"
    ]

    # Combine and create
    all_folders = subfolders + extra_folders
    for sub in all_folders:
        os.makedirs(os.path.join(destination_dir, sub), exist_ok=True)

    return destination_dir


def load_existing_success_csv(csv_path):
    try:
        df = pd.read_csv(csv_path)
        df["Created_Datetime"] = pd.to_datetime(df["Created_Datetime"], dayfirst=True, errors='coerce')
        return df
    except FileNotFoundError:
        print(f"⚠️ File not found: {csv_path}. A new one will be created.")
        return pd.DataFrame(columns=["FullPath", "Created_Datetime"])


# === Multithreaded File Copy Function ===
def copy_file_task(src_path, rel_path, dest_root):
    created_str = datetime.fromtimestamp(os.path.getctime(src_path)).strftime("%d/%m/%Y %H:%M:%S")
    try:
        dest_path = os.path.join(dest_root, rel_path)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.copy2(src_path, dest_path)
        top_folder = rel_path.split(os.sep)[0]
        return {"success": True, "FullPath": src_path, "Created_Datetime": created_str, "TopFolder": top_folder}
    except Exception as e:
        return {"success": False, "FullPath": src_path, "Created_Datetime": created_str, "Reason": str(e)}

# ============================================================ MAIN EXECUTION ============================================================

if __name__ == "__main__":
    start_time = time.time()

    # --- Phase I: Scan directory ---
    cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)
    print("🔍 Scanning files...")
    file_list = scan_directory(ROOT_DIR, excluded_folder="archive", cutoff_datetime=cutoff)
    df_today = pd.DataFrame(file_list, columns=["FullPath", "Created_Datetime"])
    df_today["Created_Datetime"] = df_today["Created_Datetime"].dt.strftime("%d/%m/%Y %H:%M:%S")

    df_existing = load_existing_success_csv(CSV_PATH)
    new_files = df_today[~df_today["FullPath"].isin(df_existing["FullPath"])]

    print(f"📦 Files to copy: {len(new_files)}")
    print(f"⏱ Scan time: {time.time() - start_time:.2f} seconds")

    # --- Phase II: Parallel Copy ---
    destination_dir = create_destination_dir(BASE_PATH)
    success_records = []
    failure_records = []
    folder_stats = {}

    print("🚚 Copying files in parallel...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for _, row in new_files.iterrows():
            src_path = row["FullPath"]
            rel_path = os.path.relpath(src_path, ROOT_DIR)
            futures.append(executor.submit(copy_file_task, src_path, rel_path, destination_dir))

        for future in as_completed(futures):
            result = future.result()
            if result["success"]:
                success_records.append({
                    "FullPath": result["FullPath"],
                    "Created_Datetime": result["Created_Datetime"]
                })
                folder = result["TopFolder"]
                folder_stats[folder] = folder_stats.get(folder, 0) + 1
            else:
                failure_records.append({
                    "FullPath": result["FullPath"],
                    "Created_Datetime": result["Created_Datetime"],
                    "Reason": result["Reason"]
                })
                print(f"❌ Failed: {result['FullPath']} | Reason: {result['Reason']}")

    # --- Phase III: Save report ---
    df_success = pd.DataFrame(success_records)
    df_unsuccess = pd.DataFrame(failure_records)

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_60_days = today - timedelta(days=60)

    df_existing = df_existing[df_existing["Created_Datetime"] >= cutoff_60_days]
    df_success["Created_Datetime"] = pd.to_datetime(df_success["Created_Datetime"], dayfirst=True, errors='coerce')
    df_success = df_success[df_success["Created_Datetime"] >= cutoff_60_days]

    combined = pd.concat([df_success, df_existing], ignore_index=True)
    combined["Created_Datetime"] = combined["Created_Datetime"].dt.strftime("%d/%m/%Y %H:%M:%S")
    combined.to_csv(CSV_PATH, index=False)

    report_df = pd.DataFrame(list(folder_stats.items()), columns=["Folder Name", today.strftime("%d-%m-%Y")])
    report_df["Folder Name"] = report_df["Folder Name"].replace(FOLDER_NAME_MAP)

    print(f"\n✅ Total files copied successfully: {len(df_success)}")
    print(f"❌ Total files failed to copy: {len(df_unsuccess)}")
    print(f"🕒 Total execution time: {time.time() - start_time:.2f} seconds")
    print("\n📊 Summary report:")
    print(report_df)

    gc.collect()

