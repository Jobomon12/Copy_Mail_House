import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext
from concurrent.futures import ThreadPoolExecutor

class MailCopyApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("1stEnergy - Copy Mail House")
        self.geometry("780x520")
        self.configure(padx=20, pady=20)

        self.default_root = r"Y:\FIRST_LIVE\Documents\Mail Merges"
        self.default_csv = r"C:\Users\manh.nguyen\Downloads\Simulate\Melhouse\Success_Copied.csv"
        self.default_base = r"C:\Users\manh.nguyen\Downloads\Simulate\Melhouse"

        self.create_widgets()

    def create_widgets(self):
        tk.Label(self, text="ROOT_DIR (Mail Merge Source):").pack(anchor='w')
        self.entry_root = tk.Entry(self, width=100)
        self.entry_root.insert(0, self.default_root)
        self.entry_root.pack(fill='x')

        tk.Label(self, text="CSV_PATH (Success Tracker):").pack(anchor='w')
        self.entry_csv = tk.Entry(self, width=100)
        self.entry_csv.insert(0, self.default_csv)
        self.entry_csv.pack(fill='x')

        tk.Label(self, text="BASE_PATH (Destination Root):").pack(anchor='w')
        self.entry_base = tk.Entry(self, width=100)
        self.entry_base.insert(0, self.default_base)
        self.entry_base.pack(fill='x')

        tk.Button(self, text="Generate", command=self.run_threaded).pack(pady=10)

        self.output = scrolledtext.ScrolledText(self, height=20, state='disabled', font=('Consolas', 10))
        self.output.pack(fill='both', expand=True)

    def log(self, message):
        self.output.config(state='normal')
        self.output.insert('end', message + '\n')
        self.output.see('end')
        self.output.config(state='disabled')
        self.update()

    def run_threaded(self):
        threading.Thread(target=self.generate, daemon=True).start()

    def fast_scan(self, path, cutoff):
        stack = [path]
        result = []
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        if entry.is_dir(follow_symlinks=False):
                            if entry.name.lower() != "archive":
                                stack.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            try:
                                ctime = datetime.fromtimestamp(entry.stat().st_ctime)
                                if ctime >= cutoff:
                                    result.append((entry.path, ctime))
                            except:
                                pass
            except:
                pass
        return result

    def copy_file(self, args):
        src, rel_path, created_dt, root_dir, dest_root = args
        dst = os.path.join(dest_root, rel_path)
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            folder = rel_path.split(os.sep)[0]
            return {"FullPath": src, "Created_Datetime": created_dt, "Success": True, "TopFolder": folder}
        except Exception as e:
            return {"FullPath": src, "Created_Datetime": created_dt, "Success": False, "Reason": str(e)}

    def generate(self):
        import time
        start_time = time.time()

        ROOT_DIR = self.entry_root.get()
        CSV_PATH = self.entry_csv.get()
        BASE_PATH = self.entry_base.get()

        cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)
        self.log("⏱ Scanning for files...")
        file_list = self.fast_scan(ROOT_DIR, cutoff)

        self.log(f"✅ Files to copy: {len(file_list)}")
        df_today = pd.DataFrame(file_list, columns=["FullPath", "Created_Datetime"])
        df_today["Created_Datetime"] = df_today["Created_Datetime"].dt.strftime("%d/%m/%Y %H:%M:%S")

        try:
            df_existing = pd.read_csv(CSV_PATH)
            df_existing["Created_Datetime"] = pd.to_datetime(df_existing["Created_Datetime"], dayfirst=True, errors='coerce')
        except FileNotFoundError:
            df_existing = pd.DataFrame(columns=["FullPath", "Created_Datetime"])

        new_files = df_today[~df_today["FullPath"].isin(df_existing["FullPath"])]
        self.log(f"🔍 New files: {len(new_files)}")

        today = datetime.today()
        year = today.strftime("%Y")
        month = today.strftime("%m %B")
        day = today.strftime("%d-%m-%Y")
        dest_root = os.path.join(BASE_PATH, year, month, day)
        os.makedirs(dest_root, exist_ok=True)

        subfolders = [f"Invoices {i if i > 1 else ''}" for i in range(1, 6)]
        for sub in subfolders:
            os.makedirs(os.path.join(dest_root, sub), exist_ok=True)

        self.log(f"✅ Created or found: {dest_root}")
        self.log("📂 Subfolders created:")
        for sub in subfolders:
            self.log(f"   - {sub}")

        self.log("🚚 Copying files in parallel...")
        args_list = []
        for _, row in new_files.iterrows():
            src_path = row["FullPath"]
            created_dt = row["Created_Datetime"]
            rel_path = os.path.relpath(src_path, ROOT_DIR)
            args_list.append((src_path, rel_path, created_dt, ROOT_DIR, dest_root))

        results = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(self.copy_file, args_list))

        df_success = pd.DataFrame([r for r in results if r["Success"]])
        df_fail = pd.DataFrame([r for r in results if not r["Success"]])
        folder_stats = df_success["TopFolder"].value_counts().to_dict()

        # Save updated CSV
        df_success["Created_Datetime"] = pd.to_datetime(df_success["Created_Datetime"], dayfirst=True, errors='coerce')
        cutoff_60 = today - timedelta(days=60)
        df_existing = df_existing[df_existing["Created_Datetime"] >= cutoff_60]
        df_success = df_success[df_success["Created_Datetime"] >= cutoff_60]
        combined = pd.concat([df_success[["FullPath", "Created_Datetime"]], df_existing], ignore_index=True)
        combined["Created_Datetime"] = combined["Created_Datetime"].dt.strftime("%d/%m/%Y %H:%M:%S")
        combined.to_csv(CSV_PATH, index=False)

        self.log(f"\n✅ Total files copied successfully: {len(df_success)}")
        self.log(f"❌ Total files failed to copy: {len(df_fail)}")
        self.log(f"🕒 Total execution time: {time.time() - start_time:.2f} seconds")

        self.log("\n📊 Summary report:")
        for folder, count in folder_stats.items():
            self.log(f"   {folder}: {count}")

        self.log("🎉 Done!")


if __name__ == "__main__":
    app = MailCopyApp()
    app.mainloop()
