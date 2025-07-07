import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext

# === GUI App ===
class MailCopyApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("1stEnergy - Copy Mail House")
        self.geometry("700x500")
        self.configure(padx=20, pady=20)

        # Default values
        self.default_root = r"Y:\FIRST_LIVE\Documents\Mail Merges"
        self.default_csv = r"C:\Users\manh.nguyen\Downloads\Simulate\Melhouse\Success_Copied.csv"
        self.default_base = r"C:\Users\manh.nguyen\Downloads\Simulate\Melhouse"

        # UI Components
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

        tk.Button(self, text="Generate", command=self.run_process).pack(pady=10)

        self.output = scrolledtext.ScrolledText(self, height=20, state='disabled', font=('Consolas', 10))
        self.output.pack(fill='both', expand=True)

    def log(self, message):
        self.output.config(state='normal')
        self.output.insert('end', message + '\n')
        self.output.see('end')
        self.output.config(state='disabled')
        self.update()

    def run_process(self):
        threading.Thread(target=self.generate, daemon=True).start()

    def generate(self):
        import time
        start_time = time.time()

        ROOT_DIR = self.entry_root.get()
        CSV_PATH = self.entry_csv.get()
        BASE_PATH = self.entry_base.get()

        if not os.path.exists(ROOT_DIR):
            messagebox.showerror("Error", f"ROOT_DIR does not exist: {ROOT_DIR}")
            return

        cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)
        self.log("⏱ Scanning for files...")

        file_list = []
        for root, dirs, files in os.walk(ROOT_DIR):
            if 'archive' in dirs:
                dirs.remove('archive')
            for file in files:
                full_path = os.path.join(root, file)
                try:
                    ctime = datetime.fromtimestamp(os.path.getctime(full_path))
                    if ctime >= cutoff:
                        file_list.append((full_path, ctime))
                except Exception as e:
                    self.log(f"⚠️ Error reading file: {file} - {e}")

        df_today = pd.DataFrame(file_list, columns=["FullPath", "Created_Datetime"])
        df_today["Created_Datetime"] = df_today["Created_Datetime"].dt.strftime("%d/%m/%Y %H:%M:%S")

        self.log(f"✅ Files to copy: {len(df_today)}")

        try:
            df_existing = pd.read_csv(CSV_PATH)
            df_existing["Created_Datetime"] = pd.to_datetime(df_existing["Created_Datetime"], dayfirst=True, errors='coerce')
        except FileNotFoundError:
            df_existing = pd.DataFrame(columns=["FullPath", "Created_Datetime"])

        new_files = df_today[~df_today["FullPath"].isin(df_existing["FullPath"])]

        # === Create Destination Directory ===
        today = datetime.today()
        year = today.strftime("%Y")
        month = today.strftime("%m %B")
        day = today.strftime("%d-%m-%Y")

        destination_dir = os.path.join(BASE_PATH, year, month, day)
        os.makedirs(destination_dir, exist_ok=True)

        subfolders = [f"Invoices {i if i > 1 else ''}" for i in range(1, 6)]
        for sub in subfolders:
            os.makedirs(os.path.join(destination_dir, sub), exist_ok=True)

        self.log(f"✅ Created or found: {destination_dir}")
        self.log("📂 Subfolders created:")
        for sub in subfolders:
            self.log(f"   - {sub}")

        # === Copy Files ===
        self.log("🚚 Copying files in parallel...")
        success, fail, stats = [], [], {}

        for _, row in new_files.iterrows():
            src = row["FullPath"]
            created_dt = row["Created_Datetime"]
            try:
                rel = os.path.relpath(src, ROOT_DIR)
                dst = os.path.join(destination_dir, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
                success.append({"FullPath": src, "Created_Datetime": created_dt})
                folder = rel.split(os.sep)[0]
                stats[folder] = stats.get(folder, 0) + 1
            except Exception as e:
                fail.append({"FullPath": src, "Created_Datetime": created_dt, "Reason": str(e)})
                self.log(f"❌ Failed to copy: {src} — {e}")

        # === Save CSV ===
        df_success = pd.DataFrame(success)
        df_unsuccess = pd.DataFrame(fail)

        cutoff_60 = today - timedelta(days=60)
        df_existing = df_existing[df_existing["Created_Datetime"] >= cutoff_60]
        df_success["Created_Datetime"] = pd.to_datetime(df_success["Created_Datetime"], dayfirst=True, errors='coerce')
        df_success = df_success[df_success["Created_Datetime"] >= cutoff_60]

        final = pd.concat([df_success, df_existing], ignore_index=True)
        final["Created_Datetime"] = final["Created_Datetime"].dt.strftime("%d/%m/%Y %H:%M:%S")
        final.to_csv(CSV_PATH, index=False)

        self.log(f"✅ Total files copied successfully: {len(df_success)}")
        self.log(f"❌ Total files failed to copy: {len(df_unsuccess)}")
        self.log(f"🕒 Total execution time: {time.time() - start_time:.2f} seconds")

        self.log("\n📊 Summary report:")
        for folder, count in stats.items():
            self.log(f"   {folder}: {count}")

        self.log("✅ Done!")


if __name__ == "__main__":
    app = MailCopyApp()
    app.mainloop()
