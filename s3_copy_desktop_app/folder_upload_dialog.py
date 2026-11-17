"""Folder upload preview. All Tk operations stay on the application's UI thread."""

from __future__ import annotations

import csv
from pathlib import Path
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from .folder_upload import destination_key, map_local_folder, parse_destination, scan_s3_folders, validate_selection
from .s3_service import create_s3_client


class FolderUploadDialog(tk.Toplevel):
    def __init__(self, app):
        super().__init__(app.root)
        self.app = app
        self.title("Scan Folder Structure - Bulk Upload")
        self.geometry("1080x700")
        self.minsize(850, 550)
        self.transient(app.root)
        self.rows = []
        self.folders = []
        self.bucket = ""
        self.prefix = ""
        self._closed = False
        self._busy = False
        self._cancelled = threading.Event()
        self.protocol("WM_DELETE_WINDOW", self._close)
        self.local_var = tk.StringVar()
        configured_bucket = app.config.dest_bucket
        configured_prefix = app.config.dest_prefix
        self.destination_var = tk.StringVar(value=(f"s3://{configured_bucket}/{configured_prefix}" if configured_bucket else ""))
        self.all_files_var = tk.BooleanVar(value=False)
        self.status_var = tk.StringVar(value="Choose your local folder and destination, then scan. No files are uploaded during scanning.")
        self.folder_var = tk.StringVar()

        body = ttk.Frame(self, padding=12)
        body.pack(fill="both", expand=True)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(5, weight=1)
        ttk.Label(body, text="Local Upload Folder").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.local_entry = ttk.Entry(body, textvariable=self.local_var)
        self.local_entry.grid(row=0, column=1, sticky="ew", pady=4)
        self.browse_button = ttk.Button(body, text="Browse...", command=self._browse)
        self.browse_button.grid(row=0, column=2, padx=(8, 0))
        ttk.Label(body, text="S3 Destination").grid(row=1, column=0, sticky="w", padx=(0, 8))
        self.dest_entry = ttk.Entry(body, textvariable=self.destination_var)
        self.dest_entry.grid(row=1, column=1, columnspan=2, sticky="ew", pady=4)
        self.filter_button = ttk.Checkbutton(body, text="Include all file types (otherwise media, artwork and captions only; hidden files and links are skipped)", variable=self.all_files_var)
        self.filter_button.grid(row=2, column=0, columnspan=3, sticky="w", pady=6)
        self.scan_button = ttk.Button(body, text="Scan Folder Structure & Map Files", command=self._scan)
        self.scan_button.grid(row=3, column=0, columnspan=3, sticky="w", pady=6)
        ttk.Label(body, textvariable=self.status_var, wraplength=990, justify="left").grid(row=4, column=0, columnspan=3, sticky="ew", pady=6)

        table = ttk.Frame(body)
        table.grid(row=5, column=0, columnspan=3, sticky="nsew")
        table.columnconfigure(0, weight=1)
        table.rowconfigure(0, weight=1)
        self.tree = ttk.Treeview(table, columns=("upload", "local", "folder", "destination"), show="headings", selectmode="extended")
        for column, title, width in (("upload", "Upload?", 75), ("local", "Local File (relative)", 300), ("folder", "Desired Move Folder", 180), ("destination", "Resolved S3 Destination (read only)", 550)):
            self.tree.heading(column, text=title)
            self.tree.column(column, width=width, minwidth=65, stretch=column != "upload")
        self.tree.grid(row=0, column=0, sticky="nsew")
        vertical = ttk.Scrollbar(table, orient="vertical", command=self.tree.yview)
        vertical.grid(row=0, column=1, sticky="ns")
        horizontal = ttk.Scrollbar(table, orient="horizontal", command=self.tree.xview)
        horizontal.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
        self.tree.bind("<Double-1>", lambda event: self._toggle())
        self.tree.bind("<space>", lambda event: self._toggle())

        selection = ttk.Frame(body)
        selection.grid(row=6, column=0, columnspan=3, sticky="ew", pady=8)
        ttk.Button(selection, text="Select All", command=lambda: self._select_all(True)).pack(side="left")
        ttk.Button(selection, text="Select None", command=lambda: self._select_all(False)).pack(side="left", padx=6)
        ttk.Button(selection, text="Toggle Highlighted Files", command=self._toggle).pack(side="left")
        mapping = ttk.Frame(body)
        mapping.grid(row=7, column=0, columnspan=3, sticky="ew", pady=4)
        mapping.columnconfigure(1, weight=1)
        ttk.Label(mapping, text="Map highlighted files to:").grid(row=0, column=0, padx=(0, 8))
        self.folder_combo = ttk.Combobox(mapping, textvariable=self.folder_var, state="readonly")
        self.folder_combo.grid(row=0, column=1, sticky="ew")
        ttk.Button(mapping, text="Apply Folder", command=self._apply_folder).grid(row=0, column=2, padx=(8, 0))
        ttk.Label(body, text="Double-click a file or press Space to include/exclude it. Unmatched folders start excluded; map them explicitly.\nFiles keep their names. After confirmation, every destination is checked and existing files require overwrite approval.", wraplength=990, justify="left").grid(row=8, column=0, columnspan=3, sticky="ew", pady=8)
        buttons = ttk.Frame(body)
        buttons.grid(row=9, column=0, columnspan=3, sticky="e")
        self.export_button = ttk.Button(buttons, text="Export Mapping CSV...", command=self._export, state="disabled")
        self.export_button.pack(side="left", padx=4)
        ttk.Button(buttons, text="Cancel", command=self._close).pack(side="left", padx=4)
        self.upload_button = ttk.Button(buttons, text="Review & Upload Selected...", command=self._upload, state="disabled")
        self.upload_button.pack(side="left", padx=4)
        for variable in (self.local_var, self.destination_var, self.all_files_var):
            variable.trace_add("write", self._invalidate_scan)

    def _invalidate_scan(self, *_args):
        if self.folders and not self._busy:
            self.folders = []
            self.rows = []
            self.tree.delete(*self.tree.get_children())
            self.folder_combo.configure(values=[])
            self.export_button.configure(state="disabled")
            self.upload_button.configure(state="disabled")
            self.status_var.set("Inputs changed. Scan again to refresh the mapping before exporting or uploading.")

    def _browse(self):
        folder = filedialog.askdirectory(parent=self, title="Select Local Upload Folder")
        if folder:
            self.local_var.set(folder)

    def _close(self):
        self._closed = True
        self._cancelled.set()
        self.destroy()

    def _scan(self):
        if self._busy or self.app._running:
            messagebox.showerror("Folder Upload", "Wait for the current operation to finish.", parent=self)
            return
        try:
            bucket, prefix = parse_destination(self.destination_var.get())
            local = self.local_var.get().strip()
            if local and not Path(local).is_dir():
                raise ValueError("Choose an existing local folder, or leave it blank to export a folder-only template.")
            credentials = self.app._get_active_credentials()
        except Exception as error:
            messagebox.showerror("Folder Scan", str(error), parent=self)
            return
        include_all = self.all_files_var.get()
        config = self.app.config
        self._busy = True
        self.rows = []
        self.folders = []
        self.tree.delete(*self.tree.get_children())
        self.export_button.configure(state="disabled")
        self.upload_button.configure(state="disabled")
        for widget in (self.scan_button, self.local_entry, self.dest_entry, self.browse_button, self.filter_button):
            widget.configure(state="disabled")
        self.status_var.set("Scanning destination folders and local files... Nothing is being uploaded. Cancel closes this scan.")

        def worker():
            try:
                client = create_s3_client(config, credentials)
                try:
                    folders = scan_s3_folders(client, bucket, prefix, self._cancelled.is_set)
                finally:
                    client.close()
                rows = map_local_folder(local, prefix, folders, include_all) if local else []
                self.app._enqueue_ui(self._scan_complete, bucket, prefix, folders, rows, None)
            except Exception as error:
                self.app._enqueue_ui(self._scan_complete, bucket, prefix, [], [], str(error))
        threading.Thread(target=worker, daemon=True).start()

    def _scan_complete(self, bucket, prefix, folders, rows, error):
        if self._closed:
            return
        self._busy = False
        for widget in (self.scan_button, self.local_entry, self.dest_entry, self.browse_button, self.filter_button):
            widget.configure(state="normal")
        if error:
            self.status_var.set(f"Scan failed: {error}. Nothing uploaded.")
            messagebox.showerror("Folder Scan Failed", error, parent=self)
            return
        self.bucket, self.prefix, self.folders, self.rows = bucket, prefix, folders, rows
        self.folder_combo.configure(values=["(destination root)", *[folder for folder in folders if folder]])
        self.folder_var.set("(destination root)")
        for index in range(len(rows)):
            self.tree.insert("", "end", iid=str(index))
        self.export_button.configure(state="normal")
        self.upload_button.configure(state="normal" if rows else "disabled")
        self._refresh()

    def _refresh(self):
        for index, row in enumerate(self.rows):
            uri = f"s3://{self.bucket}/{destination_key(self.prefix, row.folder, Path(row.local_path).name)}" if row.folder is not None else "Unmatched - choose a folder or exclude"
            self.tree.item(str(index), values=("Yes" if row.selected else "No", row.relative_path, row.folder if row.folder else ("(destination root)" if row.folder == "" else "UNMATCHED"), uri))
        unmatched = sum(row.folder is None for row in self.rows)
        selected = sum(row.selected for row in self.rows)
        self.status_var.set(f"Scanned s3://{self.bucket}/{self.prefix} | {max(0, len(self.folders) - 1)} subfolders, {len(self.rows)} files, {selected} selected, {unmatched} unmatched. Export saves this scanned destination; rescan after changing inputs.")

    def _select_all(self, selected):
        for row in self.rows:
            row.selected = selected
        if self.folders:
            self._refresh()

    def _toggle(self):
        for index in self.tree.selection():
            row = self.rows[int(index)]
            row.selected = not row.selected
        if self.folders:
            self._refresh()
        return "break"

    def _apply_folder(self):
        folder = self.folder_var.get()
        folder = "" if folder == "(destination root)" else folder
        if folder not in self.folders:
            return
        for index in self.tree.selection():
            row = self.rows[int(index)]
            row.folder = folder
            row.selected = True
        self._refresh()

    def _export(self):
        try:
            selected = validate_selection(self.rows, self.prefix) if self.rows else []
        except ValueError as error:
            messagebox.showerror("Export Mapping", str(error), parent=self)
            return
        path = filedialog.asksaveasfilename(parent=self, title="Export Folder Upload Mapping", defaultextension=".csv", initialfile="folder_upload_mapping.csv", filetypes=[("CSV", "*.csv")])
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Title #", "Local File Path", "Desired Move Folder", "Destination Bucket", "Destination Prefix"])
                if selected:
                    for index, row in enumerate(selected, 1):
                        writer.writerow([index, row.local_path, row.folder, self.bucket, self.prefix])
                else:
                    for index, folder in enumerate(self.folders[1:] or [""], 1):
                        writer.writerow([index, "", folder, self.bucket, self.prefix])
            messagebox.showinfo("Mapping Exported", f"Saved to:\n{path}\n\nThe destination bucket and base prefix are included so reloading preserves the mapping.", parent=self)
        except OSError as error:
            messagebox.showerror("Export Failed", str(error), parent=self)

    def _upload(self):
        try:
            selected = validate_selection(self.rows, self.prefix)
        except ValueError as error:
            messagebox.showerror("Folder Upload", str(error), parent=self)
            return
        destination = f"s3://{self.bucket}/{self.prefix}"
        if not messagebox.askokcancel("Confirm Folder Upload Mapping", f"Upload {len(selected)} selected file(s) to:\n{destination}\n\nReview the mapping table before continuing. Local files will be kept. The app will check every destination and ask before overwriting existing files.\n\nContinue to destination checks?", parent=self):
            return
        if self.app._start_mapped_folder_upload(selected, self.bucket, self.prefix):
            self._close()
