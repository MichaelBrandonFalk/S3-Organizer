"""Notebook navigation that wraps full labels instead of clipping them."""

from tkinter import StringVar, ttk


class WrappedModeTabs(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.columnconfigure(0, weight=1)
        style = ttk.Style(self)
        style.layout("ModeBody.TNotebook.Tab", [])
        style.configure("Mode.Toolbutton", padding=(10, 6), font="TkDefaultFont")
        self.navigation = ttk.Frame(self, height=1)
        self.navigation.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.notebook = ttk.Notebook(self, style="ModeBody.TNotebook")
        self.notebook.grid(row=1, column=0, sticky="ew")
        self.selected = StringVar(master=self)
        self.buttons = []
        self.navigation.bind("<Configure>", self._layout)
        self.notebook.bind("<<NotebookTabChanged>>", self._sync_selection, add="+")

    def populate(self):
        for tab in self.notebook.tabs():
            button = ttk.Radiobutton(
                self.navigation,
                text=self.notebook.tab(tab, "text"),
                value=tab,
                variable=self.selected,
                style="Mode.Toolbutton",
                command=lambda target=tab: self.notebook.select(target),
                takefocus=True,
            )
            self.buttons.append(button)
        self._sync_selection()
        self.after_idle(self._layout)

    def _sync_selection(self, _event=None):
        self.selected.set(self.notebook.select())

    def _layout(self, _event=None):
        available = self.navigation.winfo_width()
        if available <= 1 or not self.buttons:
            return
        x = y = row_height = 0
        gap = 6
        for button in self.buttons:
            width = button.winfo_reqwidth()
            height = button.winfo_reqheight()
            if x and x + width > available:
                x = 0
                y += row_height + gap
                row_height = 0
            button.place(x=x, y=y, width=width, height=height)
            x += width + gap
            row_height = max(row_height, height)
        self.navigation.configure(height=y + row_height)
