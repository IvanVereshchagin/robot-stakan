import sys
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout,
    QToolBar, QPushButton, QTableWidget, QHeaderView,
    QDialog, QFormLayout, QLineEdit, QLabel, QMessageBox,
    QTableWidgetItem, QDialogButtonBox
)
from PyQt5.QtCore import Qt, QSize
from PyQt5.QtGui import QFont


# ─────────────────────────────────────────────
#  Диалог добавления инструмента
# ─────────────────────────────────────────────
class AddInstrumentDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Добавить инструмент")
        self.setMinimumWidth(360)
        self.setModal(True)

        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)

        # Заголовок
        title = QLabel("Новый инструмент")
        title.setFont(QFont("Segoe UI", 11, QFont.Bold))
        layout.addWidget(title)

        # Форма
        form = QFormLayout()
        form.setSpacing(8)

        self.ed_name  = QLineEdit()
        self.ed_isin  = QLineEdit()
        self.ed_board = QLineEdit()

        self.ed_name.setPlaceholderText("например: Сбербанк")
        self.ed_isin.setPlaceholderText("например: RU0009029540")
        self.ed_board.setPlaceholderText("например: TQBR")

        form.addRow("Название:", self.ed_name)
        form.addRow("ISIN:",     self.ed_isin)
        form.addRow("Board:",    self.ed_board)

        layout.addLayout(form)

        # Кнопки OK / Отмена
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.button(QDialogButtonBox.Ok).setText("Добавить")
        btns.button(QDialogButtonBox.Cancel).setText("Отмена")
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _on_accept(self):
        name  = self.ed_name.text().strip()
        isin  = self.ed_isin.text().strip()
        board = self.ed_board.text().strip()

        if not name:
            QMessageBox.warning(self, "Ошибка", "Введите название инструмента.")
            self.ed_name.setFocus()
            return
        if not isin:
            QMessageBox.warning(self, "Ошибка", "Введите ISIN.")
            self.ed_isin.setFocus()
            return
        if not board:
            QMessageBox.warning(self, "Ошибка", "Введите Board.")
            self.ed_board.setFocus()
            return

        self.accept()

    # Удобные геттеры
    def get_name(self)  -> str: return self.ed_name.text().strip()
    def get_isin(self)  -> str: return self.ed_isin.text().strip()
    def get_board(self) -> str: return self.ed_board.text().strip()


# ─────────────────────────────────────────────
#  Главное окно
# ─────────────────────────────────────────────
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Робот — Панель управления")
        self.setMinimumSize(900, 500)

        self._build_toolbar()
        self._build_central()

    # ── Тулбар ─────────────────────────────────
    def _build_toolbar(self):
        toolbar = QToolBar("Главная панель")
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(16, 16))
        toolbar.setStyleSheet("""
            QToolBar {
                background: #2b2b2b;
                border-bottom: 1px solid #444;
                padding: 4px 8px;
                spacing: 6px;
            }
            QPushButton {
                background: #4a9d5e;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 5px 14px;
                font-weight: bold;
            }
            QPushButton:hover  { background: #5ab870; }
            QPushButton:pressed { background: #3a8050; }
        """)
        self.addToolBar(toolbar)

        btn_add = QPushButton("+ Добавить")
        btn_add.clicked.connect(self.on_add_clicked)
        toolbar.addWidget(btn_add)

    # ── Центральный виджет ───────────────────────
    def _build_central(self):
        central = QWidget()
        self.setCentralWidget(central)

        layout = QVBoxLayout(central)
        layout.setContentsMargins(8, 8, 8, 8)

        # Таблица инструментов
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Название", "ISIN", "Board"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet("""
            QTableWidget {
                background: #1e1e1e;
                color: #dcdcdc;
                gridline-color: #3a3a3a;
                font-size: 13px;
            }
            QHeaderView::section {
                background: #2b2b2b;
                color: #aaaaaa;
                padding: 4px;
                border: none;
                border-bottom: 1px solid #444;
                font-weight: bold;
            }
            QTableWidget::item:selected {
                background: #3a6ea8;
                color: white;
            }
            QTableWidget { alternate-background-color: #252525; }
        """)

        layout.addWidget(self.table)

    # ── Обработчики ─────────────────────────────
    def on_add_clicked(self):
        dlg = AddInstrumentDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            name  = dlg.get_name()
            isin  = dlg.get_isin()
            board = dlg.get_board()
            self._add_row(name, isin, board)

    def _add_row(self, name: str, isin: str, board: str):
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(name))
        self.table.setItem(row, 1, QTableWidgetItem(isin))
        self.table.setItem(row, 2, QTableWidgetItem(board))


# ─────────────────────────────────────────────
#  Точка входа
# ─────────────────────────────────────────────
def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = MainWindow()
    win.show()
    return app.exec_() if hasattr(app, "exec_") else app.exec()


if __name__ == "__main__":
    sys.exit(main())