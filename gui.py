import sys
import os
import base64
import subprocess

import instruments_db as db

from datetime import datetime
from PyQt5.QtCore import Qt, QByteArray, QTimer, QThread, QObject, pyqtSignal
from PyQt5.QtGui import QFont, QIcon, QPixmap
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QToolBar, QPushButton, QTableWidget, QHeaderView,
    QDialog, QFormLayout, QLineEdit, QLabel, QMessageBox,
    QTableWidgetItem, QDialogButtonBox, QInputDialog,
    QRadioButton, QButtonGroup, QSpinBox,
    QListWidget, QListWidgetItem, QComboBox,
    QSplitter, QDoubleSpinBox, QPlainTextEdit
)


# Оставь свой текущий большой base64 из старого файла, если нужна иконка.
ICON_B64 = ""


def get_icon() -> QIcon:
    if not ICON_B64.strip():
        return QIcon()
    try:
        data = base64.b64decode(ICON_B64)
        px = QPixmap()
        px.loadFromData(QByteArray(data))
        return QIcon(px)
    except Exception:
        return QIcon()


HEADERS = [
    "Название",
    "Состояние",
    "Боевой\nрежим",
    "Торговля",
    "Лучший\nоффер кол-во",
    "Лучший\nоффер",
    "Лимит\nлучш. оффер",
    "Лимит\nцены",
    "Лимит\nбидов",
    "Тек. кол-во\nбид",
    "Лимит\nсделок",
    "Кол-во\nсделок",
    "Большой бид\n(Алерт)",
    "API\nTelegram",
    "Chat ID\nTelegram",
    "Аккаунт",
    "Код клиента",
]

COL_FIELD = {
    0: None,
    1: "condition",
    2: "battle_regime",
    3: "trade_interval",
    4: "best_offer_qty",
    5: "best_offer",
    6: "best_offer_limit",
    7: "price_limit",
    8: "bid_limit",
    9: None,
    10: "trades_limit",
    11: None,
    12: "big_bid_alert_qty",
    13: "tgapi",
    14: "tgchat",
    15: "account",
    16: "client_code",
}


class TradesCurrWidget(QWidget):
    def __init__(self, isin: str, value=0, parent=None):
        super().__init__(parent)
        self._isin = isin

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 0)
        layout.setSpacing(4)

        self._label = QLabel(str(value))
        self._label.setAlignment(Qt.AlignCenter)
        self._label.setMinimumWidth(36)
        self._label.setStyleSheet(
            "QLabel { color: #d0d0d0; background: transparent; border: none; font-size: 12px; }"
        )
        layout.addWidget(self._label, 1)

        self._btn = QPushButton("↺")
        self._btn.setToolTip("Сбросить счётчик сделок в 0")
        self._btn.setFixedSize(20, 20)
        self._btn.setCursor(Qt.PointingHandCursor)
        self._btn.setStyleSheet(
            "QPushButton { background: #3a3a3a; color: #bcbcbc; border: 1px solid #555555; border-radius: 4px; font-size: 11px; padding: 0px; }"
            "QPushButton:hover { background: #4a4a4a; color: #efefef; border: 1px solid #777777; }"
            "QPushButton:pressed { background: #2f2f2f; }"
        )
        self._btn.clicked.connect(self._on_reset_clicked)
        layout.addWidget(self._btn, 0, Qt.AlignCenter)

    def set_value(self, value):
        self._label.setText(str(value))

    def _on_reset_clicked(self):
        try:
            db.reset_trades_curr(self._isin)
            self.set_value(0)
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Не удалось сбросить trades_curr:\n{e}")


class ToggleWidget(QWidget):
    def __init__(self, isin: str, field: str, value: str = "OFF", parent=None):
        super().__init__(parent)
        self._isin = isin
        self._field = field

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 0)
        layout.setSpacing(6)

        self._grp = QButtonGroup(self)
        self._r_on = QRadioButton("Вкл")
        self._r_off = QRadioButton("Выкл")
        self._grp.addButton(self._r_on, 1)
        self._grp.addButton(self._r_off, 0)

        layout.addWidget(self._r_on)
        layout.addWidget(self._r_off)

        self._grp.blockSignals(True)
        if value == "ON":
            self._r_on.setChecked(True)
        else:
            self._r_off.setChecked(True)
        self._grp.blockSignals(False)

        self._grp.buttonClicked.connect(self._on_toggle)
        self.setStyleSheet("QWidget { background: transparent; } QRadioButton { color: #dcdcdc; }")

    def _on_toggle(self):
        val = "ON" if self._r_on.isChecked() else "OFF"
        try:
            db.update_field(self._isin, self._field, val)
        except Exception as e:
            QMessageBox.critical(None, "Ошибка БД", str(e))


class SpinWidget(QWidget):
    def __init__(self, isin: str, field: str, value: int = 0, parent=None):
        super().__init__(parent)
        self._isin = isin
        self._field = field

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 0)

        self._spin = QSpinBox()
        self._spin.setRange(0, 9_999_999)
        self._spin.setValue(int(value or 0))
        self._spin.setButtonSymbols(QSpinBox.NoButtons)
        self._spin.setStyleSheet("QSpinBox { background: transparent; color: #dcdcdc; border: none; }")
        self._spin.editingFinished.connect(self._on_changed)
        layout.addWidget(self._spin)
        self.setStyleSheet("background: transparent;")

    def _on_changed(self):
        try:
            db.update_field(self._isin, self._field, self._spin.value())
        except Exception as e:
            QMessageBox.critical(None, "Ошибка БД", str(e))


class DoubleSpinWidget(QWidget):
    def __init__(self, isin: str, field: str, value: float = 0.0, parent=None):
        super().__init__(parent)
        self._isin = isin
        self._field = field

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 0)

        self._spin = QDoubleSpinBox()
        self._spin.setRange(0.0, 9_999_999.0)
        self._spin.setDecimals(4)
        self._spin.setValue(float(value or 0.0))
        self._spin.setButtonSymbols(QDoubleSpinBox.NoButtons)
        self._spin.setStyleSheet("QDoubleSpinBox { background: transparent; color: #dcdcdc; border: none; }")
        self._spin.editingFinished.connect(self._on_changed)
        layout.addWidget(self._spin)
        self.setStyleSheet("background: transparent;")

    def _on_changed(self):
        try:
            db.update_field(self._isin, self._field, self._spin.value())
        except Exception as e:
            QMessageBox.critical(None, "Ошибка БД", str(e))


class IntervalWidget(QWidget):
    def __init__(self, isin: str, value: str = "10:00-23:50", parent=None):
        super().__init__(parent)
        self._isin = isin

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 0)

        self._edit = QLineEdit(value)
        self._edit.setPlaceholderText("10:00-23:50")
        self._edit.editingFinished.connect(self._on_changed)
        layout.addWidget(self._edit)

        self.setStyleSheet("background: transparent;")
        self.refresh_color()

    def _parse_interval(self, text: str):
        text = (text or "").strip()
        try:
            left, right = text.split("-", 1)
            start = datetime.strptime(left.strip(), "%H:%M").time()
            end = datetime.strptime(right.strip(), "%H:%M").time()
            return start, end
        except Exception:
            return None, None

    def _is_now_in_interval(self) -> bool:
        start, end = self._parse_interval(self._edit.text())
        if start is None or end is None:
            return False

        now_t = datetime.now().time().replace(second=0, microsecond=0)
        if start <= end:
            return start <= now_t <= end
        return now_t >= start or now_t <= end

    def refresh_color(self):
        start, end = self._parse_interval(self._edit.text().strip())
        if start is None or end is None:
            self._edit.setStyleSheet(
                "QLineEdit { background: #4a1f1f; color: #ffd7d7; border: 1px solid #aa4444; border-radius: 4px; padding: 2px 6px; }"
            )
            self._edit.setToolTip("Неверный формат. Используй HH:MM-HH:MM")
            return

        if self._is_now_in_interval():
            self._edit.setStyleSheet(
                "QLineEdit { background: #1f4a2a; color: #d8ffe1; border: 1px solid #3fa55b; border-radius: 4px; padding: 2px 6px; }"
            )
            self._edit.setToolTip("Сейчас внутри торгового интервала")
        else:
            self._edit.setStyleSheet(
                "QLineEdit { background: #4a1f1f; color: #ffd7d7; border: 1px solid #c25555; border-radius: 4px; padding: 2px 6px; }"
            )
            self._edit.setToolTip("Сейчас вне торгового интервала")

    def _on_changed(self):
        try:
            db.update_field(self._isin, "trade_interval", self._edit.text().strip())
            self.refresh_color()
        except Exception as e:
            QMessageBox.critical(None, "Ошибка БД", str(e))


class ComboWidget(QWidget):
    def __init__(self, isin: str, field: str, values=None, current: str = "", parent=None):
        super().__init__(parent)
        self._isin = isin
        self._field = field
        self._values = list(values or [])

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 0)

        self._combo = QComboBox()
        self._combo.setStyleSheet(
            "QComboBox { background: #2b2b2b; color: #dcdcdc; border: 1px solid #555; border-radius: 3px; padding: 2px 6px; }"
            "QComboBox::drop-down { border: none; }"
            "QComboBox QAbstractItemView { background: #2b2b2b; color: #dcdcdc; selection-background-color: #3a6ea8; }"
        )
        self._combo.currentTextChanged.connect(self._on_changed)
        layout.addWidget(self._combo)
        self.setStyleSheet("background: transparent;")

        self.refresh_values(self._values, current)

    def refresh_values(self, values, current: str = None):
        self._values = list(values or [])
        if current is None:
            current = self._combo.currentText()

        self._combo.blockSignals(True)
        self._combo.clear()
        self._combo.addItem("")
        for val in self._values:
            self._combo.addItem(str(val))
        idx = self._combo.findText(current)
        self._combo.setCurrentIndex(idx if idx >= 0 else 0)
        self._combo.blockSignals(False)

    def _on_changed(self, text: str):
        try:
            db.update_field(self._isin, self._field, text)
        except Exception as e:
            QMessageBox.critical(None, "Ошибка БД", str(e))


class TelegramListDialog(QDialog):
    def __init__(self, title: str, placeholder: str, fetch_fn, insert_fn, delete_fn, parent=None):
        super().__init__(parent)
        self._fetch = fetch_fn
        self._insert = insert_fn
        self._delete = delete_fn

        self.setWindowTitle(title)
        self.setMinimumWidth(420)
        self.setMinimumHeight(320)
        self.setModal(True)

        root = QVBoxLayout(self)
        root.setSpacing(8)
        root.setContentsMargins(16, 16, 16, 16)

        self._list = QListWidget()
        self._list.setStyleSheet(
            "QListWidget { background: #1e1e1e; color: #dcdcdc; border: 1px solid #444; font-size: 13px; }"
            "QListWidget::item:selected { background: #3a6ea8; color: white; }"
        )
        root.addWidget(self._list)

        row = QHBoxLayout()
        self._edit = QLineEdit()
        self._edit.setPlaceholderText(placeholder)
        self._edit.setStyleSheet(
            "QLineEdit { background:#2b2b2b; color:#dcdcdc; border:1px solid #555; border-radius:3px; padding:4px 6px; }"
        )
        self._edit.returnPressed.connect(self._on_add)

        btn_add = QPushButton("+ Добавить")
        btn_add.setStyleSheet(
            "QPushButton { background:#4a9d5e; color:white; border:none; border-radius:4px; padding:5px 12px; font-weight:bold; }"
            "QPushButton:hover { background:#5ab870; }"
        )
        btn_add.clicked.connect(self._on_add)
        row.addWidget(self._edit)
        row.addWidget(btn_add)
        root.addLayout(row)

        btn_del = QPushButton("— Удалить выбранное")
        btn_del.setStyleSheet(
            "QPushButton { background:#9d4a4a; color:white; border:none; border-radius:4px; padding:5px 12px; font-weight:bold; }"
            "QPushButton:hover { background:#b85a5a; }"
        )
        btn_del.clicked.connect(self._on_delete)
        root.addWidget(btn_del)

        btn_close = QPushButton("Закрыть")
        btn_close.setStyleSheet(
            "QPushButton { background:#444; color:#dcdcdc; border:none; border-radius:4px; padding:5px 12px; }"
            "QPushButton:hover { background:#555; }"
        )
        btn_close.clicked.connect(self.accept)
        root.addWidget(btn_close)

        self._refresh()

    def _refresh(self):
        self._list.clear()
        for val in self._fetch():
            self._list.addItem(QListWidgetItem(str(val)))

    def _on_add(self):
        val = self._edit.text().strip()
        if not val:
            QMessageBox.warning(self, "Ошибка", "Поле не может быть пустым.")
            return
        ok = self._insert(val)
        if not ok:
            QMessageBox.warning(self, "Дубликат", f"«{val}» уже существует.")
            return
        self._edit.clear()
        self._refresh()

    def _on_delete(self):
        item = self._list.currentItem()
        if not item:
            QMessageBox.warning(self, "Ошибка", "Выберите запись для удаления.")
            return
        val = item.text()
        reply = QMessageBox.question(self, "Подтверждение", f"Удалить «{val}»?", QMessageBox.Yes | QMessageBox.No)
        if reply != QMessageBox.Yes:
            return
        self._delete(val)
        self._refresh()


class ProxyDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Прокси")
        self.setMinimumWidth(480)
        self.setMinimumHeight(380)
        self.setModal(True)

        root = QVBoxLayout(self)
        root.setSpacing(8)
        root.setContentsMargins(16, 16, 16, 16)

        self._list = QListWidget()
        self._list.setStyleSheet(
            "QListWidget { background: #1e1e1e; color: #dcdcdc; border: 1px solid #444; font-size: 12px; font-family: Consolas, monospace; }"
            "QListWidget::item:selected { background: #3a6ea8; color: white; }"
            "QListWidget::item { padding: 4px; }"
        )
        self._list.itemClicked.connect(self._on_select)
        root.addWidget(self._list)

        form_box = QWidget()
        form_box.setStyleSheet("background: #252525; border-radius: 4px;")
        form = QFormLayout(form_box)
        form.setContentsMargins(10, 8, 10, 8)
        form.setSpacing(6)

        self._ed_host = QLineEdit(); self._ed_host.setPlaceholderText("45.130.131.49")
        self._ed_port = QSpinBox(); self._ed_port.setRange(1, 65535); self._ed_port.setValue(8000)
        self._ed_user = QLineEdit(); self._ed_user.setPlaceholderText("username (необязательно)")
        self._ed_pass = QLineEdit(); self._ed_pass.setPlaceholderText("password (необязательно)")
        self._ed_pass.setEchoMode(QLineEdit.Password)

        field_style = "QLineEdit,QSpinBox { background:#2b2b2b; color:#dcdcdc; border:1px solid #555; border-radius:3px; padding:4px 6px; }"
        for w in (self._ed_host, self._ed_port, self._ed_user, self._ed_pass):
            w.setStyleSheet(field_style)

        form.addRow("Host:", self._ed_host)
        form.addRow("Port:", self._ed_port)
        form.addRow("User:", self._ed_user)
        form.addRow("Pass:", self._ed_pass)
        root.addWidget(form_box)

        btn_row = QHBoxLayout()

        btn_add = QPushButton("+ Добавить")
        btn_add.setStyleSheet(
            "QPushButton{background:#4a9d5e;color:white;border:none;border-radius:4px;padding:5px 12px;font-weight:bold;}"
            "QPushButton:hover{background:#5ab870;}"
        )
        btn_add.clicked.connect(self._on_add)

        btn_del = QPushButton("— Удалить")
        btn_del.setStyleSheet(
            "QPushButton{background:#9d4a4a;color:white;border:none;border-radius:4px;padding:5px 12px;font-weight:bold;}"
            "QPushButton:hover{background:#b85a5a;}"
        )
        btn_del.clicked.connect(self._on_delete)

        self._btn_activate = QPushButton("✔ Применить выбранный")
        self._btn_activate.setStyleSheet(
            "QPushButton{background:#2e6da4;color:white;border:none;border-radius:4px;padding:5px 12px;font-weight:bold;}"
            "QPushButton:hover{background:#3a85c8;}"
        )
        self._btn_activate.clicked.connect(self._on_activate)

        btn_close = QPushButton("Закрыть")
        btn_close.setStyleSheet(
            "QPushButton{background:#444;color:#dcdcdc;border:none;border-radius:4px;padding:5px 12px;}"
            "QPushButton:hover{background:#555;}"
        )
        btn_close.clicked.connect(self.accept)

        btn_row.addWidget(btn_add)
        btn_row.addWidget(btn_del)
        btn_row.addWidget(self._btn_activate)
        btn_row.addStretch()
        btn_row.addWidget(btn_close)
        root.addLayout(btn_row)

        self._proxy_ids = []
        self._refresh()

    def _refresh(self):
        self._list.clear()
        self._proxy_ids = []
        proxies = db.fetch_proxies()
        for p in proxies:
            active = " ✔ АКТИВЕН" if p["is_active"] else ""
            user_part = f"  [{p['username']}]" if p["username"] else ""
            label = f"{p['host']}:{p['port']}{user_part}{active}"
            item = QListWidgetItem(label)
            if p["is_active"]:
                item.setForeground(Qt.green)
            self._list.addItem(item)
            self._proxy_ids.append(p["id"])

    def _selected_id(self):
        idx = self._list.currentRow()
        if idx < 0 or idx >= len(self._proxy_ids):
            return None
        return self._proxy_ids[idx]

    def _on_select(self):
        pass

    def _on_add(self):
        host = self._ed_host.text().strip()
        port = self._ed_port.value()
        if not host:
            QMessageBox.warning(self, "Ошибка", "Введите host прокси.")
            return
        db.insert_proxy(host, port, self._ed_user.text(), self._ed_pass.text())
        self._ed_host.clear(); self._ed_user.clear(); self._ed_pass.clear()
        self._refresh()

    def _on_delete(self):
        pid = self._selected_id()
        if pid is None:
            QMessageBox.warning(self, "Ошибка", "Выберите прокси для удаления.")
            return
        reply = QMessageBox.question(self, "Удалить?", "Удалить выбранный прокси?", QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            db.delete_proxy(pid)
            self._refresh()

    def _on_activate(self):
        pid = self._selected_id()
        if pid is None:
            QMessageBox.warning(self, "Ошибка", "Выберите прокси.")
            return
        db.set_active_proxy(pid)
        self._refresh()
        QMessageBox.information(self, "Готово", "Прокси применён.\nВступит в силу при следующем запуске робота.")


class AddInstrumentDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Добавить инструмент")
        self.setMinimumWidth(360)
        self.setModal(True)

        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)

        title = QLabel("Новый инструмент")
        title.setFont(QFont("Segoe UI", 11, QFont.Bold))
        layout.addWidget(title)

        form = QFormLayout()
        form.setSpacing(8)
        self.ed_name = QLineEdit(); self.ed_name.setPlaceholderText("Сбербанк")
        self.ed_isin = QLineEdit(); self.ed_isin.setPlaceholderText("RU0009029540")
        self.ed_board = QLineEdit(); self.ed_board.setPlaceholderText("TQBR")
        form.addRow("Название:", self.ed_name)
        form.addRow("ISIN:", self.ed_isin)
        form.addRow("Board:", self.ed_board)
        layout.addLayout(form)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.button(QDialogButtonBox.Ok).setText("Добавить")
        btns.button(QDialogButtonBox.Cancel).setText("Отмена")
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _on_accept(self):
        for ed, label in ((self.ed_name, "Название"), (self.ed_isin, "ISIN"), (self.ed_board, "Board")):
            if not ed.text().strip():
                QMessageBox.warning(self, "Ошибка", f"Введите {label}.")
                ed.setFocus()
                return
        self.accept()

    def get_name(self):
        return self.ed_name.text().strip()

    def get_isin(self):
        return self.ed_isin.text().strip()

    def get_board(self):
        return self.ed_board.text().strip()


class InitialLoadWorker(QObject):
    finished = pyqtSignal(dict)
    failed = pyqtSignal(str)

    def run(self):
        try:
            db.init_db()
            payload = {
                "rows": db.fetch_all_instruments(),
                "tgapi_values": db.fetch_tgapi(),
                "tgchat_values": db.fetch_tgchat(),
                "account_values": db.fetch_accounts(),
                "client_code_values": db.fetch_client_codes(),
                "decay": db.fetch_decay(),
                "tg_enabled": db.fetch_tg_enabled(),
            }
            self.finished.emit(payload)
        except Exception as e:
            self.failed.emit(str(e))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Робот — Панель управления")
        self.resize(1400, 600)
        self.setWindowIcon(get_icon())

        self._robot_process = None
        self._loading = False
        self._load_thread = None
        self._load_worker = None
        self._log_pos = 0

        self._tgapi_values = []
        self._tgchat_values = []
        self._account_values = []
        self._client_code_values = []

        self._build_toolbar()
        self._build_central()
        self._build_timers()
        self._set_loading_state(True)

        QTimer.singleShot(0, self._start_initial_load)

    def _build_timers(self):
        self._poll_timer = QTimer(self)
        self._poll_timer.setInterval(1000)
        self._poll_timer.timeout.connect(self._check_robot)
        self._poll_timer.start()

        self._readonly_timer = QTimer(self)
        self._readonly_timer.setInterval(3000)
        self._readonly_timer.timeout.connect(self._refresh_readonly_cells)

        self._trade_interval_timer = QTimer(self)
        self._trade_interval_timer.setInterval(3000)
        self._trade_interval_timer.timeout.connect(self._refresh_trade_interval_colors)

        self._log_timer = QTimer(self)
        self._log_timer.setInterval(1000)
        self._log_timer.timeout.connect(self._read_log)
        self._log_timer.start()

    def _set_loading_state(self, state: bool):
        self._loading = state
        self.table.setEnabled(not state)
        self._btn_robot.setEnabled(not state)
        if state:
            self.statusBar().showMessage("Загрузка GUI и данных из БД...")
        else:
            self.statusBar().showMessage("Готово", 3000)

    def _start_initial_load(self):
        self._load_thread = QThread(self)
        self._load_worker = InitialLoadWorker()
        self._load_worker.moveToThread(self._load_thread)

        self._load_thread.started.connect(self._load_worker.run)
        self._load_worker.finished.connect(self._on_initial_loaded)
        self._load_worker.failed.connect(self._on_initial_failed)
        self._load_worker.finished.connect(self._load_thread.quit)
        self._load_worker.failed.connect(self._load_thread.quit)
        self._load_worker.finished.connect(self._load_worker.deleteLater)
        self._load_worker.failed.connect(self._load_worker.deleteLater)
        self._load_thread.finished.connect(self._load_thread.deleteLater)

        self._load_thread.start()

    def _on_initial_loaded(self, payload: dict):
        self._tgapi_values = list(payload.get("tgapi_values", []))
        self._tgchat_values = list(payload.get("tgchat_values", []))
        self._account_values = list(payload.get("account_values", []))
        self._client_code_values = list(payload.get("client_code_values", []))

        self._spin_decay.blockSignals(True)
        self._spin_decay.setValue(float(payload.get("decay", 1.0) or 1.0))
        self._spin_decay.blockSignals(False)

        self._grp_tg.blockSignals(True)
        if bool(payload.get("tg_enabled", False)):
            self._rb_tg_yes.setChecked(True)
        else:
            self._rb_tg_no.setChecked(True)
        self._grp_tg.blockSignals(False)

        self._populate_table(payload.get("rows", []))
        self._readonly_timer.start()
        self._trade_interval_timer.start()
        self._set_loading_state(False)

    def _on_initial_failed(self, error_text: str):
        self._set_loading_state(False)
        QMessageBox.critical(self, "Ошибка загрузки", error_text)

    def _populate_table(self, rows):
        self.table.setUpdatesEnabled(False)
        try:
            self.table.setRowCount(0)
            for rec in rows:
                self._add_row(rec)
        finally:
            self.table.setUpdatesEnabled(True)
            self.table.viewport().update()

    def _reload_combo_source(self, col: int):
        try:
            if col == 13:
                self._tgapi_values = db.fetch_tgapi()
                return self._tgapi_values
            if col == 14:
                self._tgchat_values = db.fetch_tgchat()
                return self._tgchat_values
            if col == 15:
                self._account_values = db.fetch_accounts()
                return self._account_values
            if col == 16:
                self._client_code_values = db.fetch_client_codes()
                return self._client_code_values
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Не удалось перечитать список:\n{e}")
        return []

    def _build_toolbar(self):
        tb = QToolBar()
        tb.setMovable(False)
        tb.setStyleSheet(
            "QToolBar { background:#2b2b2b; border-bottom:1px solid #444; padding:4px 8px; spacing:6px; }"
            "QPushButton#btn_add { background:#4a9d5e; color:white; border:none; border-radius:4px; padding:5px 14px; font-weight:bold; }"
            "QPushButton#btn_add:hover { background:#5ab870; }"
            "QPushButton#btn_add:pressed { background:#3a8050; }"
            "QPushButton#btn_del { background:#9d4a4a; color:white; border:none; border-radius:4px; padding:5px 14px; font-weight:bold; }"
            "QPushButton#btn_del:hover { background:#b85a5a; }"
            "QPushButton#btn_del:pressed { background:#803a3a; }"
            "QPushButton#btn_tg { background:#2e6da4; color:white; border:none; border-radius:4px; padding:5px 14px; font-weight:bold; }"
            "QPushButton#btn_tg:hover { background:#3a85c8; }"
            "QPushButton#btn_tg:pressed { background:#22518a; }"
            "QPushButton#btn_acc { background:#8a6a2e; color:white; border:none; border-radius:4px; padding:5px 14px; font-weight:bold; }"
            "QPushButton#btn_acc:hover { background:#a8822e; }"
            "QPushButton#btn_acc:pressed { background:#6a5020; }"
            "QPushButton#btn_proxy { background:#1a5276; color:white; border:none; border-radius:4px; padding:5px 14px; font-weight:bold; }"
            "QPushButton#btn_proxy:hover { background:#2471a3; }"
            "QPushButton#btn_proxy:pressed { background:#154360; }"
            "QPushButton#btn_robot_off { background:#3a3a3a; color:#aaaaaa; border:1px solid #555; border-radius:4px; padding:5px 14px; font-weight:bold; }"
            "QPushButton#btn_robot_off:hover { background:#4a4a4a; color:white; }"
            "QPushButton#btn_robot_on { background:#c0392b; color:white; border:none; border-radius:4px; padding:5px 14px; font-weight:bold; }"
            "QPushButton#btn_robot_on:hover { background:#e74c3c; }"
        )
        self.addToolBar(tb)

        btn_add = QPushButton("+ Добавить"); btn_add.setObjectName("btn_add")
        btn_add.clicked.connect(self.on_add_clicked)
        tb.addWidget(btn_add)

        btn_del = QPushButton("— Удалить"); btn_del.setObjectName("btn_del")
        btn_del.clicked.connect(self.on_delete_clicked)
        tb.addWidget(btn_del)

        btn_tgapi = QPushButton("🔑 API Telegram"); btn_tgapi.setObjectName("btn_tg")
        btn_tgapi.clicked.connect(self.on_tgapi_clicked)
        tb.addWidget(btn_tgapi)

        btn_tgchat = QPushButton("💬 Chat ID Telegram"); btn_tgchat.setObjectName("btn_tg")
        btn_tgchat.clicked.connect(self.on_tgchat_clicked)
        tb.addWidget(btn_tgchat)

        btn_acc = QPushButton("👤 Аккаунты"); btn_acc.setObjectName("btn_acc")
        btn_acc.clicked.connect(self.on_accounts_clicked)
        tb.addWidget(btn_acc)

        btn_cc = QPushButton("🏷 Счета"); btn_cc.setObjectName("btn_acc")
        btn_cc.clicked.connect(self.on_client_codes_clicked)
        tb.addWidget(btn_cc)

        btn_proxy = QPushButton("🌐 Прокси"); btn_proxy.setObjectName("btn_proxy")
        btn_proxy.clicked.connect(self.on_proxy_clicked)
        tb.addWidget(btn_proxy)

        lbl_tg = QLabel("  Отправка ТГ:")
        lbl_tg.setStyleSheet("color: #aaaaaa; font-size: 12px;")
        tb.addWidget(lbl_tg)

        self._grp_tg = QButtonGroup(self)
        self._rb_tg_yes = QRadioButton("Да")
        self._rb_tg_no = QRadioButton("Нет")
        for rb in (self._rb_tg_yes, self._rb_tg_no):
            rb.setStyleSheet("QRadioButton { color: #dcdcdc; font-size: 12px; }")
        self._grp_tg.addButton(self._rb_tg_yes, 1)
        self._grp_tg.addButton(self._rb_tg_no, 0)
        self._rb_tg_no.setChecked(True)
        self._grp_tg.buttonClicked.connect(self._on_tg_enabled_changed)
        tb.addWidget(self._rb_tg_yes)
        tb.addWidget(self._rb_tg_no)

        lbl_decay = QLabel("  Задержка (сек):")
        lbl_decay.setStyleSheet("color: #aaaaaa; font-size: 12px;")
        tb.addWidget(lbl_decay)

        self._spin_decay = QDoubleSpinBox()
        self._spin_decay.setRange(0.0, 9999.0)
        self._spin_decay.setSingleStep(0.5)
        self._spin_decay.setDecimals(1)
        self._spin_decay.setValue(1.0)
        self._spin_decay.setFixedWidth(70)
        self._spin_decay.setStyleSheet(
            "QDoubleSpinBox { background:#2b2b2b; color:#dcdcdc; border:1px solid #555; border-radius:3px; padding:3px 6px; font-size:12px; }"
            "QDoubleSpinBox::up-button, QDoubleSpinBox::down-button { width:0px; }"
        )
        self._spin_decay.editingFinished.connect(self._on_decay_changed)
        tb.addWidget(self._spin_decay)

        sep = QWidget(); sep.setFixedWidth(16); sep.setStyleSheet("background: transparent;")
        tb.addWidget(sep)

        self._lamp = QLabel("●")
        self._lamp.setFixedWidth(22)
        self._lamp.setAlignment(Qt.AlignCenter)
        self._lamp.setStyleSheet("color: #555555; font-size: 18px;")
        self._lamp.setToolTip("Робот остановлен")
        tb.addWidget(self._lamp)

        self._btn_robot = QPushButton("▶ Запустить робота")
        self._btn_robot.setObjectName("btn_robot_off")
        self._btn_robot.clicked.connect(self.on_robot_clicked)
        tb.addWidget(self._btn_robot)

    def _build_central(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(0)

        splitter = QSplitter(Qt.Horizontal)
        splitter.setHandleWidth(6)
        splitter.setStyleSheet("QSplitter::handle { background: #3a3a3a; }")
        root.addWidget(splitter)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self.table = QTableWidget(0, len(HEADERS))
        self.table.setHorizontalHeaderLabels(HEADERS)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setDefaultSectionSize(36)
        self.table.setStyleSheet(
            "QTableWidget { background:#1e1e1e; color:#dcdcdc; gridline-color:#3a3a3a; font-size:12px; }"
            "QHeaderView::section { background:#2b2b2b; color:#aaaaaa; padding:4px; border:none; border-bottom:1px solid #444; font-weight:bold; }"
            "QTableWidget::item:selected { background:#3a6ea8; color:white; }"
            "QTableWidget { alternate-background-color:#252525; }"
        )
        left_layout.addWidget(self.table)
        splitter.addWidget(left)

        right = QWidget()
        right.setMinimumWidth(260)
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(6, 0, 0, 0)
        right_layout.setSpacing(4)

        log_header = QLabel("📋 Логи робота")
        log_header.setStyleSheet("color:#aaaaaa; font-size:11px; font-weight:bold; padding:2px 0;")
        right_layout.addWidget(log_header)

        self._log_view = QPlainTextEdit()
        self._log_view.setReadOnly(True)
        self._log_view.document().setMaximumBlockCount(5000)
        self._log_view.setStyleSheet(
            "QPlainTextEdit { background: #141414; color: #b0b0b0; border: 1px solid #333; border-radius: 3px; font-family: Consolas, monospace; font-size: 11px; padding: 4px; }"
        )
        right_layout.addWidget(self._log_view)

        btn_clear = QPushButton("Очистить")
        btn_clear.setStyleSheet(
            "QPushButton { background:#2b2b2b; color:#888; border:1px solid #444; border-radius:3px; padding:3px 8px; font-size:11px; }"
            "QPushButton:hover { color:#dcdcdc; }"
        )
        btn_clear.clicked.connect(self._log_view.clear)
        right_layout.addWidget(btn_clear)

        splitter.addWidget(right)
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)

    def _add_row(self, r: dict):
        isin = r["isin"]
        row = self.table.rowCount()
        self.table.insertRow(row)

        def label(text, align=Qt.AlignCenter):
            it = QTableWidgetItem(str(text))
            it.setTextAlignment(align)
            it.setFlags(it.flags() & ~Qt.ItemIsEditable)
            return it

        self.table.setItem(row, 0, label(r["name"], Qt.AlignLeft | Qt.AlignVCenter))
        self.table.setCellWidget(row, 1, ToggleWidget(isin, "condition", r.get("condition", "OFF")))
        self.table.setCellWidget(row, 2, ToggleWidget(isin, "battle_regime", r.get("battle_regime", "OFF")))
        self.table.setCellWidget(row, 3, IntervalWidget(isin, r.get("trade_interval", "10:00-23:50")))
        self.table.setCellWidget(row, 4, SpinWidget(isin, "best_offer_qty", r.get("best_offer_qty", 0)))
        self.table.setCellWidget(row, 5, ToggleWidget(isin, "best_offer", r.get("best_offer", "OFF")))
        self.table.setCellWidget(row, 6, DoubleSpinWidget(isin, "best_offer_limit", r.get("best_offer_limit", 0)))
        self.table.setCellWidget(row, 7, DoubleSpinWidget(isin, "price_limit", r.get("price_limit", 0)))
        self.table.setCellWidget(row, 8, SpinWidget(isin, "bid_limit", r.get("bid_limit", 0)))
        self.table.setItem(row, 9, label(r.get("bid_curr", 0)))
        self.table.setCellWidget(row, 10, SpinWidget(isin, "trades_limit", r.get("trades_limit", 0)))
        self.table.setCellWidget(row, 11, TradesCurrWidget(isin, r.get("trades_curr", 0)))
        self.table.setCellWidget(row, 12, SpinWidget(isin, "big_bid_alert_qty", r.get("big_bid_alert_qty", 0)))
        self.table.setCellWidget(row, 13, ComboWidget(isin, "tgapi", self._tgapi_values, r.get("tgapi", "")))
        self.table.setCellWidget(row, 14, ComboWidget(isin, "tgchat", self._tgchat_values, r.get("tgchat", "")))
        self.table.setCellWidget(row, 15, ComboWidget(isin, "account", self._account_values, r.get("account", "")))
        self.table.setCellWidget(row, 16, ComboWidget(isin, "client_code", self._client_code_values, r.get("client_code", "")))

    def _refresh_combos(self, col: int):
        values = self._reload_combo_source(col)
        for row in range(self.table.rowCount()):
            w = self.table.cellWidget(row, col)
            if isinstance(w, ComboWidget):
                w.refresh_values(values)

    def on_tgapi_clicked(self):
        dlg = TelegramListDialog(
            title="API Telegram",
            placeholder="Вставьте токен бота (123456:ABC-DEF...)",
            fetch_fn=db.fetch_tgapi,
            insert_fn=db.insert_tgapi,
            delete_fn=db.delete_tgapi,
            parent=self,
        )
        dlg.exec_()
        self._refresh_combos(13)

    def on_tgchat_clicked(self):
        dlg = TelegramListDialog(
            title="Chat ID Telegram",
            placeholder="Например: -1001234567890",
            fetch_fn=db.fetch_tgchat,
            insert_fn=db.insert_tgchat,
            delete_fn=db.delete_tgchat,
            parent=self,
        )
        dlg.exec_()
        self._refresh_combos(14)

    def on_accounts_clicked(self):
        dlg = TelegramListDialog(
            title="Аккаунты",
            placeholder="Введите аккаунт",
            fetch_fn=db.fetch_accounts,
            insert_fn=db.insert_account,
            delete_fn=db.delete_account,
            parent=self,
        )
        dlg.exec_()
        self._refresh_combos(15)

    def on_client_codes_clicked(self):
        dlg = TelegramListDialog(
            title="Коды клиентов (Счета)",
            placeholder="Введите код клиента",
            fetch_fn=db.fetch_client_codes,
            insert_fn=db.insert_client_code,
            delete_fn=db.delete_client_code,
            parent=self,
        )
        dlg.exec_()
        self._refresh_combos(16)

    def on_proxy_clicked(self):
        ProxyDialog(self).exec_()

    def on_robot_clicked(self):
        if self._robot_process is None or self._robot_process.poll() is not None:
            self._start_robot()
        else:
            self._stop_robot()

    def _start_robot(self):
        flag = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stop.flag")
        if os.path.exists(flag):
            os.remove(flag)
        robot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "robot.py")
        kwargs = {}
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        self._robot_process = subprocess.Popen([sys.executable, robot_path], **kwargs)
        self._log_pos = 0
        self._log_view.clear()
        self._set_lamp(True)

    def _stop_robot(self):
        flag = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stop.flag")
        try:
            with open(flag, "w", encoding="utf-8") as f:
                f.write("stop")
        except Exception:
            pass
        self._set_lamp(False)

    def _check_robot(self):
        if self._robot_process is not None and self._robot_process.poll() is not None:
            self._robot_process = None
            self._set_lamp(False)

    def _set_lamp(self, running: bool):
        if running:
            self._lamp.setStyleSheet("color: #2ecc71; font-size: 18px;")
            self._lamp.setToolTip("Робот запущен")
            self._btn_robot.setText("⏹ Остановить робота")
            self._btn_robot.setObjectName("btn_robot_on")
        else:
            self._lamp.setStyleSheet("color: #555555; font-size: 18px;")
            self._lamp.setToolTip("Робот остановлен")
            self._btn_robot.setText("▶ Запустить робота")
            self._btn_robot.setObjectName("btn_robot_off")
        self._btn_robot.style().unpolish(self._btn_robot)
        self._btn_robot.style().polish(self._btn_robot)

    def _on_decay_changed(self):
        try:
            db.update_decay(self._spin_decay.value())
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Не удалось сохранить задержку:\n{e}")

    def _on_tg_enabled_changed(self):
        try:
            db.update_tg_enabled(self._rb_tg_yes.isChecked())
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Не удалось сохранить:\n{e}")

    def _refresh_trade_interval_colors(self):
        if self._loading:
            return
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, 3)
            if hasattr(w, "refresh_color"):
                w.refresh_color()

    def _refresh_readonly_cells(self):
        if self._loading:
            return
        try:
            rows = db.fetch_all_instruments()
        except Exception:
            return

        isin_to_row = {}
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, 1)
            if hasattr(w, "_isin"):
                isin_to_row[w._isin] = r

        for rec in rows:
            isin = rec["isin"]
            r = isin_to_row.get(isin)
            if r is None:
                continue

            item9 = self.table.item(r, 9)
            val9 = str(rec.get("bid_curr", 0))
            if item9:
                item9.setText(val9)
            else:
                it = QTableWidgetItem(val9)
                it.setTextAlignment(Qt.AlignCenter)
                it.setFlags(it.flags() & ~Qt.ItemIsEditable)
                self.table.setItem(r, 9, it)

            w11 = self.table.cellWidget(r, 11)
            val11 = rec.get("trades_curr", 0)
            if hasattr(w11, "set_value"):
                w11.set_value(val11)
            else:
                self.table.setCellWidget(r, 11, TradesCurrWidget(isin, val11))

    def _read_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "robot.log")
        if not os.path.exists(log_path):
            return
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                f.seek(self._log_pos)
                new_text = f.read()
                self._log_pos = f.tell()
            if new_text:
                self._log_view.moveCursor(self._log_view.textCursor().End)
                self._log_view.insertPlainText(new_text)
                sb = self._log_view.verticalScrollBar()
                sb.setValue(sb.maximum())
        except Exception:
            pass

    def on_add_clicked(self):
        dlg = AddInstrumentDialog(self)
        if dlg.exec_() != QDialog.Accepted:
            return

        name, isin, board = dlg.get_name(), dlg.get_isin(), dlg.get_board()
        try:
            ok = db.insert_instrument(name, isin, board)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка БД", str(e))
            return

        if not ok:
            QMessageBox.warning(self, "Дубликат", f"Инструмент «{name}» с ISIN «{isin}» уже существует.")
            return

        try:
            rows = db.fetch_all_instruments()
            rec = next((r for r in rows if r["isin"] == isin), None)
            if rec:
                self.table.setUpdatesEnabled(False)
                try:
                    self._add_row(rec)
                finally:
                    self.table.setUpdatesEnabled(True)
                    self.table.viewport().update()
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Инструмент добавлен, но не удалось обновить таблицу:\n{e}")

    def on_delete_clicked(self):
        isin, ok = QInputDialog.getText(self, "Удалить инструмент", "Введите ISIN:")
        if not ok or not isin.strip():
            return
        isin = isin.strip()

        row_to_delete = None
        for row in range(self.table.rowCount()):
            w = self.table.cellWidget(row, 1)
            if isinstance(w, ToggleWidget) and w._isin == isin:
                row_to_delete = row
                break

        if row_to_delete is None:
            QMessageBox.warning(self, "Не найден", f"ISIN «{isin}» не найден.")
            return

        name_item = self.table.item(row_to_delete, 0)
        name = name_item.text() if name_item else isin
        reply = QMessageBox.question(self, "Подтверждение", f"Удалить «{name}» ({isin})?", QMessageBox.Yes | QMessageBox.No)
        if reply != QMessageBox.Yes:
            return

        try:
            db.delete_instrument(isin)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка БД", str(e))
            return

        self.table.removeRow(row_to_delete)

    def closeEvent(self, event):
        self._poll_timer.stop()
        self._log_timer.stop()
        self._readonly_timer.stop()
        self._trade_interval_timer.stop()

        self._stop_robot()
        if self._robot_process and self._robot_process.poll() is None:
            try:
                self._robot_process.terminate()
            except Exception:
                pass

        super().closeEvent(event)


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = MainWindow()
    win.show()
    return app.exec_() if hasattr(app, "exec_") else app.exec()


if __name__ == "__main__":
    sys.exit(main())
