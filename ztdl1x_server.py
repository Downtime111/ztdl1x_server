"""
ZTDL-1X TCP Server
==================
独立的多客户端 TCP 服务器，接收并解析 ZTDL-1X 协议数据。
协议与 full/models/ZTDL_1X_1_Util.py 完全一致，通过 TCP 网络替代串口连接。

功能：
- 异步多客户端并发接收
- ZTDL-1X 协议完整解析（DATA/PARAM/SOK/SER/HT/HD/HOK/HSTOP/TDF）
- SQLite 本地存储（按设备分表）
- 向客户端下发指令（set_time / start_output / 自定义指令）
- 自动对时（每日 23:53:10）
- 历史数据自动导出（每日 01:20:10）

用法：
    python ztdl1x_server.py                             # 默认监听 10101 10102 10103
    python ztdl1x_server.py --host 127.0.0.1            # 指定监听地址
    python ztdl1x_server.py --ports 10101 10102 10103   # 指定端口列表
    python ztdl1x_server.py --db data/ztdl.db           # 指定数据库路径
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import signal
import socket
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta
from typing import Optional

# ─────────────────────────── 日志配置 ───────────────────────────

import logging.handlers

LOG_FMT = logging.Formatter("%(levelname)s: %(asctime)s - %(message)s")
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("ztdl1x_server")
logger.setLevel(logging.INFO)

# 控制台
_ch = logging.StreamHandler()
_ch.setFormatter(LOG_FMT)
logger.addHandler(_ch)

# 滚动文件：10MB × 5 个备份
_fh = logging.handlers.RotatingFileHandler(
    os.path.join(LOG_DIR, "ztdl1x_server.log"),
    maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8",
)
_fh.setFormatter(LOG_FMT)
logger.addHandler(_fh)

# 默认表头定义（设备未返回 table_def 时使用）
DEFAULT_TDF = [
    "dt", "An_1", "An_2", "An_3", "An_4",
    "CumAn_1", "CumAn_2", "CumAn_3", "CumAn_4",
    "BTemp_1", "BTemp_2", "BTemp_3", "BTemp_4",
    "ATemp", "AHumi", "APress", "WS", "WD", "Precip", "CO2",
    "SR_1", "DTemp_1", "DHumi_1", "AngleX_1", "AngleY_1",
    "SR_2", "DTemp_2", "DHumi_2", "AngleX_2", "AngleY_2",
    "SR_3", "DTemp_3", "DHumi_3", "AngleX_3", "AngleY_3",
    "WBTemp",
]


# ─────────────────────────── 协议解析 ───────────────────────────

def judge_msg_type(msg: str) -> int:
    """判断消息类型，与 ZTDL_1X_1_Util.judge_msg_type 一致"""
    if not msg:
        return 0
    if re.match(r"^\"\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}(,(-)?(\s)*(\d)+(\.)?(\d)+|,(\s)*)+0x(.{2})\"", msg):
        return 1  # DATA
    elif re.match(r"^{(\"[a-zA-Z0-9_]+\":\s?\"?[a-zA-Z0-9_\x2e\x2d]+\"?,?\s?)+}", msg):
        return 2  # PARAM
    elif re.match("OK.+", msg):
        return 3  # SOK
    elif re.match("ER.+", msg):
        return 4  # SER
    elif re.match(r"^HT [0-9]{5}", msg):
        return 5  # HISTORY TOTAL COUNT
    elif re.match(
            r"^HD [0-9]{5}#\"\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}(,(-)?(\s)*(\d)+(\.)?(\d)+|,(\s)*)+0x(.{2})\"",
            msg):
        return 6  # HISTORY DATA
    elif re.match(r"^HOK \d{4}\d{1,2}\d{1,2}", msg):
        return 7  # HISTORY DATA OUTPUT DONE
    elif re.match(r"^HSTOP.+", msg):
        return 8  # OUTPUT STOP
    elif re.match(r"^#([a-zA-Z0-9]+(_\d+)?,)*[a-zA-Z0-9]+(_\d+)?#", msg):
        return 9  # TABLE DEFINITION
    return 0


def parser_data(dev: str, raw_line: str) -> Optional[list]:
    """
    解析一行 ZTDL-1X 协议数据。
    返回解析后的数据列表（仅 DATA 和 HISTORY DATA 类型），其他类型返回 None。
    """
    line = raw_line.strip("\r\n")
    msg_type = judge_msg_type(line)

    if msg_type == 1:  # DATA
        parts = line.strip("\"").split(',')[:-1]
        parts[0] = f"'{parts[0]}'"
        return parts

    elif msg_type == 2:  # PARAM
        try:
            payload = json.loads(line)
            logger.info(f"[{dev}] PARAM: {payload}")
        except json.JSONDecodeError:
            logger.warning(f"[{dev}] PARAM 解析失败: {line}")

    elif msg_type == 3:  # SOK
        logger.info(f"[{dev}] SOK: {line}")

    elif msg_type == 4:  # SER
        logger.warning(f"[{dev}] SER: {line}")

    elif msg_type == 5:  # HT - 历史数据总量
        try:
            total = int(line[3:8])
            hdt = f"{line[10:14]}{line[15:17]}{line[18:20]}"
            logger.info(f"[{dev}] HT: date={hdt}, total={total}")
        except (IndexError, ValueError) as e:
            logger.warning(f"[{dev}] HT 解析失败: {line} ({e})")

    elif msg_type == 6:  # HD - 历史数据行
        try:
            parts = line[9:].strip("\"").split(',')[:-1]
            parts[0] = f"'{parts[0]}'"
            return parts
        except (IndexError, ValueError) as e:
            logger.warning(f"[{dev}] HD 解析失败: {line} ({e})")

    elif msg_type == 7:  # HOK - 历史数据输出完成
        try:
            payload = line[4:12]
            logger.info(f"[{dev}] HOK: {payload}")
        except IndexError:
            logger.warning(f"[{dev}] HOK 解析失败: {line}")

    elif msg_type == 8:  # HSTOP
        logger.info(f"[{dev}] HSTOP: 导出终止")

    elif msg_type == 9:  # TDF - 表定义
        table_def = line.strip('#').split(',')
        logger.info(f"[{dev}] TDF: {table_def}")

    else:
        logger.debug(f"[{dev}] 未识别消息: {line}")

    return None


# ─────────────────────────── SQLite 存储（分库分表）───────────────────────────

class DataStore:
    """
    分库分表存储：
      - 设备信息表：_devices.db → devices
      - 数据分库：{base}/ztdl1x_{port}_{YYYY}.db（一端口一年一个库）
      - 数据分表：dev_{name}_{MM}（一设备一月一张表）
    """

    def __init__(self, db_path: str):
        self.base_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
        os.makedirs(self.base_dir, exist_ok=True)
        self._dev_db = os.path.join(self.base_dir, "_devices.db")
        self._dev_year_dbs: dict[tuple[str, int], sqlite3.Connection] = {}
        self._known_tables: set[tuple[str, int, str]] = set()  # (dev, year, table)
        self._last_device_update: dict[str, float] = {}
        self._table_defs: dict[str, list[str]] = {}  # 设备名 → TDF 字段名列表
        self._init_dev_db()
        # 启动时清理上次未正常关闭留下的 WAL 文件
        self._checkpoint_dev_db()
        self._load_table_defs()

    # ── 设备信息库 ──

    def _init_dev_db(self):
        try:
            with sqlite3.connect(self._dev_db) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=5000")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS devices (
                        name TEXT PRIMARY KEY,
                        port INTEGER,
                        first_seen TEXT,
                        last_seen TEXT,
                        last_param TEXT
                    )
                """)
                # 兼容旧库：追加 table_def 列
                cols = {c[1] for c in conn.execute("PRAGMA table_info(devices)").fetchall()}
                if 'table_def' not in cols:
                    conn.execute("ALTER TABLE devices ADD COLUMN table_def TEXT")
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"设备信息库初始化失败: {self._dev_db} ({e})")
            raise

    def _checkpoint_dev_db(self):
        try:
            with sqlite3.connect(self._dev_db) as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error as e:
            logger.warning(f"设备信息库 checkpoint 失败: {e}")

    def _load_table_defs(self):
        """启动时从 devices 表恢复所有已持久化的 TDF。"""
        try:
            with sqlite3.connect(self._dev_db) as conn:
                rows = conn.execute(
                    "SELECT name, table_def FROM devices WHERE table_def IS NOT NULL"
                ).fetchall()
            for name, td_json in rows:
                try:
                    self._table_defs[name] = json.loads(td_json)
                except json.JSONDecodeError:
                    pass
            if self._table_defs:
                logger.info(f"已恢复 {len(self._table_defs)} 个设备的表头定义")
        except sqlite3.Error as e:
            logger.warning(f"加载表头定义失败: {e}")

    def update_device(self, dev: str, port: int = 0, param: Optional[str] = None):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with sqlite3.connect(self._dev_db) as conn:
                conn.execute("""
                    INSERT INTO devices (name, port, first_seen, last_seen, last_param)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        port = excluded.port,
                        last_seen = excluded.last_seen,
                        last_param = COALESCE(excluded.last_param, devices.last_param)
                """, (dev, port, now, now, param))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"更新设备信息失败: {dev} ({e})")

    def get_device_list(self) -> list:
        try:
            with sqlite3.connect(self._dev_db) as conn:
                rows = conn.execute(
                    "SELECT name, port, last_seen FROM devices ORDER BY last_seen DESC"
                ).fetchall()
            return rows
        except sqlite3.Error as e:
            logger.error(f"查询设备列表失败: {e}")
            return []

    def delete_device(self, name: str):
        try:
            with sqlite3.connect(self._dev_db) as conn:
                conn.execute("DELETE FROM devices WHERE name = ?", (name,))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"删除设备记录失败: {name} ({e})")

    def get_dev_type(self, dev: str) -> str:
        """从设备信息中提取 PN 作为设备类型，回退到 ztdl1x"""
        try:
            with sqlite3.connect(self._dev_db) as conn:
                row = conn.execute(
                    "SELECT last_param FROM devices WHERE name = ?", (dev,)
                ).fetchone()
            if row and row[0]:
                params = json.loads(row[0])
                pn = params.get("PN", "")
                if pn:
                    return pn.replace("-", "_")
        except (sqlite3.Error, json.JSONDecodeError):
            pass
        return "ztdl1x"

    # ── 数据分库 ──

    def _add_columns(self, conn: sqlite3.Connection, table: str, fields: list[str], from_index: int = 0):
        """对已有表追加字段列，跳过已存在的列。首列为 TEXT，其余为 REAL。"""
        existing = {c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        for i, f in enumerate(fields):
            safe = self._sanitize(f)
            if safe not in existing:
                col_type = "TEXT" if i == from_index else "REAL"
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {safe} {col_type}")

    def set_table_def(self, dev: str, fields: list[str]):
        """保存表定义（内存 + 持久化到 devices 表），若字段名有变化则对所有已有月表追加新列。"""
        old_fields = self._table_defs.get(dev)
        self._table_defs[dev] = fields
        # 持久化
        try:
            with sqlite3.connect(self._dev_db) as conn:
                conn.execute("""
                    INSERT INTO devices (name, table_def) VALUES (?, ?)
                    ON CONFLICT(name) DO UPDATE SET table_def = excluded.table_def
                """, (dev, json.dumps(fields)))
                conn.commit()
        except sqlite3.Error as e:
            logger.warning(f"[{dev}] TDF 持久化失败: {e}")
        if not old_fields:
            logger.info(f"[{dev}] TDF 已记录: {fields}")
            return
        if old_fields == fields:
            logger.info(f"[{dev}] TDF 无变化 ({len(fields)} 字段)")
            return
        old_set = set(old_fields)
        new_fields = [f for f in fields if f not in old_set]
        if not new_fields:
            return
        logger.info(f"[{dev}] 字段扩充: +{new_fields}")
        for (d, year), conn in self._dev_year_dbs.items():
            if d != dev:
                continue
            for table in self.list_tables(dev, year):
                try:
                    self._add_columns(conn, table, new_fields, from_index=fields.index(new_fields[0]))
                except sqlite3.Error as e:
                    logger.warning(f"[{dev}] 追加列失败: {table} ({e})")

    def _get_conn(self, dev: str, year: int) -> sqlite3.Connection:
        key = (dev, year)
        if key not in self._dev_year_dbs:
            dev_type = self.get_dev_type(dev)
            path = os.path.join(self.base_dir, f"{dev_type}_{self._sanitize(dev)}_{year}.db")
            try:
                conn = sqlite3.connect(path)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=5000")
                self._dev_year_dbs[key] = conn
            except sqlite3.Error as e:
                logger.error(f"数据库连接失败: {path} ({e})")
                raise
        return self._dev_year_dbs[key]

    def _sanitize(self, name: str) -> str:
        """将设备名中的非法字符替换为下划线，保证表名合法"""
        return re.sub(r'[^a-zA-Z0-9_]', '_', name)

    def _ensure_table(self, conn: sqlite3.Connection, table: str, dev: str, year: int):
        cache_key = (dev, year, table)
        if cache_key in self._known_tables:
            return
        fields = self._table_defs.get(dev)
        if fields:
            safe_fields = [self._sanitize(f) for f in fields]
            ts_col = safe_fields[0]
            data_cols = ", ".join(f"{f} REAL" for f in safe_fields[1:])
            col_defs = f"{ts_col} TEXT PRIMARY KEY"
            if data_cols:
                col_defs += f", {data_cols}"
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    {col_defs},
                    _received_at TEXT NOT NULL,
                    _port INTEGER
                )
            """)
        else:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at TEXT NOT NULL,
                    data TEXT NOT NULL,
                    port INTEGER
                )
            """)
        self._known_tables.add(cache_key)

    def _has_data_column(self, conn: sqlite3.Connection, table: str) -> bool:
        """检测表是否为旧格式（含 data 列）。"""
        cols = {c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        return "data" in cols

    def _upgrade_table(self, conn: sqlite3.Connection, table: str, dev: str):
        """将旧格式表升级：追加 TDF 字段列，删除冗余 data 列。"""
        fields = self._table_defs.get(dev)
        if not fields:
            return
        self._add_columns(conn, table, fields)
        try:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN data")
        except sqlite3.OperationalError:
            pass  # SQLite < 3.35 不支持 DROP COLUMN，data 列保留（后续写入填空字符串）

    def _is_columnar(self, conn: sqlite3.Connection, table: str, dev: str) -> bool:
        """检查表是否已包含 TDF 首列（即已升级为列式）。"""
        fields = self._table_defs.get(dev)
        if not fields:
            return False
        ts_col = self._sanitize(fields[0])
        cols = {c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        return ts_col in cols

    def save_data(self, dev: str, port: int, parsed_data: list):
        now = datetime.now()
        year = now.year
        month = now.strftime("%m")
        table = f"dev_{month}"
        conn = self._get_conn(dev, year)
        try:
            self._ensure_table(conn, table, dev, year)
            fields = self._table_defs.get(dev)
            # 若旧格式表且有 TDF，先升级追加列
            if fields and self._has_data_column(conn, table):
                self._upgrade_table(conn, table, dev)
            if self._is_columnar(conn, table, dev):
                safe_fields = [self._sanitize(f) for f in fields]
                values = {}
                for i, val in enumerate(parsed_data):
                    if i < len(safe_fields):
                        clean = str(val).strip("'\"")
                        values[safe_fields[i]] = clean if clean else None
                if not values:
                    return
                # 系统列（升级后 data 列已删除；旧 SQLite 回退时仍需填充）
                existing_cols = {c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()}
                sys_cols = []
                sys_vals = []
                now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                for col in ('_received_at', 'received_at'):
                    if col in existing_cols:
                        sys_cols.append(col)
                        sys_vals.append(now_str)
                for col in ('_port', 'port'):
                    if col in existing_cols:
                        sys_cols.append(col)
                        sys_vals.append(port)
                if 'data' in existing_cols:
                    sys_cols.append('data')
                    sys_vals.append('')
                columns = list(values.keys()) + sys_cols
                placeholders = ", ".join("?" * len(columns))
                col_names = ", ".join(columns)
                row_vals = list(values.values()) + sys_vals
                conn.execute(
                    f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})",
                    row_vals
                )
            else:
                conn.execute(
                    f"INSERT INTO {table} (received_at, data, port) VALUES (?, ?, ?)",
                    (now.strftime("%Y-%m-%d %H:%M:%S"),
                     ",".join(str(x) for x in parsed_data),
                     port)
                )
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"写入数据失败: {dev}/{year}-{month} ({e})")
        # 设备信息每分钟最多更新一次（与数据写入独立，数据写入失败不影响设备信息）
        ts = time.monotonic()
        if ts - self._last_device_update.get(dev, 0) > 60:
            self.update_device(dev, port)
            self._last_device_update[dev] = ts

    # ── 查询 ──

    def query_data(self, dev: str, year: int, month: int, limit: int = 100) -> list:
        table = f"dev_{month:02d}"
        conn = self._get_conn(dev, year)
        try:
            cols = [c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            if "data" in cols and not self._is_columnar(conn, table, dev):
                rows = conn.execute(
                    f"SELECT id, received_at, data, port FROM {table} "
                    f"ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            else:
                fields = self._table_defs.get(dev)
                ts_col = self._sanitize(fields[0]) if fields else "dt"
                rows = conn.execute(
                    f"SELECT rowid, * FROM {table} "
                    f"ORDER BY REPLACE({ts_col}, char(39), '') DESC LIMIT ?", (limit,)
                ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning(f"查询失败 {dev}/{year}-{month:02d}: {e}")
            rows = []
        return rows

    def list_tables(self, dev: str, year: int) -> list:
        conn = self._get_conn(dev, year)
        try:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'dev_%' ORDER BY name"
            ).fetchall()
            return [r[0] for r in rows]
        except sqlite3.Error as e:
            logger.warning(f"查询表列表失败: dev={dev} year={year} ({e})")
            return []

    def migrate_data(self, old_dev: str, new_dev: str):
        """设备识别后，将临时库数据迁移到 SN 库（兼容旧格式 dev_SN_month）"""
        now = datetime.now()
        year = now.year
        old_key = (old_dev, year)

        # 关闭旧库连接
        old_conn = self._dev_year_dbs.pop(old_key, None)
        if old_conn is None:
            return
        old_conn.commit()
        old_conn.close()

        old_path = os.path.join(self.base_dir, f"ztdl1x_{self._sanitize(old_dev)}_{year}.db")
        sanitized_new = self._sanitize(new_dev)

        # 重新以独立连接打开旧库（避免干扰缓存连接）
        migrated = 0
        try:
            conn = sqlite3.connect(old_path)
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'dev_%'"
            ).fetchall()
            if tables:
                new_conn = self._get_conn(new_dev, year)
                fields = self._table_defs.get(new_dev)
                for (table_name,) in tables:
                    rows = conn.execute(f"SELECT received_at, data, port FROM {table_name}").fetchall()
                    if not rows:
                        continue
                    # dev_SN_month → dev_month
                    if table_name.startswith(f"dev_{sanitized_new}_"):
                        target_table = f"dev_{table_name.split('_')[-1]}"
                    else:
                        target_table = table_name
                    self._ensure_table(new_conn, target_table, new_dev, year)
                    if fields and self._is_columnar(new_conn, target_table, new_dev):
                        # 目标表是列式：拆分旧 data 文本映射到 TDF 列
                        safe_fields = [self._sanitize(f) for f in fields]
                        existing_cols = {c[1] for c in new_conn.execute(
                            f"PRAGMA table_info({target_table})").fetchall()}
                        for recv_at, data_str, port_val in rows:
                            parts = data_str.split(',')
                            values = {}
                            for i, val in enumerate(parts):
                                if i < len(safe_fields):
                                    clean = val.strip("'\"")
                                    values[safe_fields[i]] = clean if clean else None
                            if not values:
                                continue
                            sys_cols, sys_vals = [], []
                            for col in ('_received_at', 'received_at'):
                                if col in existing_cols:
                                    sys_cols.append(col)
                                    sys_vals.append(recv_at)
                            for col in ('_port', 'port'):
                                if col in existing_cols:
                                    sys_cols.append(col)
                                    sys_vals.append(port_val)
                            if 'data' in existing_cols:
                                sys_cols.append('data')
                                sys_vals.append(data_str)
                            columns = list(values.keys()) + sys_cols
                            placeholders = ", ".join("?" * len(columns))
                            new_conn.execute(
                                f"INSERT OR REPLACE INTO {target_table} ({', '.join(columns)}) "
                                f"VALUES ({placeholders})",
                                list(values.values()) + sys_vals
                            )
                            migrated += 1
                    else:
                        new_conn.executemany(
                            f"INSERT INTO {target_table} (received_at, data, port) VALUES (?, ?, ?)",
                            rows
                        )
                        migrated += len(rows)
                    new_conn.commit()
                    logger.info(f"[迁移] {old_dev} → {new_dev}: {table_name} → {target_table} ({migrated} 条)")
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"数据迁移失败: {old_dev} → {new_dev} ({e})")
            return

        # 删除旧库文件（无论是否有数据）
        try:
            os.remove(old_path)
            for suffix in ("-wal", "-shm"):
                p = old_path + suffix
                if os.path.exists(p):
                    os.remove(p)
            if migrated:
                logger.info(f"[迁移] 已删除临时库: {old_path}")
            else:
                logger.info(f"[迁移] 旧库无数据，已删除: {old_path}")
        except OSError as e:
            logger.warning(f"[迁移] 删除临时库失败: {old_path} ({e})")

    def checkpoint(self):
        """对所有已打开的数据库执行 WAL checkpoint，收缩 -wal 文件"""
        for (dev, year), conn in self._dev_year_dbs.items():
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.Error as e:
                logger.warning(f"checkpoint 失败: dev={dev} year={year} ({e})")
        # 设备信息库也做 checkpoint
        try:
            with sqlite3.connect(self._dev_db) as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error as e:
            logger.warning(f"checkpoint 失败: devices ({e})")

    def export_csv(self, export_dir: str, dev: Optional[str] = None,
                   target_date: Optional[str] = None) -> dict:
        """导出数据为 CSV：{export_dir}/{SN}_{YYYYMMDD}.csv，跳过值全为空的列。"""
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            return {"ok": False, "error": f"日期格式错误: {target_date} (需 YYYY-MM-DD)"}
        year = int(target_date[:4])
        month = int(target_date[5:7])
        os.makedirs(export_dir, exist_ok=True)
        files = []
        results = []
        devs = [dev] if dev else list(self._table_defs.keys())
        if dev and dev not in self._table_defs:
            return {"ok": False, "error": f"设备 {dev} 无表头定义"}

        for d in devs:
            fields = self._table_defs.get(d)
            if not fields:
                results.append({"dev": d, "rows": 0, "error": "无表头"})
                continue
            # 扫描目标月 + 下一月（跨月延迟写入的数据可能在下一月表中）
            all_rows = []
            for m_offset in (0, 1):
                m = month + m_offset
                y = year
                if m > 12:
                    m = 1
                    y += 1
                rows_block = self._query_export_rows(d, y, m, target_date)
                if rows_block:
                    all_rows.extend(rows_block)
            if not all_rows:
                results.append({"dev": d, "rows": 0})
                continue

            # 按 dt 正序
            ts_col = self._sanitize(fields[0])
            all_rows.sort(key=lambda r: str(r[0]).strip("'\""))
            # 过滤空列
            conn = self._get_conn(d, year)
            table = f"dev_{month:02d}"
            self._ensure_table(conn, table, d, year)
            cols = [c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            col_map = {c: i for i, c in enumerate(cols)}
            keep_indices = []
            for f in fields:
                safe = self._sanitize(f)
                idx = col_map.get(safe)
                if idx is None:
                    continue
                if any(idx < len(r) and r[idx] is not None and str(r[idx]).strip()
                       for r in all_rows):
                    keep_indices.append((safe, idx))
            if not keep_indices:
                results.append({"dev": d, "rows": len(all_rows), "error": "所有列为空"})
                continue

            path = os.path.join(export_dir, f"{d}_{target_date.replace('-', '')}.csv")
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([name for name, _ in keep_indices])
                for row in all_rows:
                    w.writerow([str(row[idx]) if idx < len(row) and row[idx] is not None else ""
                                for _, idx in keep_indices])
            files.append(path)
            results.append({"dev": d, "rows": len(all_rows), "file": path})
            logger.info(f"[export] {d} → {path} ({len(all_rows)} 条)")

        exported_count = sum(1 for r in results if r.get("file"))
        if exported_count:
            logger.info(f"[export] 完成: {exported_count} 个设备, {target_date}")
        return {"ok": True, "exported": exported_count, "date": target_date,
                "files": files, "results": results}

    def _query_export_rows(self, dev: str, year: int, month: int, target_date: str) -> list:
        """查询指定年月中匹配 target_date 的数据行。"""
        table = f"dev_{month:02d}"
        fields = self._table_defs.get(dev)
        if not fields:
            return []
        try:
            conn = self._get_conn(dev, year)
        except sqlite3.Error:
            return []
        self._ensure_table(conn, table, dev, year)
        cols = [c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if not cols or not self._is_columnar(conn, table, dev):
            return []
        ts_col = self._sanitize(fields[0])
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE {ts_col} LIKE ? "
            f"ORDER BY REPLACE({ts_col}, char(39), '') ASC",
            (f"{target_date}%",)
        ).fetchall()
        return list(rows)

    def close(self):
        # 关闭前先 checkpoint，确保数据落盘
        self.checkpoint()
        for conn in self._dev_year_dbs.values():
            try:
                conn.close()
            except sqlite3.Error:
                pass
        self._dev_year_dbs.clear()
        self._known_tables.clear()
        self._last_device_update.clear()
        self._table_defs.clear()


# ─────────────────────────── 客户端会话 ───────────────────────────

class ClientSession:
    """管理单个客户端连接"""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 store: DataStore, server: 'ZTDL1XServer', listen_port: int):
        self.reader = reader
        self.writer = writer
        self.store = store
        self.server = server
        self.listen_port = listen_port
        self.dev_name: Optional[str] = None
        self.addr = writer.get_extra_info('peername')
        self._running = True
        self.ut: int = 5  # 上传间隔（秒），默认5，从 PARAM 更新
        self.last_data_time: float = time.monotonic()  # 最后收到数据的时间
        self._last_response: Optional[dict] = None  # 最近一次指令响应
        self._response_event = asyncio.Event()  # 响应到达通知
        self._query_info_sent: bool = False  # 是否已发送过 query_info
        self._table_def_sent: bool = False  # 是否已发送过 table_def

    @property
    def client_id(self) -> str:
        return self.dev_name or f"{self.addr[0]}:{self.addr[1]}"

    @property
    def log_id(self) -> str:
        """日志显示用：SN@IP 或 IP:端口"""
        ip = self.addr[0]
        return f"{self.dev_name}@{ip}" if self.dev_name else f"{ip}:{self.addr[1]}"

    @staticmethod
    def _expand_magic(command: str) -> str:
        """替换指令中的魔法值为实际时间"""
        if "{now+2s}" in command:
            real_time = (datetime.now() + timedelta(seconds=2)).strftime('%Y-%m-%d %H:%M:%S')
            command = command.replace("{now+2s}", real_time)
        return command

    async def send_command(self, command: str):
        """向客户端发送指令（发送前替换魔法值）"""
        try:
            command = self._expand_magic(command)
            self.writer.write((command + "\n").encode())
            await self.writer.drain()
            logger.info(f"[{self.log_id}] >>> 指令已发送: {command}")
            # query_info 后会收到 PARAM，需要重新请求表头
            if command == "query_info":
                self._table_def_sent = False
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            logger.error(f"[{self.log_id}] 发送失败: {e}")

    def _enable_keepalive(self):
        """启用 TCP keepalive"""
        transport = self.writer.transport
        sock = transport.get_extra_info('socket')
        if sock is None:
            return
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except (OSError, AttributeError):
            pass  # Windows 部分参数不支持

    async def run(self):
        """主循环：持续读取客户端数据"""
        self._enable_keepalive()
        logger.info(f"[{self.log_id}] 客户端已连接 (端口:{self.listen_port} 来源:{self.addr[0]}:{self.addr[1]})")
        # 立即以 IP:端口 注册，保证 broadcast 能覆盖到
        self.server.register_device(self.client_id, self)
        await self.send_command("query_info")
        try:
            while self._running:
                raw = await self.reader.readline()
                if not raw:
                    logger.info(f"[{self.log_id}] 客户端关闭连接")
                    break
                line = raw.decode(errors='replace').strip()
                if not line:
                    continue
                await self._process_line(line)
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            logger.info(f"[{self.log_id}] 连接异常断开: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            self._cleanup()

    async def _process_line(self, line: str):
        """处理一行原始数据"""
        msg_type = judge_msg_type(line)

        # 指令响应（PARAM/SOK/SER/HT/HOK/HSTOP）记录供管理端查询
        if msg_type in (2, 3, 4, 5, 7, 8):
            resp_text = line
            if msg_type == 5:  # HT: 提取 total 和 date
                try:
                    total = int(line[3:8])
                    hdt = f"{line[10:14]}-{line[15:17]}-{line[18:20]}"
                    resp_text = f"total={total} date={hdt}"
                except (IndexError, ValueError):
                    pass
            self._last_response = {"type": msg_type, "data": resp_text}
            self._response_event.set()

        # 尝试从 PARAM 消息中提取设备名（SN 字段）
        if msg_type == 2:
            try:
                params = json.loads(line)
                dev_id = params.get("SN") or params.get("name")
                if dev_id:
                    old_id = self.client_id
                    self.dev_name = dev_id
                    if old_id != self.dev_name:
                        # 移除旧注册，改用设备名注册
                        self.server.unregister_device(old_id, self)
                        self.store.delete_device(old_id)
                    logger.info(f"[{self.log_id}] 识别设备名: {self.dev_name}")
                    self.server.register_device(self.client_id, self)
                if self.dev_name:
                    # 先保存 PARAM（含 PN），迁移时 get_dev_type 需要读取
                    self.store.update_device(self.dev_name, port=self.listen_port, param=line)
                if "UT" in params:
                    self.ut = int(params["UT"])
                # 先设 TDF（迁移需要字段映射），再迁移数据，最后请求设备真实表头
                if self.dev_name and not self._table_def_sent:
                    self._table_def_sent = True
                    if self.dev_name not in self.store._table_defs:
                        self.store.set_table_def(self.dev_name, DEFAULT_TDF)
                    asyncio.create_task(self.send_command("table_def"))
                if dev_id and old_id != self.dev_name:
                    self.store.migrate_data(old_id, self.dev_name)
            except json.JSONDecodeError:
                pass

        # SOK 响应后重新读取配置（设备参数可能已变更）
        if msg_type == 3 and self.dev_name:
            asyncio.create_task(self.send_command("query_info"))

        # TDF 表定义：存储字段列表
        if msg_type == 9 and self.dev_name:
            table_def = line.strip('#').split(',')
            self.store.set_table_def(self.dev_name, table_def)

        # 更新最后收数时间
        self.last_data_time = time.monotonic()

        # 解析数据
        try:
            parsed = parser_data(self.log_id, line)
        except Exception as e:
            logger.error(f"[{self.log_id}] 协议解析异常: {line} ({e})")
            parsed = None
        if parsed:
            self.store.save_data(self.client_id, self.listen_port, parsed)
            self.server._on_device_active(self.client_id, self)
            label = "HD" if msg_type == 6 else "DATA"
            logger.debug(f"[{self.log_id}] {label}: {','.join(str(x) for x in parsed)}")

    def _cleanup(self):
        self._running = False
        self.server.unregister_device(self.client_id, self)
        try:
            self.writer.close()
        except Exception:
            pass
        logger.info(f"[{self.log_id}] 会话已清理")


# ─────────────────────────── 管理协议处理器 ───────────────────────────

class AdminHandler:
    """处理单个管理端连接，JSON-line 协议"""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 server: 'ZTDL1XServer'):
        self.reader = reader
        self.writer = writer
        self.server = server
        self.addr = writer.get_extra_info('peername')

    async def run(self):
        logger.info(f"[admin] 管理端连接: {self.addr[0]}:{self.addr[1]}")
        try:
            while True:
                raw = await self.reader.readline()
                if not raw:
                    break
                try:
                    request = json.loads(raw.decode())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    await self._reply({"ok": False, "error": "invalid json"})
                    continue
                response = await self._dispatch(request)
                await self._reply(response)
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            try:
                self.writer.close()
            except Exception:
                pass
            logger.info(f"[admin] 管理端断开: {self.addr[0]}:{self.addr[1]}")

    async def _reply(self, data: dict):
        self.writer.write((json.dumps(data, ensure_ascii=False, default=str) + "\n").encode())
        await self.writer.drain()

    async def _broadcast_and_sample(self, command: str, timeout: float = 3.0) -> tuple[int, Optional[str]]:
        """广播指令并采样所有设备的响应。返回 (设备数, 响应文本或None)。"""
        count = len(self.server.devices)
        if not count:
            return 0, None
        # 清除所有设备的响应事件
        for session in self.server.devices.values():
            try:
                session._last_response = None
                session._response_event.clear()
            except AttributeError:
                pass
        await self.server.broadcast_command(command)
        # 并发等所有设备响应
        responses = []
        tasks = []
        for name, session in self.server.devices.items():
            tasks.append(asyncio.create_task(self._wait_one(name, session, timeout)))
        await asyncio.wait(tasks, timeout=timeout)
        for t in tasks:
            if not t.done():
                t.cancel()
            elif t.result():
                responses.append(t.result())
        if responses:
            return count, "\n".join(responses)
        return count, None

    async def _wait_one(self, name: str, session, timeout: float) -> Optional[str]:
        try:
            await asyncio.wait_for(session._response_event.wait(), timeout=timeout)
            resp = session._last_response
            if resp:
                return f"[{name}] {resp['data']}"
        except (asyncio.TimeoutError, AttributeError):
            pass
        return None

    async def _dispatch(self, req: dict) -> dict:
        cmd = req.get("cmd", "")
        try:
            if cmd == "list":
                return self._handle_list()
            elif cmd == "tables":
                return self._handle_tables(req)
            elif cmd == "query":
                return self._handle_query(req)
            elif cmd == "send":
                return await self._handle_send(req)
            elif cmd == "broadcast":
                return await self._handle_broadcast(req)
            elif cmd == "pending":
                return self._handle_pending(req)
            elif cmd == "refresh":
                return await self._handle_refresh(req)
            elif cmd == "timeproof":
                return await self._handle_timeproof()
            elif cmd == "hisdata":
                return await self._handle_hisdata(req)
            elif cmd == "export":
                return self._handle_export(req)
            elif cmd in ("shutdown", "stop"):
                return await self._handle_shutdown()
            elif cmd == "help":
                return self._handle_help()
            else:
                return {"ok": False, "error": f"unknown command: {cmd}"}
        except Exception as e:
            logger.error(f"[admin] {cmd} 执行异常: {e}")
            return {"ok": False, "error": str(e)}

    def _handle_list(self) -> dict:
        devices = self.server.store.get_device_list()
        online = set(self.server.devices.keys())
        idle = self.server._idle
        result = []
        for name, port, last_seen in devices:
            if name in online:
                status = "IDLE" if name in idle else "ONLINE"
            else:
                status = "offline"
            result.append({"name": name, "port": port, "status": status, "last_seen": last_seen})
        return {"ok": True, "devices": result}

    def _handle_tables(self, req: dict) -> dict:
        dev = req.get("dev", "")
        year = req.get("year", datetime.now().year)
        if not dev:
            return {"ok": False, "error": "missing dev"}
        tables = self.server.store.list_tables(dev, year)
        return {"ok": True, "dev": dev, "year": year, "tables": tables}

    def _handle_query(self, req: dict) -> dict:
        dev = req.get("dev", "")
        year = req.get("year")
        month = req.get("month")
        limit = min(req.get("limit", 50), 500)
        if not dev or year is None or month is None:
            return {"ok": False, "error": "missing dev/year/month"}
        rows = self.server.store.query_data(dev, year, month, limit)
        fields = self.server.store._table_defs.get(dev)
        return {"ok": True, "dev": dev, "year": year, "month": month, "rows": rows, "fields": fields}

    async def _handle_send(self, req: dict) -> dict:
        dev = req.get("dev", "")
        command = req.get("command", "")
        if not dev or not command:
            return {"ok": False, "error": "missing dev/command"}
        resp = await self.server.send_and_wait(dev, command)
        if resp:
            return {"ok": True, "msg": f"{dev} 响应", "response": f"[{dev}] {resp['data']}"}
        return {"ok": True, "msg": f"sent to {dev}: {command} (未收到响应)"}

    async def _handle_broadcast(self, req: dict) -> dict:
        command = req.get("command", "")
        if not command:
            return {"ok": False, "error": "missing command"}
        count, resp = await self._broadcast_and_sample(command, timeout=2.0)
        if resp:
            return {"ok": True, "msg": f"broadcast to {count} device(s)", "response": resp}
        return {"ok": True, "msg": f"broadcast to {count} device(s): {command}"}

    def _handle_pending(self, req: dict) -> dict:
        if req.get("clear"):
            count = len(self.server._pending)
            self.server._pending.clear()
            return {"ok": True, "cleared": count}
        now = datetime.now()
        items = []
        for cmd_str, deadline, created in self.server._pending:
            remain = (deadline - now).total_seconds()
            items.append({
                "command": cmd_str,
                "created": created.strftime("%Y-%m-%d %H:%M:%S"),
                "remaining_seconds": max(0, remain),
            })
        return {"ok": True, "pending": items}

    async def _handle_refresh(self, req: dict) -> dict:
        dev = req.get("dev")
        if dev:
            resp = await self.server.send_and_wait(dev, "query_info", timeout=5.0)
            if resp:
                return {"ok": True, "msg": f"{dev} 配置", "response": f"[{dev}] {resp['data']}"}
            return {"ok": True, "msg": f"refresh sent to {dev} (未收到响应)"}
        count, resp = await self._broadcast_and_sample("query_info", timeout=5.0)
        if resp:
            return {"ok": True, "msg": f"refresh broadcast to {count} device(s)", "response": resp}
        return {"ok": True, "msg": f"refresh broadcast to {count} device(s)"}

    async def _handle_timeproof(self) -> dict:
        count, resp = await self._broadcast_and_sample("set_time {now+2s}")
        if resp:
            return {"ok": True, "msg": f"timeproof broadcast to {count} device(s)", "response": resp}
        return {"ok": True, "msg": f"timeproof broadcast to {count} device(s)"}

    async def _handle_hisdata(self, req: dict) -> dict:
        date_str = req.get("date", (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
        dev = req.get("dev")
        if dev:
            await self.server.send_to_device(dev, "stop_output")
            await asyncio.sleep(1)
            resp = await self.server.send_and_wait(dev, f"start_output {date_str}", timeout=3.0)
            if resp:
                return {"ok": True, "msg": f"hisdata sent to {dev} for {date_str}",
                        "response": f"[{dev}] {resp['data']}"}
            return {"ok": True, "msg": f"hisdata sent to {dev} for {date_str} (未收到响应)"}
        await self.server.broadcast_command("stop_output")
        await asyncio.sleep(1)
        count, resp = await self._broadcast_and_sample(f"start_output {date_str}")
        if resp:
            return {"ok": True, "msg": f"hisdata broadcast to {count} device(s) for {date_str}", "response": resp}
        return {"ok": True, "msg": f"hisdata broadcast to {count} device(s) for {date_str}"}

    async def _handle_shutdown(self) -> dict:
        logger.info("[admin] 收到关闭指令")
        asyncio.create_task(self.server.stop())
        return {"ok": True, "msg": "server shutting down"}

    def _handle_export(self, req: dict) -> dict:
        dev = req.get("dev")
        date_str = req.get("date")
        path = req.get("path") or self.server.csv_export_dir
        result = self.server.store.export_csv(path, dev=dev, target_date=date_str)
        return result

    def _handle_help(self) -> dict:
        return {"ok": True, "help": [
            "list                          - 查看所有设备",
            "tables <dev> [year]           - 查看设备数据表",
            "query <dev> <year> <month> [limit] - 查询数据",
            "send <dev> <command>          - 向指定设备发送指令",
            "broadcast <command>           - 广播指令",
            "pending [clear]               - 查看/清除待发指令",
            "refresh [dev]                 - 刷新设备配置",
            "timeproof                     - 手动对时",
            "hisdata [dev] [date]          - 历史数据导出",
            "export [dev] [date] [path]    - 导出 CSV",
            "shutdown                      - 关闭服务",
            "help                          - 显示帮助",
            "quit/exit                     - 退出客户端",
        ]}


# ─────────────────────────── 服务器主体 ───────────────────────────

class ZTDL1XServer:
    """ZTDL-1X TCP 服务器（多端口）"""

    MAX_RETRY_SECONDS = 3 * 3600  # 最大重试窗口：3小时

    def __init__(self, host: str, ports: list[int], store: DataStore,
                 admin_host: str = "127.0.0.1", admin_port: int = 10100,
                 pid_file: Optional[str] = None, csv_export_dir: str = "ztdl1x_data"):
        self.host = host
        self.ports = ports
        self.store = store
        self.admin_host = admin_host
        self.admin_port = admin_port
        self.pid_file = pid_file
        self.csv_export_dir = csv_export_dir
        self.devices: dict[str, ClientSession] = {}
        self._idle: set[str] = set()  # 超时未发数但连接仍在的设备
        self._servers: list[asyncio.AbstractServer] = []
        self._admin_server: Optional[asyncio.AbstractServer] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._pending_tasks: list[asyncio.Task] = []
        # 待发指令队列：(command, deadline, created_at)
        self._pending: list[tuple[str, datetime, datetime]] = []

    def register_device(self, name: str, session: ClientSession):
        # 处理设备重连：旧会话可能尚未清理，先主动注销
        old_session = self.devices.get(name)
        if old_session is not None and old_session is not session:
            logger.info(f"[{name}] 检测到重连，清理旧会话")
            old_session._running = False
            try:
                old_session.writer.close()
            except Exception:
                pass
        self.devices[name] = session
        logger.info(f"设备已注册: {name} (当前在线: {len(self.devices)})")
        # 设备上线，立即补发待发指令
        task = asyncio.create_task(self._flush_pending(name))
        self._pending_tasks.append(task)
        task.add_done_callback(self._pending_tasks.remove)

    def unregister_device(self, name: str, session: Optional[ClientSession] = None):
        # 只注销当前注册的会话，避免重连竞态下旧会话误删新会话
        if session is not None:
            if self.devices.get(name) is session:
                self.devices.pop(name, None)
                self._idle.discard(name)
                logger.info(f"设备注销: {name} (当前在线: {len(self.devices)})")
        else:
            self.devices.pop(name, None)
            self._idle.discard(name)
            logger.info(f"设备注销: {name} (当前在线: {len(self.devices)})")

    async def broadcast_command(self, command: str):
        """向所有在线设备广播指令，无在线设备则加入待发队列"""
        if not self.devices:
            now = datetime.now()
            deadline = now + timedelta(seconds=self.MAX_RETRY_SECONDS)
            # 去重：同一指令不重复入队
            if not any(c == command for c, _, _ in self._pending):
                self._pending.append((command, deadline, now))
                logger.warning(f"无在线设备，指令已入队列 (deadline {deadline.strftime('%Y-%m-%d %H:%M:%S')}): {command}")
            else:
                logger.info(f"指令已在队列中，跳过重复入队: {command}")
            return
        for name, session in list(self.devices.items()):
            await session.send_command(command)

    async def send_to_device(self, dev_name: str, command: str):
        """向指定设备发送指令"""
        session = self.devices.get(dev_name)
        if session:
            await session.send_command(command)
        else:
            logger.warning(f"设备 {dev_name} 不在线")

    async def send_and_wait(self, dev_name: str, command: str, timeout: float = 3.0) -> Optional[dict]:
        """向指定设备发送指令并等待响应，超时返回 None。"""
        session = self.devices.get(dev_name)
        if not session:
            return None
        session._last_response = None
        session._response_event.clear()
        await session.send_command(command)
        try:
            await asyncio.wait_for(session._response_event.wait(), timeout=timeout)
            resp = session._last_response
            if resp:
                resp["dev"] = dev_name
            return resp
        except asyncio.TimeoutError:
            return None

    async def _flush_pending(self, dev_name: str):
        """设备上线时，补发所有未过期待发指令，发送后从队列移除"""
        if not self._pending:
            return
        session = self.devices.get(dev_name)
        if not session:
            return
        sent_cmds = []
        for cmd, deadline, _ in list(self._pending):
            if datetime.now() < deadline:
                await session.send_command(cmd)
                sent_cmds.append(cmd)
        if sent_cmds:
            self._pending = [(c, d, t) for c, d, t in self._pending if c not in sent_cmds]
            logger.info(f"[{dev_name}] 补发了 {len(sent_cmds)} 条待发指令")

    def _cleanup_expired_pending(self):
        """清理过期待发指令"""
        now = datetime.now()
        before = len(self._pending)
        self._pending = [(c, d, t) for c, d, t in self._pending if now < d]
        expired = before - len(self._pending)
        if expired:
            logger.info(f"[pending] 清理 {expired} 条过期指令，剩余 {len(self._pending)} 条")

    async def _check_offline_devices(self):
        """检测超时：3.5 个上传周期未收到数据标记为空闲（不关闭连接）"""
        now = time.monotonic()
        for name, session in list(self.devices.items()):
            timeout = session.ut * 3.5
            if now - session.last_data_time > timeout:
                if name not in self._idle:
                    self._idle.add(name)
                    logger.warning(f"[{session.log_id}] 超时未发数，标记为空闲 ({timeout:.0f}s)")

    def _on_device_active(self, name: str, session: 'ClientSession'):
        """设备恢复发数时清除空闲标记，未识别的设备补发 query_info（仅一次）"""
        if name in self._idle:
            self._idle.discard(name)
            logger.info(f"[{session.log_id}] 恢复发数")
        if not session.dev_name and not session._query_info_sent:
            session._query_info_sent = True
            asyncio.create_task(session.send_command("query_info"))
        # 已识别但缺表头的设备，补发 table_def（仅一次）
        if session.dev_name and not session._table_def_sent:
            session._table_def_sent = True
            if session.dev_name not in self.store._table_defs:
                self.store.set_table_def(session.dev_name, DEFAULT_TDF)
            asyncio.create_task(session.send_command("table_def"))

    def _make_handler(self, listen_port: int):
        """为每个端口创建带端口信息的 handler 闭包"""
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            session = ClientSession(reader, writer, self.store, self, listen_port)
            await session.run()
        return handle

    def _make_admin_handler(self):
        """管理端口连接处理器"""
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            handler = AdminHandler(reader, writer, self)
            await handler.run()
        return handle

    def _write_pid_file(self):
        if not self.pid_file:
            return
        try:
            os.makedirs(os.path.dirname(self.pid_file) or ".", exist_ok=True)
            with open(self.pid_file, 'w') as f:
                f.write(str(os.getpid()))
            logger.info(f"PID 文件: {self.pid_file}")
        except OSError as e:
            logger.warning(f"PID 文件写入失败: {e}")

    def _remove_pid_file(self):
        if not self.pid_file:
            return
        try:
            os.remove(self.pid_file)
        except OSError:
            pass

    async def _scheduler(self):
        """定时任务：自动对时 & 历史数据导出"""
        last_timeproof_date = ""
        last_hisdata_date = ""
        last_csv_date = ""
        last_cleanup_minute = -1
        last_checkpoint_minute = -1
        offline_check_counter = 0

        while True:
            try:
                now = datetime.now()
                today = now.strftime("%Y-%m-%d")

                # 每日 23:53:10 自动对时
                if now.hour == 23 and now.minute == 53 and now.second >= 10 and now.second < 16:
                    if last_timeproof_date != today:
                        await self.broadcast_command("set_time {now+2s}")
                        last_timeproof_date = today
                        logger.info("[scheduler] 自动对时已执行")

                # 每日 01:20:10 自动导出昨日历史数据
                if now.hour == 1 and now.minute == 20 and now.second >= 10 and now.second < 16:
                    if last_hisdata_date != today:
                        yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                        await self.broadcast_command("stop_output")
                        await asyncio.sleep(1)
                        await self.broadcast_command(f"start_output {yesterday}")
                        last_hisdata_date = today
                        logger.info(f"[scheduler] 历史数据导出指令已发送: {yesterday}")

                # 每日 03:00:10 导出 CSV 数据文件
                if now.hour == 3 and now.minute == 0 and now.second >= 10 and now.second < 16:
                    if last_csv_date != today:
                        self.store.export_csv(self.csv_export_dir)
                        last_csv_date = today

                # 每分钟清理一次过期待发指令（每分钟只触发一次）
                current_minute = now.hour * 60 + now.minute
                if current_minute != last_cleanup_minute:
                    last_cleanup_minute = current_minute
                    self._cleanup_expired_pending()

                # 每 30 分钟执行一次 WAL checkpoint
                if current_minute % 30 == 0 and current_minute != last_checkpoint_minute:
                    last_checkpoint_minute = current_minute
                    self.store.checkpoint()

                # 每 3 秒检测一次离线
                offline_check_counter += 1
                if offline_check_counter >= 3:
                    offline_check_counter = 0
                    await self._check_offline_devices()

            except Exception as e:
                logger.error(f"[scheduler] 异常: {e}")

            await asyncio.sleep(1)

    async def start(self):
        """启动服务器，监听所有端口"""
        # 数据端口
        try:
            for port in self.ports:
                server = await asyncio.start_server(
                    self._make_handler(port), self.host, port
                )
                self._servers.append(server)
        except OSError as e:
            logger.error(f"端口绑定失败: {e}")
            for srv in self._servers:
                srv.close()
                await srv.wait_closed()
            self._servers.clear()
            raise

        # 管理端口
        if self.admin_port > 0:
            try:
                self._admin_server = await asyncio.start_server(
                    self._make_admin_handler(), self.admin_host, self.admin_port
                )
                logger.info(f"管理端口: {self.admin_host}:{self.admin_port}")
            except OSError as e:
                logger.warning(f"管理端口启动失败 ({e})，继续运行")

        self._write_pid_file()
        self._scheduler_task = asyncio.create_task(self._scheduler())

        addrs = []
        for srv in self._servers:
            addrs.extend(str(s.getsockname()) for s in srv.sockets)
        logger.info(f"ZTDL-1X Server 已启动，监听: {', '.join(addrs)}")
        logger.info(f"数据目录: {self.store.base_dir}")
        logger.info("等待客户端连接...")

        # serve_forever on all servers
        servers = list(self._servers)
        if self._admin_server:
            servers.append(self._admin_server)
        await asyncio.gather(*(srv.serve_forever() for srv in servers))

    async def stop(self):
        """停止服务器"""
        if self._scheduler_task:
            self._scheduler_task.cancel()
        for task in list(self._pending_tasks):
            task.cancel()
        for srv in self._servers:
            srv.close()
            await srv.wait_closed()
        self._servers.clear()
        if self._admin_server:
            self._admin_server.close()
            await self._admin_server.wait_closed()
            self._admin_server = None
        self.store.close()
        self._remove_pid_file()
        logger.info("服务器已停止")


# ─────────────────────────── 交互式控制台 ───────────────────────────

async def interactive_console(server: ZTDL1XServer):
    """交互式控制台，支持手动下发指令"""
    help_text = """
可用命令:
  list                           - 查看在线设备
  tables <dev> [year]            - 查看设备数据表 (默认当年)
  query <dev> <year> <month> [limit] - 查询设备数据 (默认50条)
  pending [clear]                - 查看待发指令队列 / 清除所有待发
  send <dev> <command>           - 向指定设备发送指令
  broadcast <command>            - 向所有设备广播指令
  refresh [dev]                  - 读取设备配置 (指定设备或全部)
  timeproof                      - 手动触发对时
  hisdata [date]                 - 手动触发历史数据导出 (默认昨日)
  help                           - 显示帮助
  quit / exit                    - 停止服务器
"""
    print(help_text)

    loop = asyncio.get_running_loop()
    while True:
        try:
            line = await loop.run_in_executor(None, lambda: input("ztdl1x> "))
            line = line.strip()
            if not line:
                continue

            parts = line.split(maxsplit=1)
            cmd = parts[0].lower()

            if cmd in ("quit", "exit"):
                await server.stop()
                break
            elif cmd == "help":
                print(help_text)
            elif cmd == "list":
                devices = server.store.get_device_list()
                if not devices:
                    print("  (无设备记录)")
                else:
                    online = set(server.devices.keys())
                    idle = server._idle
                    for name, port, last_seen in devices:
                        if name in online:
                            status = "IDLE" if name in idle else "ONLINE"
                        else:
                            status = "OFFLINE"
                        print(f"  [{status:>6}] {name}  port:{port}  (last: {last_seen})")
            elif cmd == "tables":
                if len(parts) < 2:
                    print("用法: tables <dev> [year]")
                    continue
                try:
                    targs = parts[1].split()
                    dev = targs[0]
                    year = int(targs[1]) if len(targs) > 1 else datetime.now().year
                except ValueError:
                    print("用法: tables <dev> [year]  (年份必须是数字)")
                    continue
                tables = server.store.list_tables(dev, year)
                if not tables:
                    print(f"  {dev} / {year} 年无数据表")
                else:
                    print(f"  {dev} / {year} 年数据表 ({len(tables)} 张):")
                    for t in tables:
                        print(f"    {t}")
            elif cmd == "query":
                if len(parts) < 2:
                    print("用法: query <dev> <year> <month> [limit]")
                    continue
                try:
                    qargs = parts[1].split()
                    if len(qargs) < 3:
                        print("用法: query <dev> <year> <month> [limit]")
                        continue
                    dev, year, month = qargs[0], int(qargs[1]), int(qargs[2])
                    limit = int(qargs[3]) if len(qargs) > 3 else 50
                except ValueError:
                    print("用法: query <dev> <year> <month> [limit]  (年/月/条数必须是数字)")
                    continue
                rows = server.store.query_data(dev, year, month, limit)
                if not rows:
                    print(f"  {dev} ({year}-{month:02d}) 无数据")
                else:
                    print(f"  {dev} ({year}-{month:02d}) 最新 {len(rows)} 条:")
                    for row in rows:
                        print(f"    [{row[0]}] {row[1]}  {row[2]}")
            elif cmd == "send":
                if len(parts) < 2:
                    print("用法: send <dev_name> <command>")
                    continue
                sub = parts[1].split(maxsplit=1)
                if len(sub) < 2:
                    print("用法: send <dev_name> <command>")
                    continue
                await server.send_to_device(sub[0], sub[1])
            elif cmd == "broadcast":
                if len(parts) < 2:
                    print("用法: broadcast <command>")
                    continue
                await server.broadcast_command(parts[1])
            elif cmd == "refresh":
                if len(parts) > 1:
                    # 指定设备
                    dev_name = parts[1]
                    await server.send_to_device(dev_name, "query_info")
                else:
                    # 全部设备
                    await server.broadcast_command("query_info")
            elif cmd == "timeproof":
                await server.broadcast_command("set_time {now+2s}")
            elif cmd == "hisdata":
                date_str = parts[1] if len(parts) > 1 else (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                await server.broadcast_command(f"start_output {date_str}")
            elif cmd == "pending":
                if len(parts) > 1 and parts[1] == "clear":
                    count = len(server._pending)
                    server._pending.clear()
                    print(f"  已清除 {count} 条待发指令")
                elif not server._pending:
                    print("  (无待发指令)")
                else:
                    now = datetime.now()
                    for i, (cmd_str, deadline, created) in enumerate(server._pending, 1):
                        remain = (deadline - now).total_seconds()
                        if remain > 0:
                            m, s = divmod(int(remain), 60)
                            h, m = divmod(m, 60)
                            print(f"  {i}. {cmd_str}")
                            print(f"     创建: {created.strftime('%H:%M:%S')}  剩余: {h}h{m:02d}m{s:02d}s")
                        else:
                            print(f"  {i}. {cmd_str}  [已过期]")
            else:
                print(f"未知命令: {cmd}，输入 help 查看帮助")
        except (EOFError, KeyboardInterrupt):
            await server.stop()
            break


# ─────────────────────────── 入口 ───────────────────────────

def _expand_ports(port_args: list[str]) -> list[int]:
    """展开端口列表：支持单个端口和范围 10101-10110。"""
    result = []
    for arg in port_args:
        if "-" in arg:
            try:
                start, end = arg.split("-", 1)
                result.extend(range(int(start), int(end) + 1))
            except ValueError:
                raise argparse.ArgumentTypeError(f"端口范围格式错误: {arg} (期望如 10101-10110)")
        else:
            try:
                result.append(int(arg))
            except ValueError:
                raise argparse.ArgumentTypeError(f"端口格式错误: {arg}")
    return result


def parse_args():
    parser = argparse.ArgumentParser(description="ZTDL-1X TCP Server")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址 (默认: 0.0.0.0)")
    parser.add_argument("--ports", type=str, nargs="+", default=["10101", "10102", "10103"],
                        help="监听端口，支持范围如 10101-10110 (默认: 10101 10102 10103)")
    parser.add_argument("--db", default="data/ztdl1x.db", help="数据库目录路径 (仅取目录部分，默认: data/)")
    parser.add_argument("--no-console", action="store_true", help="禁用交互式控制台")
    parser.add_argument("--admin-host", default="127.0.0.1", help="管理端口监听地址 (默认: 127.0.0.1)")
    parser.add_argument("--admin-port", type=int, default=10100, help="管理端口 (默认: 10100, 0=禁用)")
    parser.add_argument("--pid-file", default=None, help="PID 文件路径 (默认: 系统临时目录/ztdl1x_server.pid)")
    return parser.parse_args()


async def main():
    args = parse_args()
    ports = _expand_ports(args.ports)

    pid_file = args.pid_file or os.path.join(tempfile.gettempdir(), "ztdl1x_server.pid")
    store = DataStore(args.db)
    csv_export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ztdl1x_data")
    server = ZTDL1XServer(args.host, ports, store,
                          admin_host=args.admin_host,
                          admin_port=args.admin_port,
                          pid_file=pid_file,
                          csv_export_dir=csv_export_dir)
    _shutdown_task: Optional[asyncio.Task] = None

    # 优雅退出
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_ignore_conn_reset)

    def _signal_handler():
        nonlocal _shutdown_task
        if _shutdown_task is None:
            _shutdown_task = asyncio.create_task(server.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass  # Windows 不支持 add_signal_handler (SIGTERM)

    try:
        if args.no_console:
            await server.start()
        else:
            server_task = asyncio.create_task(server.start())
            console_task = asyncio.create_task(interactive_console(server))

            done, pending = await asyncio.wait(
                [server_task, console_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
    finally:
        if _shutdown_task is None:
            _shutdown_task = asyncio.create_task(server.stop())
        await _shutdown_task


def _ignore_conn_reset(loop, context):
    exc = context.get('exception')
    if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
        return  # Windows proactor 关闭时正常噪声
    loop.default_exception_handler(context)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n服务器已停止")
