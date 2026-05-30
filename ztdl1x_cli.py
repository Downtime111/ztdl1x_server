"""
ztdl1x CLI Client
==================
ZTDL-1X 服务管理客户端，通过管理端口与后台服务通信。

用法：
    ztdl1x                       # 连接默认 127.0.0.1:10100
    ztdl1x --port 10101          # 指定管理端口
    ztdl1x --host 192.168.1.100  # 指定管理地址
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import tempfile
from datetime import datetime, timedelta

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.history import FileHistory

# ──────────────────────── 跨平台终端输入（prompt_toolkit）─────────────────────────

_COMMANDS = [
    "list", "tables", "query", "pending",
    "send", "broadcast", "refresh",
    "timeproof", "hisdata", "export",
    "shutdown", "help", "quit", "exit",
]

_NEEDS_ARG = {"tables", "query", "send", "pending", "broadcast", "refresh", "hisdata", "export"}

_SUB_COMMANDS = {"pending": ["clear"]}

_HISTORY_FILE = os.path.join(os.path.expanduser("~"), ".ztdl1x_history")


class _ZTDCompleter(Completer):
    """Tab 补全：命令名 → 设备名 → 子命令"""

    def __init__(self):
        self.devices: list[str] = []

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        word = document.get_word_before_cursor(WORD=True)

        stripped = text.strip()
        if " " not in text or not stripped:
            candidates = [c for c in _COMMANDS if c.startswith(word)]
        else:
            cmd = stripped.split()[0]
            if cmd in ("tables", "query", "send", "refresh", "hisdata", "export"):
                candidates = [d for d in self.devices if d.startswith(word)]
            else:
                subs = _SUB_COMMANDS.get(cmd, [])
                candidates = [s for s in subs if s.startswith(word)]

        if len(candidates) == 1 and " " not in text and candidates[0] in _NEEDS_ARG:
            candidates = [candidates[0] + " "]

        for c in candidates:
            yield Completion(c, start_position=-len(word))


_completer = _ZTDCompleter()
_session = None


def _get_session():
    """延迟创建 PromptSession，避免 import 时报错（如 Windows Git Bash）。"""
    global _session
    if _session is None:
        _session = PromptSession(
            history=FileHistory(_HISTORY_FILE),
            completer=_completer,
        )
    return _session


def console_input(prompt: str = "") -> str:
    """跨平台终端输入。prompt_toolkit 提供历史、Tab 补全、行编辑。"""
    return _get_session().prompt(prompt)



def check_pid_file() -> bool:
    """检查 PID 文件是否存在"""
    pid_path = os.path.join(tempfile.gettempdir(), "ztdl1x_server.pid")
    try:
        with open(pid_path) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return True
    except (FileNotFoundError, ValueError, OSError):
        return False


async def _fetch_devices(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """从服务端拉取设备列表，更新补全候选。"""
    try:
        writer.write(b'{"cmd":"list"}\n')
        await writer.drain()
        raw = await asyncio.wait_for(reader.readline(), timeout=5)
        resp = json.loads(raw.decode())
        if resp.get("ok"):
            devices = [d["name"] for d in resp.get("devices", [])]
            _completer.devices = devices
    except Exception:
        pass  # 静默失败，不影响主流程


async def client(host: str, port: int):
    print(f"ztdl1x 管理客户端 (服务 {host}:{port})")
    print('输入 help 查看命令，quit 退出客户端\n')

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, limit=2 * 1024 * 1024), timeout=3
        )
    except (ConnectionRefusedError, OSError):
        print(f"错误: 无法连接到 {host}:{port}，服务未运行？")
        if check_pid_file():
            print("  PID 文件存在但无法连接，服务可能正在启动或已异常退出")
        else:
            print("  未找到 PID 文件，请先启动 ztdl1xd 服务")
        return
    except asyncio.TimeoutError:
        print(f"错误: 连接 {host}:{port} 超时")
        return

    # 后台拉取设备列表，不阻塞用户输入
    asyncio.create_task(_fetch_devices(reader, writer))

    try:
        await _interactive_loop(reader, writer)
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def _interactive_loop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    loop = asyncio.get_running_loop()

    while True:
        try:
            line = await loop.run_in_executor(None, lambda: console_input("ztdl1x> "))
        except (EOFError, KeyboardInterrupt):
            print()
            break

        line = line.strip()
        if not line:
            continue

        parts = line.split(maxsplit=1)
        cmd = parts[0].lower()

        if cmd in ("quit", "exit"):
            break

        request = _build_request(cmd, parts)
        if request is None:
            continue

        try:
            writer.write((json.dumps(request) + "\n").encode())
            await writer.drain()

            raw = await asyncio.wait_for(reader.readline(), timeout=10)
            if not raw:
                print("服务端关闭连接")
                break
            response = json.loads(raw.decode())
        except asyncio.TimeoutError:
            print("请求超时")
            continue
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"连接断开: {e}")
            break
        except json.JSONDecodeError:
            print("响应解析失败")
            continue

        _display_response(cmd, response)


def _validate_dev(dev: str) -> bool:
    """校验设备名是否在已知列表中。列表为空（未拉取）时跳过校验。"""
    if not _completer.devices:
        return True
    if dev not in _completer.devices:
        names = ", ".join(_completer.devices)
        print(f"  未知设备: {dev} (可用: {names} 或 list 刷新)")
        return False
    return True


def _arg_is_dev(s: str) -> bool:
    """判断参数是否为已知设备名。设备列表未拉取时总是返回 True。"""
    return not _completer.devices or s in _completer.devices


def _arg_is_date(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _default_date() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def _build_request(cmd: str, parts: list) -> dict | None:
    """将用户输入转换为 JSON 请求"""
    if cmd == "help":
        print("""可用命令:
  list                           - 查看所有设备
  tables <dev> [year]            - 查看设备数据表 (默认当年)
  query <dev> <year> <month> [limit] - 查询设备数据 (默认50条)
  pending [clear]                - 查看/清除待发指令
  send <dev> <command>           - 向指定设备发送指令
  broadcast <command>            - 向所有设备广播指令
  refresh [dev]                  - 读取设备配置
  timeproof                      - 手动触发对时
  hisdata [dev] [date]           - 历史数据导出 (默认:全部设备/昨天)
  export [dev] [date] [path]     - 导出CSV (默认:全部设备/昨天/ztdl1x_data)
  shutdown                       - 关闭服务
  help                           - 显示帮助
  quit / exit                    - 退出客户端""")
        return None

    if cmd == "list":
        if len(parts) > 1:
            print("用法: list  (不接受参数)")
            return None
        return {"cmd": "list"}

    if cmd == "tables":
        if len(parts) < 2:
            print("用法: tables <dev> [year]")
            return None
        try:
            targs = parts[1].split()
            dev = targs[0]
            year = int(targs[1]) if len(targs) > 1 else datetime.now().year
        except ValueError:
            print("用法: tables <dev> [year]  (年份必须是数字)")
            return None
        if not _validate_dev(dev):
            return None
        return {"cmd": "tables", "dev": dev, "year": year}

    if cmd == "query":
        if len(parts) < 2:
            print("用法: query <dev> <year> <month> [limit]")
            return None
        try:
            qargs = parts[1].split()
            if len(qargs) < 3:
                print("用法: query <dev> <year> <month> [limit]")
                return None
            dev, year, month = qargs[0], int(qargs[1]), int(qargs[2])
            limit = int(qargs[3]) if len(qargs) > 3 else 50
        except ValueError:
            print("用法: query <dev> <year> <month> [limit]  (年/月/条数必须是数字)")
            return None
        if not _validate_dev(dev):
            return None
        if not (1 <= month <= 12):
            print("  月份必须在 1-12 之间")
            return None
        if not (1 <= limit <= 10000):
            print("  limit 必须在 1-10000 之间")
            return None
        return {"cmd": "query", "dev": dev, "year": year, "month": month, "limit": limit}

    if cmd == "pending":
        arg = parts[1] if len(parts) > 1 else ""
        if arg and arg != "clear":
            print(f"用法: pending [clear]")
            return None
        return {"cmd": "pending", "clear": arg == "clear"}

    if cmd == "send":
        if len(parts) < 2:
            print("用法: send <dev_name> <command>")
            return None
        sub = parts[1].split(maxsplit=1)
        if len(sub) < 2:
            print("用法: send <dev_name> <command>")
            return None
        if not _validate_dev(sub[0]):
            return None
        return {"cmd": "send", "dev": sub[0], "command": sub[1]}

    if cmd == "broadcast":
        if len(parts) < 2:
            print("用法: broadcast <command>")
            return None
        return {"cmd": "broadcast", "command": parts[1]}

    if cmd == "refresh":
        dev = parts[1] if len(parts) > 1 else None
        if dev and not _validate_dev(dev):
            return None
        return {"cmd": "refresh", "dev": dev}

    if cmd == "timeproof":
        if len(parts) > 1:
            print("用法: timeproof  (不接受参数)")
            return None
        return {"cmd": "timeproof"}

    if cmd == "hisdata":
        # hisdata [dev] [date]
        args = parts[1].split() if len(parts) > 1 else []
        dev = None
        date_str = None
        for a in args:
            if _arg_is_date(a):
                date_str = a
            elif dev is None and _arg_is_dev(a):
                dev = a
            elif not dev:
                print("用法: hisdata [dev] [date]  (默认: 全部设备, 昨天)")
                return None
        return {"cmd": "hisdata", "dev": dev, "date": date_str or _default_date()}

    if cmd == "export":
        # export [dev] [date] [path]
        args = parts[1].split() if len(parts) > 1 else []
        dev = None
        date_str = None
        path = None
        for a in args:
            if _arg_is_date(a):
                date_str = a
            elif a.endswith(".csv") or "/" in a or "\\" in a:
                path = a
            elif dev is None and _arg_is_dev(a):
                dev = a
            elif not dev:
                print("用法: export [dev] [date] [path]  (默认: 全部设备, 昨天, ztdl1x_data/)")
                return None
        return {"cmd": "export", "dev": dev, "date": date_str or _default_date(), "path": path}

    if cmd == "shutdown":
        if len(parts) > 1:
            print("用法: shutdown  (不接受参数)")
            return None
        return {"cmd": "shutdown"}

    print(f"未知命令: {cmd}，输入 help 查看帮助")
    return None


def _print_aligned(rows: list, fields: list[str]):
    """以对齐列格式打印查询结果，值全为空的列不显示。"""
    col_count = len(fields)

    # 单次遍历：收集每列是否有数据 + 计算列宽 + 缓存字符串值
    keep = [False] * col_count
    widths = [len(f) for f in fields]
    str_rows: list[list[str]] = []

    for row in rows:
        str_row: list[str] = []
        for i in range(col_count):
            idx = i + 1  # row[0] 是 id 列，数据从 row[1] 开始
            if idx < len(row) and row[idx] is not None:
                v = str(row[idx])
                if v.strip():
                    keep[i] = True
                widths[i] = max(widths[i], min(len(v), 40))
                str_row.append(v)
            else:
                str_row.append("")
        str_rows.append(str_row)

    kept_indices = [i for i in range(col_count) if keep[i]]
    kept_fields = [fields[i] for i in kept_indices]
    if not kept_fields:
        return

    # 表头
    header = " | ".join(f"{fields[i]:<{widths[i]}}" for i in kept_indices)
    sep = "-+-".join("-" * widths[i] for i in kept_indices)
    print(f"    {header}")
    print(f"    {sep}")

    # 数据行
    for str_row in str_rows:
        vals = [f"{str_row[i]:<{widths[i]}}" for i in kept_indices]
        print(f"    {' | '.join(vals)}")


def _display_response(cmd: str, resp: dict):
    """格式化显示服务端响应"""
    if not resp.get("ok"):
        print(f"  错误: {resp.get('error', 'unknown')}")
        return

    if cmd == "list":
        devices = resp.get("devices", [])
        if not devices:
            print("  (无设备记录)")
        else:
            for d in devices:
                print(f"  [{d['status']:>6}] {d['name']}  port:{d['port']}  (last: {d['last_seen']})")
        # 同步设备名到补全候选
        _completer.devices = [d["name"] for d in devices]

    elif cmd == "tables":
        tables = resp.get("tables", [])
        dev = resp.get("dev", "")
        year = resp.get("year", "")
        if not tables:
            print(f"  {dev} / {year} 年无数据表")
        else:
            print(f"  {dev} / {year} 年数据表 ({len(tables)} 张):")
            for t in tables:
                print(f"    {t}")

    elif cmd == "query":
        rows = resp.get("rows", [])
        dev = resp.get("dev", "")
        year = resp.get("year", "")
        month = resp.get("month", "")
        if not rows:
            print(f"  {dev} ({year}-{month:02d}) 无数据")
        else:
            print(f"  {dev} ({year}-{month:02d}) 最新 {len(rows)} 条:")
            fields = resp.get("fields")
            if fields and rows:
                _print_aligned(rows, fields)
            else:
                for row in rows:
                    if len(row) >= 3:
                        print(f"    [{row[0]}] {row[1]}  {', '.join(str(x) for x in row[2:])}")
                    else:
                        print(f"    {row}")

    elif cmd == "pending":
        pending = resp.get("pending", [])
        cleared = resp.get("cleared")
        if cleared is not None:
            print(f"  已清除 {cleared} 条待发指令")
        elif not pending:
            print("  (无待发指令)")
        else:
            for i, item in enumerate(pending, 1):
                remain = item.get("remaining_seconds", 0)
                if remain > 0:
                    m, s = divmod(int(remain), 60)
                    h, m = divmod(m, 60)
                    print(f"  {i}. {item['command']}")
                    print(f"     创建: {item['created']}  剩余: {h}h{m:02d}m{s:02d}s")
                else:
                    print(f"  {i}. {item['command']}  [已过期]")

    elif cmd == "export":
        results = resp.get("results", [])
        if not results:
            print(f"  {resp.get('date', '?')} 无数据")
        else:
            print(f"  {resp['date']}:")
            for r in results:
                if r.get("file"):
                    print(f"    {r['dev']}  {r['rows']} 条 → {r['file']}")
                elif r.get("error"):
                    print(f"    {r['dev']}  {r.get('error', '')}")
                else:
                    print(f"    {r['dev']}  无数据")

    else:
        # send, broadcast, refresh, timeproof, hisdata, shutdown
        print(f"  {resp.get('msg', 'ok')}")
        if resp.get("response"):
            for line in resp["response"].split("\n"):
                print(f"    {line}")


def parse_args():
    parser = argparse.ArgumentParser(description="ZTDL-1X 管理客户端")
    parser.add_argument("--host", default="127.0.0.1", help="管理端口地址 (默认: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=10100, help="管理端口 (默认: 10100)")
    return parser.parse_args()


def main():
    args = parse_args()
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(_ignore_conn_reset)
    try:
        loop.run_until_complete(client(args.host, args.port))
    except KeyboardInterrupt:
        print()
    finally:
        loop.close()


def _ignore_conn_reset(loop, context):
    exc = context.get('exception')
    if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
        return  # Windows proactor 关闭时正常噪声
    loop.default_exception_handler(context)


if __name__ == "__main__":
    main()
