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

# ──────────────────────── 跨平台终端输入（历史 + 补全）─────────────────────────

_COMMANDS = [
    "list", "tables", "query", "pending",
    "send", "broadcast", "refresh",
    "timeproof", "hisdata", "export",
    "shutdown", "help", "quit", "exit",
]

# 需要跟参数的命令（Tab 补全后自动补空格）
_NEEDS_ARG = {"tables", "query", "send", "pending", "broadcast", "refresh", "hisdata", "export"}

# 子命令补全（命令 → 候选子命令列表）
_SUB_COMMANDS = {"pending": ["clear"]}

_HISTORY_FILE = os.path.join(os.path.expanduser("~"), ".ztdl1x_history")
_HISTORY_LENGTH = 1000


class _Console:
    """跨平台终端输入：↑↓ 历史、Tab 补全、左右光标、退格删除。
    优先使用 GNU readline（类 Unix），不可用时回退到原始终端 I/O（Windows 也支持）。
    """

    def __init__(self):
        self._history: list[str] = []
        self._hindex: int = 0           # 当前浏览位置（len(history)=新行）
        self._saved_line: str = ""       # 浏览历史前暂存的行
        self._use_readline = False
        self._commands: list[str] = _COMMANDS[:]
        self._devices: list[str] = []
        self._try_readline()

    # ── readline 路径 ──

    def _try_readline(self):
        try:
            import readline
            # 验证 readline 是否真正可用（Nuitka/PyInstaller 可能漏掉 so 文件）
            readline.get_line_buffer()
            readline.set_completer_delims(" \t\n;")
            readline.set_completer(self._rl_complete)
            # bind tab
            for b in ("tab: complete", '"\t": complete'):
                try:
                    readline.parse_and_bind(b)
                except Exception:
                    continue
            # bind backspace — 用八进制字面量，兼容 GNU readline / libedit / Nuitka
            for b in ('"\\177": backward-delete-char', '"\\010": backward-delete-char'):
                try:
                    readline.parse_and_bind(b)
                except Exception:
                    continue
            try:
                readline.read_history_file(_HISTORY_FILE)
            except (FileNotFoundError, OSError):
                pass
            readline.set_history_length(_HISTORY_LENGTH)
            self._use_readline = True
        except Exception:
            self._use_readline = False

    def _rl_complete(self, text: str, state: int) -> str | None:
        import readline
        full = readline.get_line_buffer()
        beg = readline.get_begidx()
        before = full[:beg]
        matches = self._match(text, before)
        # 唯一匹配 + 需要参数 → 自动补空格（readline 路径也支持）
        if len(matches) == 1 and " " not in before and matches[0] in _NEEDS_ARG:
            return (matches[0] + " ") if state == 0 else None
        try:
            return matches[state]
        except IndexError:
            return None

    def _rl_save(self):
        try:
            import readline
            readline.write_history_file(_HISTORY_FILE)
        except (ImportError, OSError):
            pass

    # ── 原始终端路径 ──

    def _getch(self) -> str:
        """读取单个按键。Windows 用 msvcrt，Unix 用原始终端。"""
        if os.name == "nt":
            import msvcrt
            ch = msvcrt.getwch()
            if ch in ("\xe0", "\x00"):
                return "\x1b" + {"H": "[A", "P": "[B", "M": "[C", "K": "[D"}.get(
                    msvcrt.getwch(), "")
            return ch
        else:
            import termios, tty
            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                ch = sys.stdin.buffer.read(1).decode(errors="replace")
                if ch == "\x1b":
                    tail = sys.stdin.buffer.read(2).decode(errors="replace")
                    return "\x1b" + tail
                return ch
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)

    def set_devices(self, devices: list[str]):
        self._devices = devices

    def _match(self, word: str, before: str) -> list[str]:
        """根据上下文返回补全候选：命令名 / 设备名 / 子命令。"""
        stripped = before.strip()
        if " " not in before or not stripped:
            # 第一词（或只有空白）→ 命令名
            return [c for c in self._commands if c.startswith(word)]
        # 第一词之后
        cmd = stripped.split()[0]
        if cmd in ("tables", "query", "send", "refresh", "hisdata", "export"):
            return [d for d in self._devices if d.startswith(word)]
        # 子命令补全
        subs = _SUB_COMMANDS.get(cmd, [])
        if subs:
            return [s for s in subs if s.startswith(word)]
        return []

    def _complete(self, word: str, before: str) -> str | None:
        """补全：返回最长公共前缀（唯一匹配时）或打印候选（多个匹配时）。
        word: 当前词, before: 光标前的完整文本。
        """
        matches = self._match(word, before)
        if not matches:
            return None
        if len(matches) == 1:
            result = matches[0]
            # 第一词唯一匹配 + 命令需要参数 → 自动补空格
            if " " not in before and result in _NEEDS_ARG:
                return result + " "
            return result
        sys.stdout.write("\n  " + "  ".join(matches) + "\n")
        self._redisplay("ztdl1x> ", before + word, len(before + word))
        prefix = os.path.commonprefix(matches)
        return prefix if len(prefix) > len(word) else None

    @staticmethod
    def _redisplay(prompt: str, line: str, cursor: int):
        sys.stdout.write("\r\x1b[K" + prompt + line)
        if cursor < len(line):
            sys.stdout.write("\r\x1b[" + str(len(prompt) + cursor + 1) + "C")
        sys.stdout.flush()

    def input(self, prompt: str = "") -> str:
        if self._use_readline:
            try:
                import builtins
                return builtins.input(prompt)
            except (EOFError, KeyboardInterrupt):
                raise
        return self._raw_input(prompt)

    def _raw_input(self, prompt: str = "") -> str:
        """原始终端模式输入循环：处理所有按键。"""
        sys.stdout.write(prompt)
        sys.stdout.flush()
        line = ""
        cursor = 0
        self._hindex = len(self._history)

        while True:
            try:
                ch = self._getch()
            except Exception:
                # 终端读取异常 → 回退到 input()
                import builtins
                return builtins.input(prompt)

            if ch in ("\r", "\n"):
                sys.stdout.write("\r\n")
                break

            elif ch in ("\x08", "\x7f"):           # Backspace
                if cursor > 0 and line:
                    line = line[:cursor - 1] + line[cursor:]
                    cursor -= 1
                    self._redisplay(prompt, line, cursor)

            elif ch == "\x04":                     # Ctrl+D
                if not line:
                    sys.stdout.write("\r\n")
                    raise EOFError()
                # 非空行当作 Delete
                if cursor < len(line):
                    line = line[:cursor] + line[cursor + 1:]
                    self._redisplay(prompt, line, cursor)

            elif ch == "\x03":                     # Ctrl+C
                sys.stdout.write("^C\r\n")
                raise KeyboardInterrupt()

            elif ch == "\x1b[A" or ch == "\x1bOH":  # ↑
                self._nav_history(prompt, -1, line, cursor)
                if self._hindex < len(self._history):
                    line = self._history[self._hindex]
                else:
                    line = self._saved_line
                cursor = len(line)
                self._redisplay(prompt, line, cursor)

            elif ch == "\x1b[B" or ch == "\x1bOF":  # ↓
                self._nav_history(prompt, 1, line, cursor)
                if self._hindex < len(self._history):
                    line = self._history[self._hindex]
                else:
                    line = self._saved_line
                cursor = len(line)
                self._redisplay(prompt, line, cursor)

            elif ch == "\x1b[C":                    # →
                if cursor < len(line):
                    cursor += 1
                    sys.stdout.write("\x1b[1C")
                    sys.stdout.flush()

            elif ch == "\x1b[D":                    # ←
                if cursor > 0:
                    cursor -= 1
                    sys.stdout.write("\x1b[1D")
                    sys.stdout.flush()

            elif ch == "\x1b[H":                    # Home
                cursor = 0
                self._redisplay(prompt, line, cursor)

            elif ch == "\x1b[F":                    # End
                cursor = len(line)
                self._redisplay(prompt, line, cursor)

            elif ch == "\t":                        # Tab
                full = line[:cursor]
                # 拆分：光标前文本 = before + 当前词
                if " " in full:
                    last_space = full.rfind(" ")
                    word = full[last_space + 1:]
                    before = full[:last_space + 1]
                else:
                    word = full
                    before = ""
                completed = self._complete(word, before)
                if completed:
                    line = before + completed + line[cursor:]
                    cursor = len(before) + len(completed)
                    self._redisplay(prompt, line, cursor)

            elif ch == "\x0c":                      # Ctrl+L: 清屏
                sys.stdout.write("\x1b[2J\x1b[H")
                self._redisplay(prompt, line, cursor)

            elif len(ch) == 1 and ord(ch) >= 32:   # 可打印字符
                line = line[:cursor] + ch + line[cursor:]
                cursor += 1
                sys.stdout.write("\r\x1b[K" + prompt + line)
                if cursor < len(line):
                    sys.stdout.write("\r\x1b[" + str(len(prompt) + cursor + 1) + "C")
                sys.stdout.flush()

        # 保存到历史
        stripped = line.strip()
        if stripped:
            if not self._history or self._history[-1] != stripped:
                self._history.append(stripped)
            if len(self._history) > _HISTORY_LENGTH:
                self._history = self._history[-_HISTORY_LENGTH:]
        return line

    def _nav_history(self, prompt: str, direction: int, line: str, cursor: int):
        """在历史记录中上下导航。"""
        if self._hindex == len(self._history):
            self._saved_line = line
        new_idx = self._hindex + direction
        if 0 <= new_idx <= len(self._history):
            self._hindex = new_idx

    def _save_history_file(self):
        try:
            with open(_HISTORY_FILE, "w", encoding="utf-8") as f:
                for entry in self._history[-_HISTORY_LENGTH:]:
                    f.write(entry + "\n")
        except OSError:
            pass

    def save_history(self):
        """持久化历史到文件（退出时调用一次）。"""
        if self._use_readline:
            self._rl_save()
        else:
            self._save_history_file()


# 全局单例
_console = _Console()
console_input = _console.input  # 跨平台 input，不覆盖 builtins


def check_server(host: str, port: int) -> bool:
    """快速检查服务是否可达"""
    try:
        sock = socket.create_connection((host, port), timeout=2)
        sock.close()
        return True
    except (ConnectionRefusedError, OSError):
        return False


def check_pid_file() -> bool:
    """检查 PID 文件是否存在"""
    pid_path = os.path.join(tempfile.gettempdir(), "ztdl1x_server.pid")
    if not os.path.exists(pid_path):
        return False
    try:
        with open(pid_path) as f:
            pid = int(f.read().strip())
        # 检查进程是否存在
        os.kill(pid, 0)
        return True
    except (ValueError, OSError):
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
            _console.set_devices(devices)
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

    await _fetch_devices(reader, writer)

    try:
        await _interactive_loop(reader, writer)
    finally:
        _console.save_history()
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
    if not _console._devices:
        return True  # 还没拉取设备列表，放行
    if dev not in _console._devices:
        names = ", ".join(_console._devices)
        print(f"  未知设备: {dev} (可用: {names} 或 list 刷新)")
        return False
    return True


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
        if limit < 1:
            print("  limit 必须大于 0")
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
            try:
                datetime.strptime(a, "%Y-%m-%d")
                date_str = a
            except ValueError:
                if dev is None and (_console._devices and a in _console._devices or not _console._devices):
                    dev = a
                elif not dev:
                    print("用法: hisdata [dev] [date]  (默认: 全部设备, 昨天)")
                    return None
        if not date_str:
            date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        return {"cmd": "hisdata", "dev": dev, "date": date_str}

    if cmd == "export":
        # export [dev] [date] [path]
        args = parts[1].split() if len(parts) > 1 else []
        dev = None
        date_str = None
        path = None
        for a in args:
            try:
                datetime.strptime(a, "%Y-%m-%d")
                date_str = a
            except ValueError:
                if a.endswith(".csv") or "/" in a or "\\" in a:
                    path = a
                elif dev is None and (_console._devices and a in _console._devices or not _console._devices):
                    dev = a
                elif not dev:
                    print("用法: export [dev] [date] [path]  (默认: 全部设备, 昨天, ztdl1x_data/)")
                    return None
        if not date_str:
            date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        return {"cmd": "export", "dev": dev, "date": date_str, "path": path}
        if len(parts) > 1:
            print("用法: shutdown  (不接受参数)")
            return None
        return {"cmd": "shutdown"}

    print(f"未知命令: {cmd}，输入 help 查看帮助")
    return None


def _print_aligned(rows: list, fields: list[str]):
    """以对齐列格式打印查询结果，值全为空的列不显示。"""
    col_count = len(fields)

    # 过滤：跳过所有行值全为空的列
    keep = [False] * col_count
    for row in rows:
        for i in range(col_count):
            if i + 1 < len(row) and row[i + 1] is not None and str(row[i + 1]).strip():
                keep[i] = True

    kept_fields = [fields[i] for i in range(col_count) if keep[i]]
    kept_indices = [i for i in range(col_count) if keep[i]]
    if not kept_fields:
        return  # 不应该发生

    # 计算列宽
    widths = [len(f) for f in kept_fields]
    for row in rows[:20]:
        for j, idx in enumerate(kept_indices):
            if idx + 1 < len(row):
                v = str(row[idx + 1]) if row[idx + 1] is not None else ""
                widths[j] = max(widths[j], min(len(v), 40))

    # 表头
    header = " | ".join(f"{kept_fields[j]:<{widths[j]}}" for j in range(len(kept_fields)))
    sep = "-+-".join("-" * widths[j] for j in range(len(kept_fields)))
    print(f"    {header}")
    print(f"    {sep}")

    # 数据行
    for row in rows:
        vals = []
        for j, idx in enumerate(kept_indices):
            v = str(row[idx + 1]) if idx + 1 < len(row) and row[idx + 1] is not None else ""
            vals.append(f"{v:<{widths[j]}}")
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
        _console.set_devices([d["name"] for d in devices])

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
