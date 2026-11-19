#!/usr/bin/env python3
"""通用 MQTT 桥接骨架：AI Agent -> 机器人。

不绑定任何厂商。协议约定见 docs/05-协议模板.md，本文件只做四件事：
  1) 把工具调用翻译成一条 MQTT 指令（带序号）
  2) 订阅读遥测，缓存最新状态
  3) 等 ACK，超时就如实返回失败
  4) 打印 JSON，方便 Agent 或脚本解析

环境变量：
  ROBOT_MQTT_SERVER   默认 127.0.0.1
  ROBOT_MQTT_PORT     默认 1883
  ROBOT_MQTT_USER / ROBOT_MQTT_PASSWORD
  ROBOT_TOPIC         默认 robot/main
  ROBOT_CMD_PREFIX    默认 ">>"（厂商"纯指令、不落库"前缀；不需要就设为空）
  ROBOT_ACK_TIMEOUT   默认 2.0 秒
  ROBOT_STALE_AFTER   默认 1.5 秒，超过则 look 返回 stale=true

用法：
  python bridge_skeleton.py look
  python bridge_skeleton.py move F2
  python bridge_skeleton.py servo S1A
  python bridge_skeleton.py stop
  python bridge_skeleton.py watch --seconds 10
  python bridge_skeleton.py --dry-run move F2
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover
    sys.exit("缺少依赖：pip install paho-mqtt")

SERVER = os.getenv("ROBOT_MQTT_SERVER", "127.0.0.1")
PORT = int(os.getenv("ROBOT_MQTT_PORT", "1883"))
USER = os.getenv("ROBOT_MQTT_USER", "")
PASSWORD = os.getenv("ROBOT_MQTT_PASSWORD", "")
TOPIC = os.getenv("ROBOT_TOPIC", "robot/main")
CMD_PREFIX = os.getenv("ROBOT_CMD_PREFIX", ">>")
ACK_TIMEOUT = float(os.getenv("ROBOT_ACK_TIMEOUT", "2.0"))
STALE_AFTER = float(os.getenv("ROBOT_STALE_AFTER", "1.5"))


class Bridge:
    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.connected = threading.Event()
        self._lock = threading.Lock()
        self._telemetry: dict[str, list[str]] = {}
        self._telemetry_ts = 0.0
        self._acks: dict[str, threading.Event] = {}
        self._seq = 0
        self.client = None if dry_run else self._make_client()

    # ---------- 连接 ----------
    def _make_client(self):
        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"robot-bridge-{os.getpid()}",
            )
        except AttributeError:  # paho-mqtt 1.x
            client = mqtt.Client(client_id=f"robot-bridge-{os.getpid()}")
        if USER:
            client.username_pw_set(USER, PASSWORD)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_disconnect = self._on_disconnect
        return client

    def open(self, timeout: float = 5.0) -> None:
        if self.dry_run:
            return
        self.client.connect(SERVER, PORT, keepalive=30)
        self.client.loop_start()
        if not self.connected.wait(timeout):
            raise TimeoutError(f"连接 {SERVER}:{PORT} 超时")

    def close(self) -> None:
        if self.client is not None:
            self.client.loop_stop()
            self.client.disconnect()

    # ---------- 回调 ----------
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        client.subscribe(TOPIC, qos=0)
        self.connected.set()

    def _on_disconnect(self, client, userdata, *args):
        self.connected.clear()

    def _on_message(self, client, userdata, msg):
        try:
            text = msg.payload.decode("utf-8", errors="replace").strip()
        except Exception:
            return
        kind, _, rest = text.partition(":")
        if not rest:
            return
        if kind == "T":
            parts = [p.strip() for p in rest.split(",") if p.strip()]
            if not parts:
                return
            with self._lock:
                self._telemetry[parts[0]] = parts[1:]
                self._telemetry_ts = time.monotonic()
        elif kind == "A":
            seq = rest.split(",")[0].strip()
            with self._lock:
                event = self._acks.get(seq)
            if event:
                event.set()

    # ---------- 指令 ----------
    def send(self, command: str, wait_ack: bool = True) -> dict:
        with self._lock:
            self._seq += 1
            seq = str(self._seq)
        payload = f"{CMD_PREFIX}C:{seq},{command}"

        if self.dry_run:
            return {"command": command, "payload": payload, "dry_run": True}

        event = threading.Event()
        with self._lock:
            self._acks[seq] = event
        started = time.monotonic()
        self.client.publish(TOPIC, payload, qos=0)

        ack = event.wait(ACK_TIMEOUT) if wait_ack else False
        with self._lock:
            self._acks.pop(seq, None)
        return {
            "command": command,
            "seq": seq,
            "payload": payload,
            "ack": bool(ack),
            "latency_ms": round((time.monotonic() - started) * 1000, 1),
        }

    # ---------- 状态 ----------
    def look(self) -> dict:
        with self._lock:
            data = {k: list(v) for k, v in self._telemetry.items()}
            age = time.monotonic() - self._telemetry_ts if self._telemetry_ts else None
        return {
            "telemetry": data,
            "age_ms": None if age is None else round(age * 1000, 1),
            "stale": True if age is None else age > STALE_AFTER,
        }


def emit(obj: dict) -> None:
    print(json.dumps(obj, ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="AI Agent 到机器人的通用 MQTT 桥")
    parser.add_argument("--dry-run", action="store_true", help="只打印将要发送的消息")
    parser.add_argument("--no-ack", action="store_true", help="不等 ACK，直接返回")
    sub = parser.add_subparsers(dest="action", required=True)

    for name in ("move", "servo"):
        p = sub.add_parser(name)
        p.add_argument("value", help="命令枚举，例如 F2 / S1A")

    sub.add_parser("stop")
    sub.add_parser("look")

    p_watch = sub.add_parser("watch")
    p_watch.add_argument("--seconds", type=float, default=10.0)
    p_watch.add_argument("--interval", type=float, default=1.0)

    args = parser.parse_args()
    bridge = Bridge(dry_run=args.dry_run)

    try:
        bridge.open()
        if args.action == "look":
            emit(bridge.look())
        elif args.action == "watch":
            deadline = time.monotonic() + args.seconds
            while time.monotonic() < deadline:
                emit(bridge.look())
                time.sleep(args.interval)
        elif args.action == "stop":
            emit(bridge.send("STOP", wait_ack=not args.no_ack))
        else:
            emit(bridge.send(args.value.upper(), wait_ack=not args.no_ack))
    except (TimeoutError, OSError) as exc:
        emit({"error": str(exc)})
        return 1
    finally:
        bridge.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
