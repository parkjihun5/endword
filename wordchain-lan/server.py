from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Set, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# 정적 파일은 /static 아래로만 제공
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


HANGUL_RE = re.compile(r"^[가-힣]{2,}$")

def normalize_word(raw: str) -> str:
    return re.sub(r"\s+", "", (raw or "").strip())

def first_char(w: str) -> str:
    return w[0] if w else ""

def last_char(w: str) -> str:
    return w[-1] if w else ""

@dataclass
class RoomState:
    current_word: str = ""
    used: Set[str] = field(default_factory=set)
    clients: Set[WebSocket] = field(default_factory=set)

rooms: Dict[str, RoomState] = {}


async def broadcast(room_id: str, payload: dict):
    room = rooms.get(room_id)
    if not room:
        return
    dead: Set[WebSocket] = set()
    msg = json.dumps(payload, ensure_ascii=False)
    for ws in list(room.clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    for ws in dead:
        room.clients.discard(ws)


def validate_move(room: RoomState, word: str) -> Optional[str]:
    w = normalize_word(word)
    if not HANGUL_RE.match(w):
        return "한글 2글자 이상 단어만 가능해요."
    if room.current_word:
        need = last_char(room.current_word)
        if first_char(w) != need:
            return f"규칙 위반! '{need}'(으)로 시작해야 해요."
    if w in room.used:
        return "이미 사용된 단어예요."
    return None


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    room_id = websocket.query_params.get("room", "demo").strip() or "demo"
    name = websocket.query_params.get("name", "익명").strip() or "익명"

    await websocket.accept()

    room = rooms.setdefault(room_id, RoomState())
    room.clients.add(websocket)

    # 접속자에게 현재 상태 보내기
    await websocket.send_text(json.dumps({
        "type": "state",
        "room": room_id,
        "currentWord": room.current_word,
        "usedCount": len(room.used),
        "log": [f"[시스템] '{room_id}' 방 접속: {name}"],
    }, ensure_ascii=False))

    await broadcast(room_id, {
        "type": "log",
        "log": [f"[시스템] {name} 님이 입장했습니다."],
    })

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "reset":
                room.current_word = ""
                room.used.clear()
                await broadcast(room_id, {
                    "type": "state",
                    "room": room_id,
                    "currentWord": "",
                    "usedCount": 0,
                    "log": [f"[시스템] {name} 님이 리셋했습니다."],
                })
                continue

            if msg_type == "start":
                start_word = normalize_word(data.get("word", ""))
                err = validate_move(RoomState(), start_word)
                if err:
                    await websocket.send_text(json.dumps({"type": "error", "message": err}, ensure_ascii=False))
                    continue

                room.current_word = start_word
                room.used = {start_word}

                await broadcast(room_id, {
                    "type": "state",
                    "room": room_id,
                    "currentWord": room.current_word,
                    "usedCount": len(room.used),
                    "log": [f"[시스템] 게임 시작: {name} → {start_word}"],
                })
                continue

            if msg_type == "play":
                word = normalize_word(data.get("word", ""))
                if not room.current_word:
                    await websocket.send_text(json.dumps({"type": "error", "message": "먼저 시작 단어를 설정해 주세요."}, ensure_ascii=False))
                    continue

                err = validate_move(room, word)
                if err:
                    await websocket.send_text(json.dumps({"type": "error", "message": err}, ensure_ascii=False))
                    continue

                room.current_word = word
                room.used.add(word)

                await broadcast(room_id, {
                    "type": "state",
                    "room": room_id,
                    "currentWord": room.current_word,
                    "usedCount": len(room.used),
                    "log": [f"{name}: {word}"],
                })
                continue

            await websocket.send_text(json.dumps({"type": "error", "message": "알 수 없는 메시지 타입"}, ensure_ascii=False))

    except WebSocketDisconnect:
        room.clients.discard(websocket)
        await broadcast(room_id, {"type": "log", "log": [f"[시스템] {name} 님이 퇴장했습니다."]})
