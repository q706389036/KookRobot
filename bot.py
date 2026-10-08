import os
import json
import asyncio
import logging
from dotenv import load_dotenv
import aiohttp

# ==================== 配置 ====================
load_dotenv()

TOKEN = os.getenv("KOOK_TOKEN")
API_BASE = "https://www.kookapp.cn/api/v3"

if not TOKEN:
    print("❌ 没有找到 KOOK_TOKEN")
    print("请在 .env 文件中设置 KOOK_TOKEN")
    exit(1)

# ==================== 全局状态 ====================
ws: aiohttp.ClientWebSocketResponse | None = None
heartbeat_task: asyncio.Task | None = None
sn: int = 0
BOT_USER_ID: str | None = None
last_processed_sn: int = 0

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


# ==================== HTTP API ====================
async def kook_request(session: aiohttp.ClientSession, path: str, method: str = "GET", body: dict | None = None):
    """调用 KOOK HTTP API"""
    headers = {
        "Authorization": f"Bot {TOKEN}",
        "Content-Type": "application/json",
    }
    url = f"{API_BASE}{path}"

    async with session.request(method, url, headers=headers, json=body) as resp:
        data = await resp.json()

    if data.get("code") != 0:
        raise Exception(f"KOOK API 错误: {data['code']} {data.get('message', '')}")

    return data.get("data")


async def send_message(session: aiohttp.ClientSession, channel_id: str, content: str, quote: str | None = None):
    """发送频道消息"""
    body = {
        "type": 9,
        "target_id": channel_id,
        "content": content,
    }
    if quote:
        body["quote"] = quote

    return await kook_request(session, "/message/create", method="POST", body=body)


async def get_gateway(session: aiohttp.ClientSession):
    """获取 WebSocket Gateway"""
    return await kook_request(session, "/gateway/index?compress=0")


# ==================== 事件处理 ====================
async def handle_event(session: aiohttp.ClientSession, packet: dict):
    """处理收到的事件"""
    global last_processed_sn

    # 仅处理 s=0 的事件
    if not packet or packet.get("s") != 0:
        return

    # 跳过已处理过的 sn
    packet_sn = packet.get("sn")
    if packet_sn is not None and packet_sn <= last_processed_sn:
        logger.info(f"⏭️ 跳过重复事件 sn={packet_sn}")
        return

    # 更新最后处理的 sn
    if packet_sn is not None:
        last_processed_sn = packet_sn

    message = packet.get("d")
    if not message:
        return

    # 忽略机器人自己的消息
    if message.get("author_id") == BOT_USER_ID:
        return

    # 检查是否 @ 了机器人
    mentions = message.get("extra", {}).get("mention", [])
    is_mentioned = BOT_USER_ID in mentions
    if not is_mentioned:
        return

    # 仅处理文字消息(type=1)和 KMarkdown 消息(type=9)
    msg_type = message.get("type")
    if msg_type not in (1, 9):
        return

    # 清理 content 中的 (met)...(met) 标签
    import re
    raw_content = message.get("content", "")
    content = re.sub(r"\(met\)\d+\(met\)", "", raw_content).strip()

    if not content:
        return

    channel_id = message.get("target_id")
    message_id = message.get("msg_id")
    author_name = message.get("extra", {}).get("author", {}).get("username", "未知用户")

    logger.info(f"[消息] {author_name}: {content}")

    # !ping
    if content == "!ping":
        await send_message(session, channel_id, "🏓 Pong!", message_id)
        return

    # !hello
    if content == "!hello":
        username = author_name or "朋友"
        await send_message(session, channel_id, f"你好，{username}！我是 KOOK 机器人 🤖", message_id)
        return

    # !help
    if content == "!help":
        help_text = "\n".join([
            "**机器人帮助**",
            "",
            "`!ping` - 测试机器人是否在线",
            "`!hello` - 和机器人打招呼",
            "`!help` - 查看帮助",
        ])
        await send_message(session, channel_id, help_text, message_id)
        return


# ==================== 机器人信息初始化 ====================
async def init_bot_info(session: aiohttp.ClientSession):
    global BOT_USER_ID
    try:
        user = await kook_request(session, "/user/me")
        BOT_USER_ID = user["id"]
        logger.info(f"🤖 机器人已登录: {user['username']} (ID: {BOT_USER_ID})")
    except Exception as e:
        logger.error(f"❌ 获取机器人信息失败: {e}")
        exit(1)


# ==================== 心跳 ====================
async def heartbeat_loop(interval_ms: int, session: aiohttp.ClientSession):
    """定时发送心跳"""
    global sn
    interval_sec = interval_ms / 1000.0
    logger.info(f"💓 心跳间隔: {interval_ms}ms")

    while True:
        await asyncio.sleep(interval_sec)
        if ws is None or ws.closed:
            break
        packet = {"s": 2, "d": {"sn": sn}}
        try:
            await ws.send_json(packet)
            logger.info("💓 发送心跳")
        except Exception as e:
            logger.error(f"❌ 发送心跳失败: {e}")
            break


def stop_heartbeat():
    global heartbeat_task
    if heartbeat_task and not heartbeat_task.done():
        heartbeat_task.cancel()
        heartbeat_task = None


# ==================== WebSocket 连接 ====================
async def connect(session: aiohttp.ClientSession):
    global ws, heartbeat_task, sn

    try:
        logger.info("正在获取 KOOK Gateway...")
        gateway = await get_gateway(session)

        if not gateway or not gateway.get("url"):
            raise Exception("KOOK Gateway 地址获取失败")

        logger.info(f"Gateway: {gateway['url']}")

        ws = await session.ws_connect(gateway["url"])
        logger.info("✅ KOOK WebSocket 已连接")

        # 启动心跳
        heartbeat_interval = gateway.get("heartbeat_interval", 30000)
        stop_heartbeat()
        heartbeat_task = asyncio.create_task(heartbeat_loop(heartbeat_interval, session))

        # 消息循环
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    packet = json.loads(msg.data)
                    logger.info(f"收到数据: {json.dumps(packet, ensure_ascii=False)}")

                    op = packet.get("s")
                    if op == 0:
                        # Event
                        if packet.get("sn") is not None:
                            sn = packet["sn"]
                        await handle_event(session, packet)
                    elif op == 1:
                        logger.info("收到 Hello")
                    elif op == 2:
                        # Ping (服务端主动 ping，回复心跳)
                        hb_packet = {"s": 2, "d": {"sn": sn}}
                        await ws.send_json(hb_packet)
                    elif op == 3:
                        logger.info("收到 Pong")
                    elif op == 4:
                        logger.info("收到 Resume")
                    else:
                        pass

                except Exception as e:
                    logger.error(f"❌ 处理 WebSocket 数据失败: {e}")

            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    except Exception as e:
        logger.error(f"❌ 连接 KOOK 失败: {e}")
    finally:
        stop_heartbeat()
        logger.info("⚠️ WebSocket 已断开")


# ==================== 自动重连 ====================
async def run_with_reconnect():
    async with aiohttp.ClientSession() as session:
        await init_bot_info(session)
        while True:
            await connect(session)
            logger.info("🔄 5秒后尝试重新连接 KOOK...")
            await asyncio.sleep(5)


# ==================== 入口 ====================
if __name__ == "__main__":
    print("=================================")
    print("        KOOK Python Bot")
    print("=================================")
    try:
        asyncio.run(run_with_reconnect())
    except KeyboardInterrupt:
        print("\n👋 机器人已停止")