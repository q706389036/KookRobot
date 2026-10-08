require("dotenv").config();

const WebSocket = require("ws");

const TOKEN = process.env.KOOK_TOKEN;

if (!TOKEN) {
  console.error("❌ 没有找到 KOOK_TOKEN");
  console.error("请在 .env 文件中设置 KOOK_TOKEN");
  process.exit(1);
}

const API_BASE = "https://www.kookapp.cn/api/v3";

let ws = null;
let heartbeatTimer = null;
let reconnectTimer = null;
let sn = 0;
let BOT_USER_ID = null;
let lastProcessedSn = 0;

/**
 * 调用 KOOK HTTP API
 */
async function kookRequest(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      Authorization: `Bot ${TOKEN}`,
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });

  const data = await response.json();

  if (data.code !== 0) {
    throw new Error(`KOOK API 错误: ${data.code} ${data.message}`);
  }

  return data.data;
}

/**
 * 发送频道消息
 */
async function sendMessage(channelId, content, quote = null) {
  const body = {
    type: 9,
    target_id: channelId,
    content,
  };

  // 如果需要引用用户刚才的消息
  if (quote) {
    body.quote = quote;
  }

  return await kookRequest("/message/create", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

/**
 * 获取 WebSocket Gateway
 */
async function getGateway() {
  return await kookRequest("/gateway/index?compress=0");
}

/**
 * 处理收到的事件
 */
async function handleEvent(packet) {
  // 仅处理 s=0 的事件
  if (!packet || packet.s !== 0) return;

  // 跳过已处理过的 sn
  if (packet.sn !== undefined && packet.sn <= lastProcessedSn) {
    console.log(`⏭️ 跳过重复事件 sn=${packet.sn}`);
    return;
  }

  // 更新最后处理的 sn
  if (packet.sn !== undefined) {
    lastProcessedSn = packet.sn;
  }

  const message = packet.d;
  if (!message) return;

  // 忽略机器人自己的消息（author_id 为字符串）
  if (message.author_id === BOT_USER_ID) return;

  // mention 是一个用户对象数组，检查其中是否包含 BOT_USER_ID
  const mentions = message.extra?.mention || [];
  const isMentioned = mentions.includes(BOT_USER_ID);

  // 未 @ 机器人，直接忽略
  if (!isMentioned) {
    return;
  }

  // 仅处理文字消息（type=1）和 KMarkdown 消息（type=9）
  // 官方文档：1=文字, 9=KMarkdown
  if (message.type !== 1 && message.type !== 9) return;

  //   console.log("收到事件:", JSON.stringify(packet));

  // 清理 content 中的 (met)...(met) 标签
  const rawContent = message.content || "";
  const content = rawContent
    .replace(/\(met\)\d+\(met\)/g, "") // 移除所有 mention 标签
    .trim();

  if (!content) return;

  const channelId = message.target_id;
  const messageId = message.msg_id;
  const authorName = message.extra?.author?.username;

  console.log(`[消息] ${authorName || "未知用户"}: ${content}`);

  // !ping
  if (content === "!ping") {
    await sendMessage(channelId, "🏓 Pong!", messageId);
    return;
  }

  // !hello
  if (content === "!hello") {
    const username = authorName || "朋友";

    await sendMessage(
      channelId,
      `你好，${username}！我是 KOOK 机器人 🤖`,
      messageId,
    );

    return;
  }

  // !help
  if (content === "!help") {
    await sendMessage(
      channelId,
      [
        "**机器人帮助**",
        "",
        "`!ping` - 测试机器人是否在线",
        "`!hello` - 和机器人打招呼",
        "`!help` - 查看帮助",
      ].join("\n"),
      messageId,
    );

    return;
  }
}

// 获取机器人信息
async function initBotInfo() {
  try {
    const user = await kookRequest("/user/me");
    BOT_USER_ID = user.id;
    console.log(`🤖 机器人已登录: ${user.username} (ID: ${BOT_USER_ID})`);
  } catch (e) {
    console.error("❌ 获取机器人信息失败:", e.message);
    process.exit(1);
  }
}

/**
 * 建立 WebSocket
 */
async function connect() {
  try {
    console.log("正在获取 KOOK Gateway...");

    const gateway = await getGateway();

    if (!gateway || !gateway.url) {
      throw new Error("KOOK Gateway 地址获取失败");
    }

    console.log("Gateway:", gateway.url);

    ws = new WebSocket(gateway.url);

    ws.on("open", () => {
      console.log("✅ KOOK WebSocket 已连接");

      startHeartbeat(gateway.heartbeat_interval);
    });

    ws.on("message", async (raw) => {
      try {
        const packet = JSON.parse(raw.toString());

        console.log("收到数据:", JSON.stringify(packet));

        // KOOK Gateway 操作码
        switch (packet.s) {
          case 0:
            // Event

            // 服务端序号
            if (packet.sn !== undefined) {
              sn = packet.sn;
            }
            await handleEvent(packet);
            break;

          case 1:
            // Hello
            console.log("收到 Hello");
            break;

          case 2:
            // Ping
            sendHeartbeat();
            break;

          case 3:
            // Pong
            console.log("收到 Pong");
            break;

          case 4:
            // Resume
            console.log("收到 Resume");
            break;

          default:
            break;
        }
      } catch (error) {
        console.error("❌ 处理 WebSocket 数据失败:", error);
      }
    });

    ws.on("close", () => {
      console.log("⚠️ WebSocket 已断开");

      stopHeartbeat();

      scheduleReconnect();
    });

    ws.on("error", (error) => {
      console.error("❌ WebSocket 错误:", error.message);
    });
  } catch (error) {
    console.error("❌ 连接 KOOK 失败:", error.message);

    scheduleReconnect();
  }
}

/**
 * 心跳
 */
function startHeartbeat(interval) {
  stopHeartbeat();

  const ms = interval || 30000;

  console.log(`💓 心跳间隔: ${ms}ms`);

  heartbeatTimer = setInterval(() => {
    sendHeartbeat();
  }, ms);
}

function sendHeartbeat() {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }

  const packet = {
    s: 2,
    d: {
      sn,
    },
  };

  ws.send(JSON.stringify(packet));

  console.log("💓 发送心跳");
}

function stopHeartbeat() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

/**
 * 自动重连
 */
function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;

    console.log("🔄 尝试重新连接 KOOK...");

    connect();
  }, 5000);
}

/**
 * 启动机器人
 */

(async () => {
  console.log("=================================");
  console.log("        KOOK JavaScript Bot");
  console.log("=================================");
  await initBotInfo();
  await connect();
})();
