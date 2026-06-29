# Sub2API Prewarm Session 交接文档

> 分支：`feat/prewarm-session-bypass` | Fork：github.com/luzhipeng728/sub2api
> 功能：绕过 Codex 5h usage_limit + 对外响应与官方 Responses API 一致

---

## 一、前置依赖

| 组件 | 要求 |
|------|------|
| Go | 1.24+ |
| Node.js / pnpm | 18+ / 8+ |
| PostgreSQL | 15+（本地已有） |
| Redis | 7+（本地已有） |
| Docker | 服务器需要 |

本地数据库连接（已配好）：
- PG：`localhost:5432`，库 `sub2api`，用户 `sub2api_user` / 密码 `sub2api_pass`，密码留空连接
- Redis：`localhost:6379`

---

## 二、本地开发运行

### 2.1 编译（含前端嵌入）

```bash
cd /Users/luzhipeng/projects/sub2api/backend

# 1. 编译前端（产物输出到 backend/internal/web/dist）
cd ../frontend && pnpm install && pnpm run build && cd ../backend

# 2. 编译后端（带 embed 标签，嵌入前端）
go build -tags embed -o bin/server ./cmd/server
```

> 注意：必须带 `-tags embed`，否则前端不会嵌入，访问 `/` 会 404。

### 2.2 配置文件

配置在 `backend` 的 `DATA_DIR` 下。本地用：
```
/Users/luzhipeng/projects/sub2api/.local-data/config.yaml
```

关键配置（prewarm 功能）：
```yaml
gateway:
  openai_ws:
    enabled: true
    oauth_enabled: true
    api_key_enabled: true
    mode_router_v2_enabled: true
    responses_websockets_v2: true
    ingress_mode_default: ctx_pool
    # === prewarm 功能开关 ===
    http_ingress_ws_v2_bypass_enabled: true   # HTTP 客户端走 WSv2
    prewarm_session_enabled: true             # prewarm 总开关
    prewarm_session_models:
      - gpt-5.4
    prewarm_session_interval_seconds: 60      # worker 预热周期
    prewarm_session_ttl_seconds: 3600
    prewarm_session_concurrency: 4
```

### 2.3 杀掉旧进程 + 运行新的

```bash
# 杀掉所有正在运行的 server 进程
pkill -f "bin/server"
sleep 2

# 启动新服务（端口 18080）
cd /Users/luzhipeng/projects/sub2api/backend
DATA_DIR=/Users/luzhipeng/projects/sub2api/.local-data SERVER_PORT=18080 ./bin/server
```

启动成功标志（日志里）：
```
Server started on 0.0.0.0:18080
[OpenAIPrewarmSession] worker started interval=1m0s models=[gpt-5.4] concurrency=4
```

### 2.4 本地端口

| 服务 | 地址 |
|------|------|
| API 网关 | `http://localhost:18080` |
| 后台登录 | `http://localhost:18080` → admin@local.com / admin123456 |
| API 入口 | `http://localhost:18080/v1/responses` |
| API Key | `sk-codextest1234567890abcdef` |

---

## 三、本地测试方式

### 3.1 准备：清账号限流状态（避免调度排除）

```bash
psql -h localhost -U sub2api_user -d sub2api -c \
  "UPDATE accounts SET schedulable=true, rate_limited_at=NULL, rate_limit_reset_at=NULL, temp_unschedulable_until=NULL WHERE deleted_at IS NULL;"

redis-cli DEL "apikey:auth:faf876203d54998bb3cc6c9ef4333df7c442a6f17d7f7757ccf283a3ba931a7f"
```

### 3.2 测试请求（非流式）

```bash
curl -s -X POST http://localhost:18080/v1/responses \
  -H "Authorization: Bearer sk-codextest1234567890abcdef" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-5.4","input":"你好","stream":false}'
```

预期：返回 JSON，`status: completed`，`output` 有内容，`previous_response_id` 为 null，`instructions` 为 null。

### 3.3 测试请求（流式）

```bash
curl -s -X POST http://localhost:18080/v1/responses \
  -H "Authorization: Bearer sk-codextest1234567890abcdef" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-5.4","input":"你好","stream":true}'
```

预期：SSE 流，有 `response.output_text.delta` 事件带内容，无 `codex.rate_limits` 事件。

### 3.4 连发稳定性测试

```bash
for i in 1 2 3 4 5; do
  RESP=$(curl -s -X POST http://localhost:18080/v1/responses \
    -H "Authorization: Bearer sk-codextest1234567890abcdef" \
    -H "Content-Type: application/json" \
    -d "{\"model\":\"gpt-5.4\",\"input\":\"Reply with: WORD$i\",\"stream\":false}")
  echo "请求 $i: $(echo "$RESP" | python3 -c "import json,sys;d=json.load(sys.stdin);print([c.get('text','') for o in d.get('output',[]) for c in o.get('content',[])])" 2>/dev/null)"
  redis-cli DEL "apikey:auth:faf876203d54998bb3cc6c9ef4333df7c442a6f17d7f7757ccf283a3ba931a7f"
done
```

预期：5/5 都返回内容。

### 3.5 单元测试

```bash
cd /Users/luzhipeng/projects/sub2api/backend

# prewarm 相关测试
go test ./internal/service/ -run "Prewarm|EnsureOpenAI|HTTPIngressWSV2Bypass|ShouldKeepIngress" -v

# 全量 service 测试
go test ./internal/service/... -count=1

# repository 测试（需 -tags unit）
go test -tags unit ./internal/repository/ -run "PrewarmSession" -v
```

### 3.6 查看日志

```bash
# 实时日志（如果服务在前台运行直接看输出）
# 如果后台运行，重定向到了文件：
tail -f /tmp/sub2api_server.log

# 关键日志关键字
grep "prewarm_session" /tmp/sub2api_server.log
```

关键日志含义：
| 日志 | 含义 |
|------|------|
| `prewarm_session_inject account_id=X ... source=cache` | cache 命中，用预存的 id |
| `prewarm_session_inject account_id=X ... source=fallback` | cache 未命中，同步兜底预热 |
| `prewarm_session_done ... response_id=resp_xxx` | 预热成功拿到 id |
| `prewarm_session_roll_update` | 续接成功，滚动更新 id |
| `prewarm_session_invalidate` | 404 失效，清 store |
| `usage_limit_reached` | 没绕过限额（prewarm 没生效） |

---

## 四、远程服务器部署

### 4.1 服务器信息

| 项 | 值 |
|---|---|
| IP | 199.127.60.139 |
| SSH 账号 | Chatify |
| SSH 密码 | sk-chatify-MoLu154! |
| 服务端口 | 18081 |
| 项目目录 | ~/projects/sub2api |
| 部署方式 | Docker Compose（PG + Redis + sub2api 三个容器） |

SSH 连接（密码含特殊字符，用 sshpass）：
```bash
export SSHPASS='sk-chatify-MoLu154!'
sshpass -e ssh -o StrictHostKeyChecking=no -o PreferredAuthentications=password -o PubkeyAuthentication=no Chatify@199.127.60.139 '<命令>'
```

### 4.2 容器架构

```
docker-compose.sub2api.yml（~/projects/sub2api/ 下）
├── sub2api-pg      (postgres:16-alpine, named volume, 内部 5432)
├── sub2api-redis   (redis:7-alpine, 仅容器网络)
└── sub2api-app     (本地构建的镜像, 18081:8080)
    └── 网络: sub2api-net（独立桥接网络，不碰其他服务）
```

### 4.3 重新制作镜像 + 部署（完整流程）

> 当代码有更新时，执行以下步骤。

```bash
export SSHPASS='sk-chatify-MoLu154!'
SSH() { sshpass -e ssh -o StrictHostKeyChecking=no -o PreferredAuthentications=password -o PubkeyAuthentication=no Chatify@199.127.60.139 "$@"; }

# 1. 拉取最新代码
SSH 'cd ~/projects/sub2api && git pull origin feat/prewarm-session-bypass'

# 2. 重新构建镜像
SSH 'cd ~/projects/sub2api && docker compose -f docker-compose.sub2api.yml -p sub2api build sub2api'

# 3. 重启容器（用新镜像）
SSH 'cd ~/projects/sub2api && docker compose -f docker-compose.sub2api.yml -p sub2api up -d --force-recreate sub2api'

# 4. 等待启动（约 10 秒）
sleep 12

# 5. 等 prewarm worker 预热（约 60 秒）
echo "等 worker 预热..."
sleep 65

# 6. 清限流状态（让调度可选）
SSH 'docker exec sub2api-pg psql -U sub2api -d sub2api -c "UPDATE accounts SET schedulable=true, rate_limited_at=NULL, rate_limit_reset_at=NULL, temp_unschedulable_until=NULL WHERE deleted_at IS NULL;"'

# 7. 清 api-key 缓存
SSH 'docker exec sub2api-redis redis-cli FLUSHDB'
```

### 4.4 服务器测试

```bash
export SSHPASS='sk-chatify-MoLu154!'
SSH() { sshpass -e ssh -o StrictHostKeyChecking=no -o PreferredAuthentications=password -o PubkeyAuthentication=no Chatify@199.127.60.139 "$@"; }

# 非流式测试
SSH 'curl -s -X POST http://localhost:18081/v1/responses \
  -H "Authorization: Bearer sk-codextest1234567890abcdef" \
  -H "Content-Type: application/json" \
  -d "{\"model\":\"gpt-5.4\",\"input\":\"你好\",\"stream\":false}"'

# 流式测试
SSH 'curl -s -X POST http://localhost:18081/v1/responses \
  -H "Authorization: Bearer sk-codextest1234567890abcdef" \
  -H "Content-Type: application/json" \
  -d "{\"model\":\"gpt-5.4\",\"input\":\"你好\",\"stream\":true}"'

# 查看日志
SSH 'docker logs sub2api-app --tail 30'

# 查 prewarm 相关日志
SSH 'docker logs sub2api-app 2>&1 | grep prewarm_session | tail -10'

# 查容器状态
SSH 'docker ps --filter name=sub2api --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"'
```

### 4.5 外部访问（公网）

```bash
curl -s -X POST http://199.127.60.139:18081/v1/responses \
  -H "Authorization: Bearer sk-codextest1234567890abcdef" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-5.4","input":"你好","stream":false}'
```

### 4.6 后台管理

浏览器打开 http://199.127.60.139:18081
- 账号：admin@local.com
- 密码：admin123456
- 首次需确认合规声明

---

## 五、代码更新发布流程

从本地改代码到服务器生效的完整流程：

```bash
cd /Users/luzhipeng/projects/sub2api/backend

# 1. 本地编译验证
go build -tags embed -o bin/server ./cmd/server

# 2. 本地测试
go test ./internal/service/ -run "Prewarm|EnsureOpenAI" -count=1

# 3. 提交代码
cd /Users/luzhipeng/projects/sub2api
git add -A
git commit -m "fix(prewarm): 描述你的改动"
git push origin feat/prewarm-session-bypass

# 4. 服务器拉取 + 重建 + 重启（见 4.3）
```

---

## 六、添加新账号

### 方式 1：用 API（推荐，自动配齐所有字段）

```bash
export SSHPASS='sk-chatify-MoLu154!'
SSH() { sshpass -e ssh -o StrictHostKeyChecking=no -o PreferredAuthentications=password -o PubkeyAuthentication=no Chatify@199.127.60.139 "$@"; }

SERVER="http://localhost:18081"  # 本地用 18080
TOKEN=$(curl -s -X POST $SERVER/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@local.com","password":"admin123456"}' \
  | python3 -c "import json,sys;print(json.load(sys.stdin)['data']['access_token'])")

# 1. 换 token 验证（写文件避免特殊字符转义）
python3 -c "
import json
print(json.dumps({
    'refresh_token': 'rt.1.AAAxxx...你的refresh_token...',
    'client_id': 'app_EMoamEEZ73f0CkXaXp7hrann'
}))
" > /tmp/refresh_req.json

curl -s -X POST $SERVER/api/v1/admin/openai/refresh-token \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/refresh_req.json > /tmp/refresh_resp.json

# 2. 创建账号（一次性配齐凭证 + WSv2 + 禁用配额暂停）
python3 -c "
import json
d=json.load(open('/tmp/refresh_resp.json'))['data']
print(json.dumps({
    'name': 'codex-pro-xxx',
    'platform': 'openai', 'type': 'oauth',
    'credentials': {
        'access_token': d['access_token'], 'refresh_token': d['refresh_token'],
        'id_token': d.get('id_token',''), 'chatgpt_account_id': d['chatgpt_account_id'],
        'chatgpt_user_id': d.get('chatgpt_user_id',''), 'organization_id': d.get('organization_id',''),
        'client_id': d.get('client_id',''), 'plan_type': d.get('plan_type',''),
        'email': d.get('email',''), 'subscription_expires_at': d.get('subscription_expires_at','')
    },
    'concurrency': 5, 'priority': 10, 'group_ids': [2],
    'extra': {
        'responses_websockets_v2_enabled': True,
        'openai_oauth_responses_websockets_v2_enabled': True,
        'openai_oauth_responses_websockets_v2_mode': 'ctx_pool',
        'auto_pause_5h_disabled': True, 'auto_pause_7d_disabled': True
    }
}))
" > /tmp/create_account.json

curl -s -X POST $SERVER/api/v1/admin/accounts \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/create_account.json
```

### 方式 2：后台 UI 添加

浏览器打开后台 → 账号管理 → 添加账号 → 平台 OpenAI → 类型 OAuth → 验证 Refresh Token。
> 注意：UI 添加后需要手动补 extra 配置（WSv2 + 禁用配额暂停），否则 prewarm 不生效。

### 账号必需的 extra 配置

```json
{
  "responses_websockets_v2_enabled": true,
  "openai_oauth_responses_websockets_v2_enabled": true,
  "openai_oauth_responses_websockets_v2_mode": "ctx_pool",
  "auto_pause_5h_disabled": true,
  "auto_pause_7d_disabled": true
}
```

---

## 七、常见问题排查

### Q: 请求返回 503 "no available accounts"
账号被限流标记排除了。清限流：
```bash
# 本地
psql -h localhost -U sub2api_user -d sub2api -c "UPDATE accounts SET rate_limited_at=NULL, rate_limit_reset_at=NULL, temp_unschedulable_until=NULL WHERE deleted_at IS NULL;"
redis-cli DEL "apikey:auth:faf876203d54998bb3cc6c9ef4333df7c442a6f17d7f7757ccf283a3ba931a7f"

# 服务器
export SSHPASS='sk-chatify-MoLu154!'
sshpass -e ssh -o StrictHostKeyChecking=no -o PreferredAuthentications=password -o PubkeyAuthentication=no Chatify@199.127.60.139 \
  'docker exec sub2api-pg psql -U sub2api -d sub2api -c "UPDATE accounts SET rate_limited_at=NULL, rate_limit_reset_at=NULL WHERE deleted_at IS NULL;"; docker exec sub2api-redis redis-cli FLUSHDB'
```
然后重启 sub2api-app（让 scheduler snapshot 重建）。

### Q: 请求返回 429 usage_limit_reached
prewarm 没生效。检查：
1. config 里 `prewarm_session_enabled: true` 和 `http_ingress_ws_v2_bypass_enabled: true`
2. 日志有无 `prewarm_session_inject`
3. 账号 extra 有无 `responses_websockets_v2_enabled: true`

### Q: 请求返回 "previous response not found"
prewarm_id 失效（已被消费）。正常现象，recover 会重新预热重试。如果持续失败：
```bash
# 清 prewarm store，让 worker 重新预热
redis-cli DEL $(redis-cli KEYS "*prewarm*" | tr '\n' ' ')
```

### Q: output 为空数组
非流式 output 组装问题。确认用的是最新代码（`output_item.done` 收集逻辑）。

### Q: 流式有 codex.rate_limits 事件
确认用的是最新代码（过滤逻辑在 buffered 阶段 + emit 阶段双重过滤）。

### Q: 首次启动后 prewarm 没预热
worker 周期 60s，启动后等一个周期。或重启服务（worker 启动时立即跑一次）。

### Q: SSH 连接失败 "Permission denied"
密码含特殊字符，用 sshpass + 环境变量：
```bash
export SSHPASS='sk-chatify-MoLu154!'
sshpass -e ssh -o PreferredAuthentications=password -o PubkeyAuthentication=no Chatify@199.127.60.139 '<命令>'
```

---

## 八、Git 仓库信息

| 项 | 值 |
|---|---|
| Fork | https://github.com/luzhipeng728/sub2api |
| 功能分支 | `feat/prewarm-session-bypass` |
| 上游（原仓库） | https://github.com/Wei-Shaw/sub2api（remote: upstream） |
| 同步上游更新 | `git fetch upstream && git checkout main && git merge upstream/main` |

拉取代码：
```bash
git clone -b feat/prewarm-session-bypass https://github.com/luzhipeng728/sub2api.git
```

---

## 九、核心文件速查

| 文件 | 看什么 |
|------|--------|
| `backend/internal/service/openai_ws_prewarm_session.go` | prewarm 发送、prompt 转换、响应清洗 |
| `backend/internal/service/openai_ws_forwarder.go` | 请求注入、非流式 output 组装、事件过滤 |
| `backend/internal/service/openai_gateway_service.go` | HTTP bypass、Forward 层注入、codex transform 跳过 |
| `backend/internal/service/openai_ws_prewarm_session_store.go` | 存储接口 + 默认实现 |
| `backend/internal/service/openai_ws_prewarm_session_service.go` | 后台 worker |
| `backend/internal/config/config.go` | 配置开关 |
| `PREWARM_CHANGES.html` | 改动总结（可截图展示） |

---

## 十、prewarm 绕限额机制原理

```
prewarm 轮（后台 worker / 请求兜底）:
  payload = { input: [], generate: false, store: false }
  → 0 token，不计配额 → 拿到 response_id（存 Redis）

正式请求轮（用户每次请求）:
  1. cache 查 prewarm_id（命中直接用 / 未命中同步兜底预热）
  2. 注入 previous_response_id = prewarm_id（写入 wsReqBody）
  3. prompt 转 developer-role（不计 user 配额统计）
  4. instructions = 空格（不注入 5KB Codex prompt）
  5. 发给上游 WSv2 → 上游当作"续接进行中的 response" → 不查限额 → 照常生成
  6. 响应清洗：previous_response_id/instructions 删除，codex.rate_limits 过滤
  7. 成功 → 滚动更新（新 id 写回）；失败(404) → 失效自愈（清 store 重新预热）
```
