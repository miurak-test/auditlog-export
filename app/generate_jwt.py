import os
import sys
import jwt
import time

# 環境変数からApp IDと秘密鍵を取得
app_id = os.getenv("APP_ID")
private_key = os.getenv("APP_PRIVATE_KEY")

# 環境変数が設定されているか確認
if not app_id:
    print("Error: APP_ID environment variable not found.")
    sys.exit(1)

if not private_key:
    print("Error: APP_PRIVATE_KEY environment variable not found.")
    sys.exit(1)

# 改行文字を復元
private_key = private_key.replace("\\n", "\n")

# JWT ペイロードを設定
payload = {
    "iat": int(time.time()),  # 発行時刻
    "exp": int(time.time()) + (10 * 60),  # 有効期限は10分後
    "iss": app_id  # 発行者はApp ID
}

# JWT トークンを生成
try:
    jwt_token = jwt.encode(payload, private_key, algorithm="RS256")
    print(jwt_token)  # 標準出力にトークンを表示
except Exception as e:
    print(f"Failed to generate JWT token: {e}")
    sys.exit(1)
