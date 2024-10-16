import os
import sys
import jwt
import time

# 環境変数から App ID と秘密鍵を取得
app_id = os.getenv("APP_ID")
private_key = os.getenv("PRIVATE_KEY")

# 環境変数が正しく取得できているか確認
if not app_id:
    print("Error: APP_ID environment variable not found.")
    sys.exit(1)

if not private_key:
    print("Error: PRIVATE_KEY environment variable not found.")
    sys.exit(1)

# 改行を復元
private_key = private_key.replace("\\n", "\n")

# JWTペイロードの設定
payload = {
    # JWTの発行時間と有効期限（10分間）
    "iat": int(time.time()),  # 発行時間
    "exp": int(time.time()) + (10 * 60),  # 有効期限（10分）
    "iss": app_id  # App ID
}

# JWTトークンの生成
try:
    jwt_token = jwt.encode(payload, private_key, algorithm="RS256")
    print("JWT token generated successfully.")
    print(jwt_token)
except Exception as e:
    print(f"Failed to generate JWT token: {e}")
    sys.exit(1)
