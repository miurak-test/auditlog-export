import os
import jwt
import time
import sys

# 環境変数からApp IDと秘密鍵を取得
app_id = os.getenv("APP_ID")
private_key = os.getenv("APP_PRIVATE_KEY")

# エラーハンドリング：環境変数が設定されているか確認
if not app_id or not private_key:
    print("Error: APP_ID or APP_PRIVATE_KEY is not set.")
    sys.exit(1)

# 環境変数のシングルライン形式の鍵を復元
private_key = private_key.replace("\\n", "\n")

# JWTトークンの生成
try:
    payload = {
        "iat": int(time.time()),  # 発行時刻
        "exp": int(time.time()) + (10 * 60),  # 有効期限（10分）
        "iss": app_id  # アプリケーションID
    }
    jwt_token = jwt.encode(payload, private_key, algorithm="RS256")
    print(jwt_token)  # トークンを出力
except Exception as e:
    print(f"Failed to generate JWT token: {e}")
    sys.exit(1)
