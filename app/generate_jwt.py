import jwt
import time
import os
import requests

# 環境変数からApp IDと秘密鍵を取得
app_id = os.getenv("APP_ID")
private_key = os.getenv("PRIVATE_KEY").replace("\\n", "\n")  # PEM形式の鍵

# JWTトークンの生成
payload = {
    "iat": int(time.time()),
    "exp": int(time.time()) + (10 * 60),  # 10分間有効
    "iss": app_id
}
jwt_token = jwt.encode(payload, private_key, algorithm="RS256")

# アクセストークンの取得
installation_id = os.getenv("INSTALLATION_ID")
headers = {
    "Authorization": f"Bearer {jwt_token}",
    "Accept": "application/vnd.github.v3+json"
}
response = requests.post(
    f"https://api.github.com/app/installations/{installation_id}/access_tokens",
    headers=headers
)

if response.status_code == 201:
    access_token = response.json()["token"]
    with open("artifacts/access_token.txt", "w") as f:
        f.write(access_token)
    print("Access token generated and saved.")
else:
    print(f"Failed to get access token: {response.status_code}")
    print(response.json())
    exit(1)
