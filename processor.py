import sys
import json
import os
import requests
import time
import base64
import random
import threading
import websocket
import concurrent.futures

# --- CẤU HÌNH ---
HEADERS = {
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://freegen.app",
    "referer": "https://freegen.app/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}
IMAGES_DIR = "Images"
MAX_WORKERS = 8 # Chạy song song 10 ảnh cùng lúc

# --- HÀM API ---
def get_signature(prompt):
    url = "https://prompt-signer.freegen.app/"
    res = requests.post(url, headers=HEADERS, json={"prompt": prompt}, timeout=30)
    return res.json()

def start_image_generation(prompt, ts, sig):
    url = "https://image-generator.freegen.app/"
    payload = {"prompt": prompt, "ts": ts, "sig": sig, "ratio_id": "16:9"}
    res = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    return res.json()

def save_base64_image(base64_string, filename):
    if "," in base64_string:
        base64_string = base64_string.split(",")[1]
    with open(filename, "wb") as fh:
        fh.write(base64.b64decode(base64_string))

class ImageWebsocketClient:
    def __init__(self, job_id, auth_token):
        self.job_id, self.auth_token = job_id, auth_token
        self.ws_url = "wss://websocket-bridge.freegen.app/ws"
        self.is_completed, self.image_base64 = False, None

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("type") == "result":
                self.image_base64 = data.get("image_data")
                self.is_completed = True
                ws.close()
        except: pass

    def on_open(self, ws):
        ws.send(json.dumps({"type": "subscribe", "job_id": self.job_id, "auth": self.auth_token}))

    def connect(self):
        ws = websocket.WebSocketApp(
            self.ws_url, 
            header=["Origin: https://freegen.app"],
            on_open=self.on_open, 
            on_message=self.on_message
        )
        ws.run_forever()

def process_single_prompt(index, prompt):
    """Xử lý đơn lẻ cho một prompt"""
    print(f"🎨 Đang xử lý index {index}...")
    try:
        sign_data = get_signature(prompt)
        ts, sig = sign_data["ts"], sign_data["sig"]
        
        # Tạo token xác thực
        rand_str = ''.join(random.choices('0123456789abcdef', k=15))
        auth_token = f"{base64.b64encode(rand_str.encode()).decode()}:{ts}"
        
        gen_data = start_image_generation(prompt, ts, sig)
        job_id = gen_data.get("job_id")
        if not job_id: return False

        # Chờ ảnh qua Websocket
        ws_client = ImageWebsocketClient(job_id, auth_token)
        threading.Thread(target=ws_client.connect).start()

        timeout = 120
        while not ws_client.is_completed and timeout > 0:
            time.sleep(2)
            timeout -= 2

        if ws_client.image_base64:
            os.makedirs(IMAGES_DIR, exist_ok=True)
            filename = os.path.join(IMAGES_DIR, f"{index}_s1.jpg")
            save_base64_image(ws_client.image_base64, filename)
            print(f"✅ Đã xong {index}")
            return True
    except Exception as e:
        print(f"❌ Lỗi index {index}: {e}")
    return False

# --- ĐIỀU KHIỂN CHÍNH ---
def main():
    if len(sys.argv) < 2: return
    input_arg = sys.argv[1]

    # Đọc dữ liệu từ file JSON hoặc chuỗi trực tiếp
    if os.path.exists(input_arg):
        with open(input_arg, 'r', encoding='utf-8') as f:
            input_data = json.load(f)
    else:
        input_data = json.loads(input_arg)

    # Chuyển đổi sang list để chạy ThreadPool
    tasks = [(idx, p) for idx, p in input_data.items()]

    print(f"🚀 Bắt đầu Batch với {MAX_WORKERS} luồng...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_prompt, idx, p): idx for idx, p in tasks}
        # Đợi tất cả hoàn thành
        concurrent.futures.wait(futures)

    print("🏁 Tất cả prompts trong lượt này đã xong.")

if __name__ == "__main__":
    main()
