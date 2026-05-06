import sys
import json
import os
import requests
import time
import base64
import random
import threading
import websocket
import concurrent.futures  # Thư viện quản lý thread
from datetime import datetime, timezone

# --- CẤU HÌNH ---
HEADERS = {
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://freegen.app",
    "referer": "https://freegen.app/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}
IMAGES_DIR = "Images"
MAX_WORKERS = 10  # Số lượng thread chạy song song

# --- CÁC HÀM XỬ LÝ API (Giữ nguyên logic cũ) ---

def get_signature(prompt):
    url = "https://prompt-signer.freegen.app/"
    headers = HEADERS.copy()
    headers["content-type"] = "application/json"
    response = requests.post(url, headers=headers, json={"prompt": prompt}, timeout=30)
    return response.json()

def start_image_generation(prompt, ts, sig):
    url = "https://image-generator.freegen.app/"
    headers = HEADERS.copy()
    headers["content-type"] = "application/json"
    payload = {"prompt": prompt, "ts": ts, "sig": sig, "ratio_id": "16:9"}
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    return response.json()

def save_base64_image(base64_string, filename):
    if "," in base64_string:
        base64_string = base64_string.split(",")[1]
    with open(filename, "wb") as fh:
        fh.write(base64.b64decode(base64_string))

class ImageWebsocketClient:
    def __init__(self, job_id, auth_token):
        self.job_id = job_id
        self.auth_token = auth_token
        self.ws_url = "wss://websocket-bridge.freegen.app/ws"
        self.is_completed = False
        self.image_base64 = None

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
    """Hàm xử lý cho một prompt đơn lẻ"""
    print(f"🎨 [Thread Started] Đang xử lý ảnh {index}...")
    try:
        # 1. Lấy Signature
        sign_data = get_signature(prompt)
        ts, sig = sign_data["ts"], sign_data["sig"]
        
        # 2. Tạo Auth Token
        random_hex = ''.join(random.choices('0123456789abcdef', k=15))
        auth_prefix = base64.b64encode(random_hex.encode('utf-8')).decode('utf-8')
        auth_token = f"{auth_prefix}:{ts}"
        
        # 3. Gửi lệnh tạo ảnh
        gen_data = start_image_generation(prompt, ts, sig)
        job_id = gen_data.get("job_id")
        
        if not job_id:
            print(f"❌ [Lỗi] Không lấy được job_id cho ảnh {index}")
            return False

        # 4. Chờ qua WebSocket
        ws_client = ImageWebsocketClient(job_id, auth_token)
        ws_thread = threading.Thread(target=ws_client.connect)
        ws_thread.start()

        timeout = 120 # Tăng timeout lên 120s cho chắc chắn
        while not ws_client.is_completed and timeout > 0:
            time.sleep(2)
            timeout -= 2

        if ws_client.image_base64:
            filename = os.path.join(IMAGES_DIR, f"{index}_s1.jpg")
            save_base64_image(ws_client.image_base64, filename)
            print(f"✅ [Xong] Ảnh {index} đã lưu.")
            return True
        else:
            print(f"❌ [Timeout] Ảnh {index} không phản hồi.")
    except Exception as e:
        print(f"❌ [Lỗi Hệ Thống] Ảnh {index}: {e}")
    return False

# --- ĐIỀU KHIỂN ĐA LUỒNG ---

def main():
    if len(sys.argv) < 2:
        print("Thiếu dữ liệu đầu vào.")
        return

    try:
        input_data = json.loads(sys.argv[1])
    except Exception as e:
        print(f"Lỗi phân giải JSON: {e}")
        return

    if not os.path.exists(IMAGES_DIR):
        os.makedirs(IMAGES_DIR)

    # Chuyển đổi input_data sang danh sách để ThreadPool dễ xử lý
    # input_data có dạng {"index": "prompt", ...}
    tasks = [(idx, p) for idx, p in input_data.items()]

    print(f"🚀 Bắt đầu xử lý Batch với {MAX_WORKERS} threads song song...")

    # Sử dụng ThreadPoolExecutor để quản lý 10 threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Gửi tất cả các task vào pool
        futures = {executor.submit(process_single_prompt, idx, p): idx for idx, p in tasks}
        
        # Chờ các task hoàn thành
        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Thread xử lý ảnh {idx} gặp lỗi nghiêm trọng: {e}")

    print("🏁 Batch đã được xử lý xong.")

if __name__ == "__main__":
    main()
