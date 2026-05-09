import paho.mqtt.client as mqtt
import json
import time

# ==========================================
# 1. إعدادات السيرفر السحابي (HiveMQ Cloud)
# ==========================================
BROKER = "987a13e793ba4872bf72241d3f11ab25.s1.eu.hivemq.cloud" # ⚠️ ضع رابطك هنا
PORT = 8883
MQTT_USER = "mehina"             # ⚠️ ضع اسم المستخدم
MQTT_PASS = "mehina1M"       # ⚠️ ضع كلمة المرور

# ==========================================
# 2. سجلات الأسطول (Fleet Memory)
# ==========================================
# سيقوم السيرفر بإنشاء سجل لأي باص جديد تلقائياً هنا
fleet_state = {}       # لحفظ حالة كل باص (الحمولة، الموقع، النشاط)
fleet_requests = {}    # لحفظ طلبات الركاب لكل باص

# دالة مساعدة لإنشاء سجل للباص إذا لم يكن موجوداً
def init_bus_if_needed(bus_id):
    if bus_id not in fleet_state:
        fleet_state[bus_id] = {
            "passengers": 0, "lat": 0.0, "lng": 0.0, 
            "offset": 0, "raw_count": 0, "active": False, 
            "last_phone": 0, "last_gps": 0
        }
        fleet_requests[bus_id] = {}
        print(f"🆕 تم تسجيل باص جديد في السيرفر: {bus_id}")

# ==========================================
# 3. دوال النشر (مع الذاكرة الدائمة Retain)
# ==========================================
def publish_state(client, bus_id):
    state = fleet_state[bus_id]
    state_payload = {
        "bus_id": bus_id, 
        "passengers": state["passengers"],
        "lat": state["lat"], 
        "lng": state["lng"],
        "active": state["active"], 
        "last_gps_time": state["last_gps"]
    }
    client.publish(f"bus/{bus_id}/state", json.dumps(state_payload), retain=True)

def publish_requests(client, bus_id):
    all_reqs = list(fleet_requests[bus_id].values())
    client.publish(f"bus/{bus_id}/active_requests", json.dumps(all_reqs), retain=True)

# ==========================================
# 4. دوال الاتصال والاستقبال الديناميكي
# ==========================================
def on_connect(client, userdata, flags, rc):
    print("✅ تم الاتصال بـ HiveMQ السحابي بنجاح!")
    # 💡 الاشتراك الديناميكي: استماع لأي باص (+) ولأي نوع رسالة (+)
    client.subscribe("bus/+/+")
    print("🎧 السيرفر يستمع الآن لأسطول الباصات...")

def on_message(client, userdata, msg):
    try:
        # تفكيك مسار الرسالة لمعرفة (كود الباص) و (نوع الرسالة)
        # مثال: bus/BUS-01/phone -> parts[1] = BUS-01, parts[2] = phone
        parts = msg.topic.split('/')
        if len(parts) != 3: return
        
        bus_id = parts[1]
        msg_type = parts[2]
        
        # تجاهل الرسائل التي يقوم السيرفر نفسه بنشرها (لمنع الحلقات المفرغة)
        if msg_type in ["state", "active_requests"]: return

        payload = json.loads(msg.payload.decode("utf-8"))
        current_time = time.time()

        # التأكد من وجود الباص في الذاكرة
        init_bus_if_needed(bus_id)
        bus = fleet_state[bus_id]

        # أ) معالجة أوامر التحكم (تشغيل، إيقاف، مسح راكب)
        if msg_type == "control":
            action = payload.get("action")
            
            if action == "start":
                bus["active"] = True
                bus["offset"] = bus["raw_count"]
                print(f"🚀 بدء الرحلة للباص: {bus_id}")
                
            elif action == "stop":
                bus["active"] = False
                bus["passengers"] = 0
                fleet_requests[bus_id].clear() # تفريغ ركاب هذا الباص فقط
                publish_requests(client, bus_id)
                print(f"🛑 إيقاف الرحلة للباص: {bus_id}")
                
            elif action == "reset":
                bus["offset"] = bus["raw_count"]
                bus["passengers"] = 0
                print(f"🔄 تصفير عداد الباص: {bus_id}")
                
            elif action == "remove_passenger":
                p_id = payload.get("id")
                if p_id in fleet_requests[bus_id]:
                    del fleet_requests[bus_id][p_id]
                    publish_requests(client, bus_id)
                    print(f"✔️ صعود راكب في الباص: {bus_id}")
                    
            publish_state(client, bus_id)

        # ب) استقبال طلب ركوب جديد
        elif msg_type == "requests":
            p_id = str(payload.get("time")) + str(payload.get("lat"))
            payload["id"] = p_id
            fleet_requests[bus_id][p_id] = payload
            publish_requests(client, bus_id)
            print(f"🙋 طلب جديد للباص {bus_id}! (الإجمالي: {len(fleet_requests[bus_id])})")

        # ج) معالجة بيانات البوردة (ESP32 / Raspberry Pi)
        elif msg_type == "esp":
            bus["raw_count"] = payload.get("passengers", 0)
            if bus["active"]:
                if bus["raw_count"] < bus["offset"]: bus["offset"] = 0
                bus["passengers"] = max(0, bus["raw_count"] - bus["offset"])
                
                if (current_time - bus["last_phone"]) > 15 and payload.get("lat") != 0.0:
                    bus["lat"], bus["lng"] = payload["lat"], payload["lng"]
                    bus["last_gps"] = current_time
                publish_state(client, bus_id)

        # د) معالجة الـ GPS من الموبايل الخاص بالسائق
        elif msg_type == "phone" and bus["active"]:
            if payload.get("lat") != 0.0:
                bus["lat"], bus["lng"] = payload["lat"], payload["lng"]
                bus["last_phone"] = current_time
                bus["last_gps"] = current_time
            publish_state(client, bus_id)

    except Exception as e:
        print(f"❌ خطأ في معالجة الرسالة: {e}")

# ==========================================
# 5. تشغيل السيرفر
# ==========================================
client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.tls_set() # تفعيل التشفير
client.on_connect = on_connect
client.on_message = on_message

# بدء الاتصال وإبقاء السيرفر يعمل
client.connect(BROKER, PORT, 60)
client.loop_forever()