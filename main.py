# AquaGuard - hidden water leak detector (ESP32 + MicroPython, runs in Wokwi)
# Flow pot  -> GPIO34   Pressure pot -> GPIO35
# Relay     -> GPIO26   (relay ON = main valve CLOSED)
# LED       -> GPIO27   Buzzer -> GPIO25   Reset button -> GPIO14
import network, time, json
print("boot")
from machine import Pin, ADC
from umqtt.simple import MQTTClient

# ---------- config ----------
DEVICE = "home01"
BASE = "aquaguard-mlh-x7k2q9/" + DEVICE         # unique prefix (public broker!). Dashboard Topic = aquaguard-mlh-x7k2q9/home01
T_TELE, T_EVT, T_CMD = BASE + "/telemetry", BASE + "/event", BASE + "/cmd"
BROKER = "broker.hivemq.com"

SAMPLE_MS = 1000
ZERO_FLOW = 0.05     # L/min: at or below this = taps closed
LEAK_MAX = 1.5       # L/min: above this = normal usage, not a leak
BURST_FLOW = 12.0    # L/min
BURST_DROP = 15.0    # psi below learned static pressure
P_HINT_DROP = 3.0    # psi drop that supports a leak diagnosis
# DEMO timings (seconds). For a real home use e.g. 1800 / 7200.
SUSPECT_S = 20
CONFIRM_S = 45

NORMAL, SUSPECT, LEAK, BURST = "NORMAL", "SUSPECT", "LEAK", "BURST"

# ---------- hardware ----------
def adc(pin):
    a = ADC(Pin(pin))
    a.atten(ADC.ATTN_11DB)          # full 0-3.3 V range
    return a

flow_adc, pres_adc = adc(34), adc(35)
relay, led, buzzer = Pin(26, Pin.OUT), Pin(27, Pin.OUT), Pin(25, Pin.OUT)
btn = Pin(14, Pin.IN, Pin.PULL_UP)
reset_flag = False

def _on_btn(_):
    global reset_flag
    reset_flag = True
btn.irq(trigger=Pin.IRQ_FALLING, handler=_on_btn)

def read_flow():
    # squared mapping gives fine resolution at the low end (the leak zone):
    # pot ~15% -> ~0.3 L/min, ~30% -> ~1.3 L/min, 100% -> 15 L/min
    r = flow_adc.read() / 4095
    f = 15.0 * r * r
    return 0.0 if f < 0.02 else f

def read_pressure():
    return pres_adc.read() / 4095 * 100.0        # 0-100 psi

# ---------- anomaly detector ----------
class LeakDetector:
    def __init__(self):
        self.reset()

    def reset(self):
        self.low_s = 0          # seconds of continuous steady low flow
        self.hist = []          # last 15 flow samples
        self.p_base = None      # learned static pressure (no-flow)

    def update(self, flow, pres, dt):
        if flow <= ZERO_FLOW:   # learn "resting" pressure only when water is still
            self.p_base = pres if self.p_base is None else 0.9 * self.p_base + 0.1 * pres
        drop = (self.p_base - pres) if self.p_base is not None else 0.0

        self.hist.append(flow)
        if len(self.hist) > 15:
            self.hist.pop(0)

        # 1) Burst: huge flow AND pressure collapse -> act immediately
        if flow >= BURST_FLOW and drop >= BURST_DROP:
            return BURST, 100

        # 2) Hidden leak: flow never returns to zero, stays low, and is steady
        in_band = ZERO_FLOW < flow < LEAK_MAX
        h = self.hist
        mean = sum(h) / len(h)
        stable = len(h) >= 5 and min(h) > ZERO_FLOW and (max(h) - min(h)) <= 0.4 * mean + 0.05
        if not in_band:
            self.low_s = 0      # a tap closing (zero) or real usage resets the clock
        elif stable:
            self.low_s += dt

        score = min(self.low_s / CONFIRM_S, 1.0) * 70
        score += 10 if (in_band and stable) else 0
        score += 20 if (in_band and drop >= P_HINT_DROP) else 0

        if self.low_s >= CONFIRM_S:
            return LEAK, min(int(score), 100)
        if self.low_s >= SUSPECT_S:
            return SUSPECT, min(int(score), 100)
        return NORMAL, min(int(score), 100)

# ---------- networking ----------
def wifi():
    sta = network.WLAN(network.STA_IF)
    sta.active(True)
    print("connecting to Wokwi-GUEST...")
    sta.connect("Wokwi-GUEST", "")          # no channel= argument on MicroPython 1.22
    for i in range(40):
        if sta.isconnected():
            print("WiFi OK", sta.ifconfig()[0])
            return
        print("waiting", i, sta.status())
        time.sleep(1)
    raise RuntimeError("WiFi failed - check simulator network")

client = None

def on_msg(topic, msg):
    global reset_flag, manual_close
    m = msg.decode().strip().lower()
    if m in ("reset", "open"):
        reset_flag = True
    elif m == "close":
        manual_close = True

def mqtt_up():
    global client
    try:
        c = MQTTClient("aquaguard-" + DEVICE, BROKER, keepalive=30)
        c.set_callback(on_msg)
        c.connect()
        c.subscribe(T_CMD)
        client = c
        print("MQTT OK")
    except Exception as e:
        client = None
        print("MQTT down:", e)

def pub(topic, payload):
    global client
    if client is None:
        return
    try:
        client.publish(topic, json.dumps(payload))
    except Exception:
        client = None

# ---------- main loop ----------
manual_close = False

def set_valve(closed):
    relay.value(1 if closed else 0)

def main():
    global reset_flag, manual_close
    wifi()
    mqtt_up()
    det = LeakDetector()
    state, score, latched = NORMAL, 0, False
    leaked_l, up_s, last, last_retry, tick = 0.0, 0, time.ticks_ms(), time.ticks_ms(), 0
    set_valve(False)

    while True:
        # poll commands often, sample once per SAMPLE_MS
        if client:
            try:
                client.check_msg()
            except Exception:
                mqtt_up()
        now = time.ticks_ms()
        if time.ticks_diff(now, last) < SAMPLE_MS:
            time.sleep_ms(50)
            continue
        last = now
        dt = SAMPLE_MS / 1000
        up_s += 1
        tick += 1

        if client is None and time.ticks_diff(now, last_retry) > 10000:
            last_retry = now
            mqtt_up()           # safety logic below keeps working while offline

        if reset_flag:
            reset_flag, latched, manual_close = False, False, False
            det.reset(); leaked_l = 0.0
            state, score = NORMAL, 0
            set_valve(False)
            pub(T_EVT, {"type": "RESET", "msg": "Valve reopened, detector reset", "up": up_s})

        valve_closed = relay.value() == 1
        raw_flow = read_flow()
        pres = read_pressure()
        flow = 0.0 if valve_closed else raw_flow    # closed valve = no water moves

        if manual_close and not latched:
            latched = True
            set_valve(True)
            pub(T_EVT, {"type": "MANUAL", "msg": "Valve closed from dashboard", "up": up_s})
            manual_close = False

        if not latched:
            new_state, score = det.update(flow, pres, dt)
            if new_state in (SUSPECT, LEAK, BURST):
                leaked_l += flow / 60 * dt
            if new_state != state:
                pub(T_EVT, {"type": new_state, "flow": round(flow, 2), "psi": round(pres, 1),
                            "score": score, "msg": "State changed to " + new_state, "up": up_s})
            state = new_state
            if state in (LEAK, BURST):      # AUTO SHUT-OFF
                latched = True
                set_valve(True)
                pub(T_EVT, {"type": "SHUTOFF",
                            "msg": "Main valve closed automatically (%s)" % state,
                            "lost_l": round(leaked_l, 2), "up": up_s})

        # local indicators (work even without Wi-Fi)
        if latched:
            led.value(1)
            buzzer.value(tick % 2)          # 0.5 Hz beeping until reset
        elif state == SUSPECT:
            led.value(tick % 2)
            buzzer.value(0)
        else:
            led.value(0)
            buzzer.value(0)

        pub(T_TELE, {"up": up_s, "flow": round(flow, 2), "psi": round(pres, 1),
                     "state": state, "score": score, "valve": "CLOSED" if relay.value() else "OPEN",
                     "lost_l": round(leaked_l, 2), "low_s": det.low_s})

main()
