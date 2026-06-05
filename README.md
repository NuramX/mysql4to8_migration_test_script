# MySQL 4 → 8 Migration Validator

เครื่องมือตรวจสอบความถูกต้องของข้อมูลหลัง migrate จาก MySQL 4.0 (source) ไป MySQL 8 (target)
มี web dashboard แสดงผลแบบ real-time — เทียบ schema, row count, checksum, timestamp range
และสุ่มเทียบข้อมูลรายแถว (field-by-field) พร้อม export รายงาน mismatch เป็น CSV/Excel

> MySQL 4.0 ใช้ protocol เก่าที่ driver ปัจจุบันต่อไม่ได้ — โปรเจกต์นี้มี connector
> เขียนเองด้วย raw socket (`mysql40.py`) สำหรับฝั่ง source โดยเฉพาะ

## สิ่งที่ต้องมี

- Python 3.10 ขึ้นไป
- MySQL client library (สำหรับ build `mysqlclient` ที่ใช้ต่อฝั่ง target)
  - **macOS:** `brew install mysql-client pkg-config`
  - **Ubuntu/Debian:** `sudo apt install default-libmysqlclient-dev build-essential pkg-config`
  - **Windows:** ปกติ `pip install mysqlclient` มี wheel สำเร็จรูป ไม่ต้องลงอะไรเพิ่ม
- เครือข่ายต้องเข้าถึง DB ทั้ง source และ target ได้ (ถ้าอยู่นอก office ต้องต่อ VPN ก่อน)

## ติดตั้ง

```bash
git clone https://github.com/NuramX/mysql4to8_migration_test_script.git
cd mysql4to8_migration_test_script

# สร้าง virtual environment
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

## ตั้งค่า

มี 2 ทาง เลือกอย่างใดอย่างหนึ่ง:

**ทาง 1 — ตั้งผ่านหน้าเว็บ (ง่ายสุด):** รัน `python web_server.py` ได้เลยโดยไม่ต้องสร้าง
`config.json` ก่อน แล้วเปิด dashboard → เมนู Settings → กรอก host/user/password ของ
source และ target → กด Save (ระบบจะทดสอบการเชื่อมต่อก่อน ถ้าต่อไม่ได้จะไม่ save)
ค่าที่กรอกถูกเขียนลง `config.json` ให้อัตโนมัติ

**ทาง 2 — แก้ไฟล์เอง:** คัดลอกไฟล์ตัวอย่างแล้วใส่ข้อมูลเชื่อมต่อ DB ของคุณ
(จำเป็นถ้าจะใช้ CLI โดยไม่เปิด web server):

```bash
cp config.example.json config.json
```

แก้ `config.json`:

| ค่า | ความหมาย |
|---|---|
| `source` | host/port/user/password ของ MySQL 4.0 (ต้นทาง) |
| `target` | host/port/user/password ของ MySQL 8 (ปลายทาง) |
| `default_db` | ชื่อ database ที่เปิดมาให้เป็นค่าเริ่มต้นใน dashboard |
| `tuning.data_window_years` | ตรวจเฉพาะข้อมูล N ปีปฏิทินล่าสุด — เช่น 2 ในปี 2026 = ตรวจแถวตั้งแต่ 2024-01-01 (นับจาก column วันที่ใน PK; table ที่ไม่มี column วันที่ตรวจเต็มทั้ง table) ใส่ 0 = ปิด ตรวจเต็มทุก table |
| `tuning.workers` | จำนวน thread ที่ตรวจพร้อมกัน (ค่าแนะนำ 8) |
| `tuning.src_timeout` | timeout (วินาที) ของ query ฝั่ง source |
| `tuning.max_errors` | จำนวน mismatch สูงสุดที่เก็บลงรายงานต่อ table |
| `tuning.decimal_tol` | ค่าความคลาดเคลื่อนที่ยอมรับได้ของเลขทศนิยม |
| `tuning.hash_chunks` | จำนวน chunk ตอนทำ checksum เทียบข้อมูล |

> ⚠️ **ห้าม commit `config.json` ขึ้น git** — ไฟล์นี้มีรหัสผ่านจริง
> ใช้ user ที่มีสิทธิ์ **อ่านอย่างเดียว (read-only)** ก็เพียงพอ เครื่องมือนี้ไม่เขียนข้อมูลลง DB

## วิธีใช้

### Web Dashboard (วิธีหลัก)

```bash
python web_server.py
```

แล้วเปิดเบราว์เซอร์ไปที่ **http://localhost:5001**

ขั้นตอนใน dashboard:

1. เลือก database จาก dropdown มุมบน
2. กด **Run Validation** — ระบบจะไล่ตรวจทุก table แสดงผล PASS / WARN / FAIL แบบ real-time
3. หลังตรวจเสร็จ ระบบจะสุ่มเทียบข้อมูลรายแถว (auto-sample) ให้ทุก table อัตโนมัติ
4. คลิก table ใดก็ได้เพื่อดูรายละเอียด — schema, timestamp range, row count, ผล compare
5. table ที่ FAIL ดูรายการ mismatch ได้ในหน้า detail และดาวน์โหลดเป็น Excel ได้
6. สลับ database ได้โดยผลตรวจเดิมไม่หาย (เก็บแยกต่อ DB)

รายงานถูกเขียนไว้ที่ `static/reports/` (ลบ/เขียนทับอัตโนมัติทุกครั้งที่เริ่ม run ใหม่)

### Command Line (ไม่ใช้ web)

```bash
python validate_all_tables.py --db <database_name>
```

flag เสริม:

| flag | ความหมาย |
|---|---|
| `--db <name>` | ระบุ database ที่จะตรวจ (ไม่ใส่ = ใช้ `default_db` จาก config) |
| `--json` | พิมพ์ผลเป็น JSON ทีละบรรทัด (NDJSON) แทนข้อความปกติ |
| `--skip-sync` | ข้ามขั้นตอนเทียบข้อมูลละเอียด ตรวจแค่ schema/row count |

## โครงสร้างโปรเจกต์

```
web_server.py            Flask server + SSE stream + REST API (port 5001)
validate_all_tables.py   ตัวตรวจหลัก — รันเป็น subprocess จาก web server หรือรันเองก็ได้
mysql40.py               MySQL 4.0 connector (raw socket, old password protocol)
config.json              ข้อมูลเชื่อมต่อ DB (สร้างเองจาก config.example.json — ห้าม commit)
static/                  หน้าเว็บ dashboard
static/reports/          รายงานผลตรวจ (สร้างอัตโนมัติ)
```

## แก้ปัญหาที่เจอบ่อย

- **`pip install mysqlclient` ล้มเหลว** — ยังไม่ได้ลง MySQL client library ดูหัวข้อ "สิ่งที่ต้องมี"
  - macOS อาจต้องชี้ path เพิ่ม: `export PKG_CONFIG_PATH="$(brew --prefix mysql-client)/lib/pkgconfig"`
- **`ERROR: cannot load config.json`** — ยังไม่ได้สร้าง `config.json` หรือ JSON ผิด format
- **ต่อ source ไม่ได้ / timeout** — เช็ค VPN และ firewall ไปยัง host ฝั่ง source, ลองเพิ่ม `src_timeout`
- **port 5001 ชน** — แก้เลข port ที่บรรทัดสุดท้ายของ `web_server.py`
