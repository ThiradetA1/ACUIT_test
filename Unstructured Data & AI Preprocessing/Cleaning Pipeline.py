import os
import pandas as pd
from email.parser import Parser
import os
import re
import pandas as pd
from email.parser import Parser


def parse_enron_emails(root_directory, max_files=1000):
    email_list = []
    
    # วนลูปอ่านไฟล์ในโฟลเดอร์ maildir
    for root, dirs, files in os.walk(root_directory):
        for filename in files:
            if len(email_list) >= max_files:
                break
                
            file_path = os.path.join(root, filename)
            
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    # ใช้ Parser สกัดโครงสร้าง Email
                    msg = Parser().parsestr(f.read())
                    
                    # Extract Headers
                    email_from = msg.get('From')
                    email_to = msg.get('To')
                    email_date = msg.get('Date')
                    
                    # Extract & Clean Body
                    payload = msg.get_payload()
                    if isinstance(payload, list): # กรณีเป็น multipart
                        payload = payload[0].get_payload()
                    
                    # ทำความสะอาดเบื้องต้น: ลบ Newlines ส่วนเกิน และ Whitespace
                    cleaned_body = " ".join(payload.split())
                    
                    email_list.append({
                        "Date": email_date,
                        "From": email_from,
                        "To": email_to,
                        "Body": cleaned_body
                    })
            except Exception as e:
                continue # ข้ามไฟล์ที่มีปัญหา
                
    return pd.DataFrame(email_list)

def advanced_body_cleaner(text):
    """
    ฟังก์ชันสำหรับลบ Noise (Forwarded, Signature, Disclaimer) ออกจากเนื้อหาอีเมล
    """
    if not text:
        return ""
    
    # 1. ลบ Forwarded Message Headers (เช่น -----Original Message----- หรือ From: ... Sent: ...)
    text = re.sub(r'(-+\s*Original Message\s*-+|From:.*|To:.*|Sent:.*|Subject:.*)', '', text, flags=re.IGNORECASE)
    
    # 2. ลบ Disclaimer Footers (คำเตือนทางกฎหมายที่พบบ่อยใน Enron)
    disclaimer_keywords = [
        r"This message is intended only for the use of the Addressee",
        r"privileged and confidential",
        r"If you are not the intended recipient",
        r"Any review, retransmission, dissemination",
        r"Information contained in this email may be confidential"
    ]
    for pattern in disclaimer_keywords:
        text = re.sub(pattern + ".*", "", text, flags=re.IGNORECASE | re.DOTALL)

    # 3. ลบ Signature Blocks เบื้องต้น (มองหาการขึ้นต้นด้วยขีดหรือช่องว่างเยอะๆ ก่อนชื่อ)
    # เช่น -- \n John Doe หรือ Best Regards,
    text = re.sub(r'(--|__|\n\s*\n)(?=[^ \n]*$).*', '', text, flags=re.DOTALL)
    text = re.sub(r'(Best regards|Regards|Thanks|Sincerely|Thank you),?.*', '', text, flags=re.IGNORECASE | re.DOTALL)

    # 4. Whitespace Normalization
    text = " ".join(text.split())
    
    return text.strip()

def parse_enron_to_llm_ready(root_dir, limit=1000):
    email_list = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if len(email_list) >= limit: break
            
            try:
                with open(os.path.join(root, file), 'r', encoding='utf-8', errors='ignore') as f:
                    msg = Parser().parsestr(f.read())
                    
                    # Extract Data
                    raw_body = msg.get_payload()
                    if isinstance(raw_body, list): raw_body = raw_body[0].get_payload()
                    
                    # Apply Advanced Cleaning
                    cleaned_body = advanced_body_cleaner(str(raw_body))
                    
                    # ข้ามอีเมลที่หลัง Clean แล้วไม่เหลือเนื้อหา (Empty Body)
                    if len(cleaned_body) < 10: continue 

                    email_list.append({
                        "Date": msg.get('Date'),
                        "From": msg.get('From'),
                        "To": msg.get('To'),
                        "Cleaned_Body": cleaned_body
                    })
            except: continue
                
    return pd.DataFrame(email_list)

import tiktoken

def sliding_window_chunking(text, window_size=400, overlap=50, model="gpt-3.5-turbo"):
    """
    แบ่งข้อความอีเมลเป็น Chunks โดยคำนวณตามจำนวน Token จริง
    """
    # 1. โหลด Tokenizer ของโมเดลที่ต้องการใช้
    enc = tiktoken.encoding_for_model(model)
    tokens = enc.encode(text)
    
    # ถ้าจำนวน Token ทั้งหมดไม่เกิน Window Size ให้คืนค่าเป็น Chunk เดียว
    if len(tokens) <= window_size:
        return [text]
    
    chunks = []
    
    # 2. เริ่มทำ Sliding Window
    # ขยับจุดเริ่มทีละ (window_size - overlap)
    step = window_size - overlap
    
    for i in range(0, len(tokens), step):
        # ดึง Tokens ตามช่วงที่กำหนด
        chunk_tokens = tokens[i : i + window_size]
        
        # แปลง Tokens กลับเป็นข้อความ (Decode)
        chunk_text = enc.decode(chunk_tokens)
        chunks.append(chunk_text)
        
        # หยุดเมื่อคุมถึง Token สุดท้าย
        if i + window_size >= len(tokens):
            break
            
    return chunks

# 1. รัน Pipeline หลัก
path_to_maildir = r'D:\ACUIT_test\Unstructured Data & AI Preprocessing\maildir'
df = parse_enron_to_llm_ready(path_to_maildir, limit=2000)

# 2. ทำ Chunking
df['Chunks'] = df['Cleaned_Body'].apply(lambda x: sliding_window_chunking(x, window_size=400, overlap=50))

# 3. ขยาย List ของ Chunks ให้เป็นแถวใหม่ (Explode)
df_final = df.explode('Chunks').reset_index(drop=True)

# 4. ตรวจสอบผลลัพธ์
print(f"จำนวนชิ้นข้อมูล (Chunks) ทั้งหมดที่พร้อมส่งให้ AI: {len(df_final)}")
print(df_final[['From', 'Chunks']].head())

# 5. บันทึกผล
df_final.to_csv('enron_llm_ready.csv', index=False)

df = parse_enron_emails(r'D:\ACUIT_test\Unstructured Data & AI Preprocessing\maildir')
df.to_csv('cleaned_enron.csv', index=False)

