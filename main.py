import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import datetime
import re
import urllib3
import json
from urllib.parse import urljoin
import os  # ★追加

# SSL警告を非表示
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. データ整形関数 ---
def clean_fixed_layout(df_raw):
    header_row_index = 2
    if len(df_raw) <= header_row_index:
        return df_raw
    new_header = df_raw.iloc[header_row_index].fillna("名称未設定")
    df_clean = df_raw.iloc[header_row_index + 1:].copy()
    df_clean.columns = new_header
    col_a = df_clean.iloc[:, 0].fillna('').astype(str)
    col_b = df_clean.iloc[:, 1].fillna('').astype(str)
    df_clean.index = col_a + col_b
    valid_rows_count = 0
    for idx in df_clean.index:
        if idx == "": break
        if "（注）" in idx or "出典" in idx: break
        valid_rows_count += 1
    df_clean = df_clean.iloc[:valid_rows_count]
    df_clean = df_clean.dropna(how='all')
    target_columns = [
        "墜落・転落", "転倒", "激突", "飛来・落下", "崩壊・倒壊", "激突され", 
        "はさまれ・巻き込まれ", "切れ・こすれ", "踏抜き", "おぼれ", 
        "高温・低温物との接触", "有害物との接触", "感電", "爆発", "破裂", "火災", 
        "交通事故（道路）", "交通事故（その他）", "動作の反動・無理な動作", 
        "その他", "分類不能"
    ]
    df_clean.columns = df_clean.columns.astype(str).str.strip()
    existing_cols = [c for c in target_columns if c in df_clean.columns]
    df_clean = df_clean[existing_cols]
    df_clean = df_clean[df_clean.index.astype(str).str.strip().str.startswith("建設業")]
    
    # 数値化とマイナス補正
    df_clean = df_clean.apply(pd.to_numeric, errors='coerce').fillna(0)
    df_clean[df_clean < 0] = 0
    return df_clean

# --- 2. 日付判定関数 ---
def get_date_from_filename(href):
    filename = href.split('/')[-1]
    match = re.search(r'(\d{2})-(\d{1,2})\.xlsx?', filename, re.IGNORECASE)
    if match:
        try:
            year_short = int(match.group(1))
            file_month = int(match.group(2))
            if file_month < 1 or file_month > 12: return None
            data_month = file_month - 1
            year_full = 2000 + year_short
            if data_month == 0:
                data_month = 12
                year_full -= 1
            return datetime.date(year_full, data_month, 1)
        except ValueError:
            return None
    return None

# --- 3. スクレイピングメイン関数 ---
def get_mhlw_latest_report():
    page_url = "https://www.mhlw.go.jp/bunya/roudoukijun/anzeneisei11/rousai-hassei/"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        print(f"サイトにアクセス中: {page_url}")
        res = requests.get(page_url, headers=headers, verify=False)
        res.raise_for_status()
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.text, 'html.parser')
        excel_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if ".xls" in href.lower():
                full_url = urljoin(page_url, href)
                date_obj = get_date_from_filename(href)
                if date_obj:
                    excel_links.append({"date": date_obj, "url": full_url, "filename": full_url.split('/')[-1]})
        if not excel_links:
            print("エラー: 有効なファイルが見つかりませんでした。")
            return None
        latest_link = sorted(excel_links, key=lambda x: x['date'], reverse=True)[0]
        print(f"最新データファイル: {latest_link['filename']}")
        print(f"データ対象年月: {latest_link['date'].strftime('%Y年%m月')}")
        excel_res = requests.get(latest_link['url'], headers=headers, verify=False)
        excel_file = io.BytesIO(excel_res.content)
        xls = pd.ExcelFile(excel_file)
        sheet_names = xls.sheet_names
        target_year_seireki = latest_link['date'].year
        reiwa_year = target_year_seireki - 2018
        reiwa_hankaku = str(reiwa_year)
        reiwa_zenkaku = reiwa_hankaku.translate(str.maketrans("0123456789", "０１２３４５６７８９"))
        target_candidates = [f"死傷災害（令和{reiwa_zenkaku}年、業種・事故の型別）", f"死傷災害（令和{reiwa_hankaku}年、業種・事故の型別）"]
        final_sheet_name = None
        for candidate in target_candidates:
            if candidate in sheet_names:
                final_sheet_name = candidate
                break
        if not final_sheet_name:
            keyword_year_zen = f"令和{reiwa_zenkaku}年"
            keyword_year_han = f"令和{reiwa_hankaku}年"
            for sheet in sheet_names:
                if "死傷災害" in sheet and "業種・事故の型別" in sheet:
                    if keyword_year_zen in sheet or keyword_year_han in sheet:
                        final_sheet_name = sheet
                        break
        if final_sheet_name:
            print(f"読み込むシート: {final_sheet_name}")
            df_raw = pd.read_excel(excel_file, sheet_name=final_sheet_name, header=None)
            df_final = clean_fixed_layout(df_raw)
            return df_final, latest_link['date']
        else:
            print("エラー: 目的のシートが見つかりませんでした。")
            return None
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return None

# --- 4. GAS送信関数 ---
def send_to_gas(latest_df, date_obj, gas_url):
    print(f"--- スプレッドシート(GAS)へ送信中 ---")
    if latest_df.empty:
        print("データが空のため送信しません。")
        return
    row_data = latest_df.iloc[0].to_dict()
    for k, v in row_data.items():
        row_data[k] = int(v)
    payload = {"date": date_obj.strftime('%Y-%m-%d'), "data": row_data}
    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(gas_url, data=json.dumps(payload), headers=headers)
        print(f"ステータス: {response.status_code}")
        if response.status_code == 200 or response.status_code == 302:
            try:
                res_json = response.json()
                print(f"GASからの応答: {res_json['message']}")
            except:
                print("送信完了（詳細レスポンスなし）")
        else:
            print(f"送信エラー: {response.text}")
    except Exception as e:
        print(f"通信エラー: {e}")

# --- 実行ブロック ---
if __name__ == "__main__":
    # ★GitHub SecretsからURLを読み込む方式に変更しました
    GAS_APP_URL = os.environ.get("GAS_APP_URL")

    if not GAS_APP_URL:
        print("エラー: GitHub Secretsに GAS_APP_URL が設定されていません。")
    else:
        result = get_mhlw_latest_report()
        if result:
            df, fetched_date = result
            print("\n=== データ取得成功 ===")
            print(df)
            send_to_gas(df, fetched_date, GAS_APP_URL)
