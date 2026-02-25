import discord
from discord import app_commands
from discord.ext import tasks
import os, aiohttp, re, gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
from flask import Flask
from threading import Thread
from bs4 import BeautifulSoup

# --- Flask Server ---
app = Flask('')
@app.route('/')
def home(): return "Bot is running!"
def run(): app.run(host='0.0.0.0', port=8080)
def keep_alive(): Thread(target=run).start()

# --- 設定 ---
JST = timezone(timedelta(hours=9))
SHEET_NAME = "AtCoderBot_DB"

EMOJI_MAP = {
    "AC": "<:atcoder_bot_AC:1463065663429021917>",
    "WA": "<:atcoder_bot_WA:1463065707703959643>",
    "TLE": "<:atcoder_bot_TLE:1463065790256382086>",
    "RE": "<:atcoder_bot_RE:1463065747705172165>",
    "CE": "<:atcoder_bot_CE:1463065865561051228>",
    "MLE": "<:atcoder_bot_MLE:1463065831763349514>"
}


class AtCoderBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.user_data = {}
        self.news_config = {}
        self.problems_map = {}
        self.diff_map = {}
        self.sent_notifications = set()
        self.pending_contests = {}
        
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            self.gc = gspread.authorize(creds)
            self.sheet = self.gc.open(SHEET_NAME)
        except Exception as e: print(f"⚠️ Sheetsエラー: {e}")
            
    def get_rated_color(self, rated_str):
        if not rated_str or rated_str in ["-", "Unrated"]:
            return 0x808080  # 灰色
        
        if "All" in rated_str:
            return 0x800080  # 紫 (Heuristic等)

        # 「2000 ~ 」や「~ 1999」を解析
        if "~" in rated_str:
            parts = rated_str.split("~")
            lower = parts[0].strip()
            upper = parts[1].strip().lower()

            # 下限が2000以上、または上限が inf (空欄含む) なら赤
            if (lower.isdigit() and int(lower) >= 2000) or upper == "" or "inf" in upper:
                return 0xFF0000  # 赤
            
            # 上限の数値で判定
            match = re.search(r'(\d+)', upper)
            if match:
                val = int(match.group(1))
                if val < 1200: return 0x008000 # 緑
                if val < 2000: return 0x0000FF # 青
                return 0xFF8000 # 橙
                
        return 0x808080 # デフォルト灰色
        
    def save_to_sheets(self):
        try:
            ws_user = self.sheet.worksheet("users")
            ws_user.clear()
            # ヘッダーを書き込む
            ws_user.append_row(["GuildID", "AtCoderID", "DiscordID", "ChannelID", "OnlyAC", "LastSubID"])
            
            rows = []
            for key, v in self.user_data.items():
                # self.user_data の中身を1行ずつリストにする
                rows.append([
                    str(v['guild_id']), 
                    v['atcoder_id'], 
                    str(v['discord_user_id']), 
                    str(v['channel_id']), 
                    str(v['only_ac']), 
                    str(v.get('last_sub_id', 0))
                ])
            
            if rows:
                ws_user.append_rows(rows) # まとめてスプレッドシートへ
        except Exception as e:
            print(f"❌ 書き込み失敗: {e}")

    def load_from_sheets(self):
        try:
            ws_user = self.sheet.worksheet("users")
            for r in ws_user.get_all_records():
                # 「サーバーID_ユーザー名」で固有の鍵を作る
                gid = str(r['GuildID'])
                aid = r['AtCoderID']
                key = f"{gid}_{aid}"
                
                self.user_data[key] = {
                    "guild_id": int(gid),
                    "atcoder_id": aid,
                    "discord_user_id": int(r['DiscordID']),
                    "channel_id": int(r['ChannelID']),
                    "only_ac": str(r['OnlyAC']).lower() == 'true',
                    "last_sub_id": int(r.get('LastSubID', 0))
                }
        except Exception as e:
            print(f"❌ 読み込み失敗: {e}")
            
    async def setup_hook(self):
        self.load_from_sheets()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://kenkoooo.com/atcoder/resources/problems.json") as r:
                    if r.status == 200: self.problems_map = {x['id']: x['title'] for x in await r.json()}
                async with session.get("https://kenkoooo.com/atcoder/resources/problem-models.json") as r:
                    if r.status == 200: self.diff_map = await r.json()
        except: pass
        self.check_submissions.start()
        # 既存の scheduler を開始（daily_schedule_update は scheduler 内で呼ばれます）
        self.auto_contest_scheduler.start() 
        await self.tree.sync()

    # --- AtCoderBotクラス内に追加 ---
    # --- AtCoderBotクラス内の既存のfetch_user_dataをこれに差し替え ---
    async def fetch_user_data(self, session, atcoder_id, mode='algo'):
        """
        AtCoderからユーザーデータを取得する。
        mode='algo' でアルゴリズム、mode='heur' でヒューリスティック用。
        """
        import re
        from bs4 import BeautifulSoup

        c_type = "heuristic" if mode == 'heur' else "algorithm"
        profile_url = f"https://atcoder.jp/users/{atcoder_id}?lang=ja&contestType={c_type}"
        history_url = f"https://atcoder.jp/users/{atcoder_id}/history/json?contestType={c_type}"
        headers = {"User-Agent": "Mozilla/5.0"}
        
        data = {
            "mode": mode, "atcoder_id": atcoder_id, "rating": 0, "max_rating": "---", 
            "diff": "---", "birth": "---", "org": "---", 
            "last_date": "---", "last_contest": "---", "last_contest_url": "",
            "contest_count": "---", "rank_all": "---", "history": []
        }

        try:
            # 1. 履歴データ (JSON) の取得と解析
            async with session.get(history_url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    h_json = await resp.json()
                    # Heuristicの場合はIsRated関係なく表示、AlgorithmはRatedのみを考慮
                    rated_only = [h for h in h_json if h.get('IsRated') or mode == 'heur']
                    
                    if rated_only:
                        # 直近5件を逆順（新しい順）で取得
                        latest_5 = rated_only[::-1][:5]
                        for i, h in enumerate(latest_5):
                            dt = datetime.fromisoformat(h['EndTime']).astimezone(JST)
                            full_name = h.get('ContestName', 'Unknown')
                            c_id = h.get('ContestScreenName', '').split('.')[0]
                            
                            # --- 短縮名のロジック ---
                            # "AtCoder Beginner Contest 441" から "ABC441" を作る
                            m = re.search(r'AtCoder\s+(Beginner|Regular|Grand|Heuristic)\s+Contest\s+(\d+)', full_name, re.IGNORECASE)
                            if m:
                                type_char = m.group(1)[0].upper()  # B, R, G, H
                                display_name = f"A{type_char}C{m.group(2)}"
                                # AHC/ABCなどのIDを小文字で構成 (例: ahc001, abc300)
                                contest_id_url = f"a{m.group(1)[0].lower()}c{m.group(2)}"
                                short_name = f"[{display_name}](https://atcoder.jp/contests/{contest_id_url})"
                            else:
                                # 企業コンテスト等の場合
                                display_name = (full_name[:12] + '..') if len(full_name) > 12 else full_name
                                # 既に定義済みの c_id (ContestScreenNameから取得) を使用してリンク化
                                short_name = f"[{display_name}](https://atcoder.jp/contests/{c_id})"

                            data["history"].append({
                                "name": short_name,
                                "date": dt.strftime('%m/%d'),
                                "perf": h.get('Performance', '---'),
                                "rate": h.get('NewRating', '---'),
                                "rank": h.get('Place', '---'),
                                "url": f"https://atcoder.jp/contests/{c_id}/standings?watching={atcoder_id}"
                            })
                            
                            # 一番新しいコンテスト (i=0) の情報を「現在のステータス」用に使用
                            if i == 0:
                                data["rating"] = h.get('NewRating', 0)
                                data["last_date"] = dt.strftime('%Y/%m/%d')
                                # ここは「フルネーム」をそのまま保持
                                data["last_contest"] = full_name
                                data["last_contest_url"] = f"https://atcoder.jp/contests/{c_id}"
                                
                                # 前回比の計算
                                if len(rated_only) >= 2:
                                    change = h['NewRating'] - rated_only[-2]['NewRating']
                                    data["diff"] = f"{'+' if change > 0 else ''}{change}"

            # 2. プロフィールページ (HTML) の解析
            async with session.get(profile_url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    soup = BeautifulSoup(await resp.text(), 'html.parser')
                    # ユーザー情報のテーブルを全スキャン
                    for row in soup.find_all('tr'):
                        th = row.find('th')
                        td = row.find('td')
                        if not th or not td: continue
                        
                        label = th.get_text(strip=True)
                        val = td.get_text(" ", strip=True).replace('―', '').strip()
                        
                        # 各種項目のマッピング
                        if label == "順位":
                            data["rank_all"] = val
                        elif "最高値" in label or "最高Rating" in label:
                            parts = val.split()
                            # Heuristic または 級が存在しない場合
                            if mode == 'heur' or len(parts) == 1:
                                data["max_rating"] = parts[0]
                            else:
                                # Algorithmかつ「級」が存在する場合
                                data["max_rating"] = f"{parts[0]} ({' '.join(parts[1:])})"
                        elif "参加回数" in label:
                            data["contest_count"] = val
                        elif "所属" in label:
                            data["org"] = val
                        elif "誕生年" in label:
                            data["birth"] = val

            return data
        except Exception as e:
            print(f"Error fetching {mode} data for {atcoder_id}: {e}")
            return None

    # --- 新規追加: 告知ページから詳細を抜く関数 ---
    import html # ファイルの1行目付近に追加

    async def fetch_post_details(self, session, contest_id):
        post_url = f"https://atcoder.jp/posts/{contest_id}_ja"
        info = {"writer": "不明", "tester": "不明", "points": "未発表"}
        try:
            async with session.get(post_url, timeout=10) as resp:
                if resp.status != 200: return info
                raw_html = await resp.text()
                # HTMLエンティティ(&lt;等)をデコード
                decoded_html = html.unescape(raw_html)
                soup = BeautifulSoup(decoded_html, 'html.parser')
                post_body = soup.find('div', class_='blog-post')
                
                if post_body:
                    # get_textではなく、要素の並びを直接ループして解析
                    content_text = post_body.get_text("\n", strip=True)
                    lines = content_text.splitlines()
                    
                    for line in lines:
                        # 全角・半角の両方のコロンに対応し、前後の不要な文字を掃除
                        clean_line = line.replace(':', '：').lstrip('- ').strip()
                        
                        if 'Writer' in clean_line and '：' in clean_line:
                            info["writer"] = clean_line.split('：', 1)[-1].strip()
                        elif 'Tester' in clean_line and '：' in clean_line:
                            info["tester"] = clean_line.split('：', 1)[-1].strip()
                        elif '配点' in clean_line and '：' in clean_line:
                            info["points"] = clean_line.split('：', 1)[-1].strip()
                            
                    # もし上記で見つからない場合(HTMLが特殊な連結をしている場合)の予備策
                    if info["writer"] == "不明":
                        # Writerという文字列を含む要素を探す
                        target = post_body.find(string=re.compile(r'Writer'))
                        if target:
                            # その親要素や兄弟要素からテキストを合成
                            parent_text = target.parent.get_text(strip=True)
                            if '：' in parent_text:
                                info["writer"] = parent_text.split('：', 1)[-1].strip()

        except Exception as e:
            print(f"❌ 詳細取得エラー: {e}")
        return info


    def format_duration(self, dur_str):
        """'100分'や'01:40'を'1時間40分'に変換"""
        if not dur_str: return "不明"
        if "日" in dur_str: return dur_str # 「10日間」はそのまま
        
        try:
            # 形式1: 「100 分」
            if "分" in dur_str:
                total_min = int(re.search(r'(\d+)', dur_str).group(1))
            # 形式2: 「01:40」
            elif ":" in dur_str:
                h, m = map(int, dur_str.split(':'))
                total_min = h * 60 + m
            else:
                return dur_str
            
            h, m = divmod(total_min, 60)
            if h > 0:
                return f"{h}時間{m}分" if m > 0 else f"{h}時間"
            return f"{m}分"
        except:
            return dur_str

    # --- 新規追加: 毎日6:00に予定を読み取るタスク ---
    @tasks.loop(hours=24)
    async def daily_schedule_update(self):
        async with aiohttp.ClientSession() as session:
            async with session.get("https://atcoder.jp/contests/?lang=ja") as resp:
                if resp.status != 200: return
                soup = BeautifulSoup(await resp.text(), 'html.parser')
            
            table = soup.find('div', id='contest-table-upcoming')
            if not table: return

            now = datetime.now(JST)
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) < 4: continue
                
                # 時刻解析
                time_str = cols[0].find('time').text
                st_dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S%z').astimezone(JST)
                
                # ID取得
                a_tag = cols[1].find('a')
                c_id = a_tag['href'].split('/')[-1]
                
                # 24時間以内に開始されるコンテストのみ詳細を取得して予約
                if 0 < (st_dt - now).total_seconds() <= 86400:
                    details = await self.fetch_post_details(session, c_id)
                    dur_str = cols[2].text.strip()
                    # 予約リストに追加 (二重登録防止のため dict を使用)
                    self.pending_contests[c_id] = {
                        "name": a_tag.text.strip(),
                        "url": f"https://atcoder.jp/contests/{c_id}",
                        "start": st_dt,
                        "end": st_dt + timedelta(minutes=self.parse_duration(dur_str)),
                        "duration": dur_str,
                        "rated": cols[3].text.strip(),
                        "details": details,
                        "sent": [] # 通知済みフラグを管理
                    }

    # --- 新規追加: 時間文字列のパース用 ---
    def parse_duration(self, dur_str):
        try:
            if "日" in dur_str:
                days = int(re.search(r'(\d+)', dur_str).group(1))
                return days * 24 * 60
            h, m = map(int, dur_str.split(':'))
            return h * 60 + m
        except: return 0

    
    @tasks.loop(minutes=3)
    async def check_submissions(self):
        # セッションをループの外で作成（効率化）
        async with aiohttp.ClientSession() as session:
            # 辞書のコピーに対してループを回す（実行中のサイズ変更エラー防止）
            for key in list(self.user_data.keys()):
                info = self.user_data[key]
                try:
                    await self.process_submissions(session, info, lookback_seconds=259200)
                except Exception as e:
                    print(f"⚠️ 提出確認エラー ({key}): {e}")

    async def process_submissions(self, session, info, lookback_seconds):
        atcoder_id = info['atcoder_id']
        guild_id = info['guild_id']
        key = f"{guild_id}_{atcoder_id}"
        
        # 過去の保存データから最後に通知したIDを取得
        last_id = int(info.get('last_sub_id', 0))
        
        # 2日分（172800秒）遡って取得するようにURLを作成
        # 引数の lookback_seconds が 172800 (2日) であることを想定
        url = f"https://kenkoooo.com/atcoder/atcoder-api/v3/user/submissions?user={atcoder_id}&from_second={int(datetime.now().timestamp() - lookback_seconds)}"
        
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    subs = await resp.json()
                    if not subs:
                        return

                    new_last_id = last_id
                    # 提出を古い順（ID昇順）に並べる
                    sorted_subs = sorted(subs, key=lambda x: x['id'])

                    for sub in sorted_subs:
                        # 既に通知済みのIDなら飛ばす（2回目以降のループ用）
                        if last_id != 0 and sub['id'] <= last_id:
                            continue
                        
                        # ACのみ通知の設定なら、AC以外を飛ばす
                        if info.get('only_ac', True) and sub['result'] != 'AC':
                            new_last_id = max(new_last_id, sub['id'])
                            continue
                        
                        # 通知送信！
                        # (登録直後なら、ここで過去2日分の通知が連続で飛びます)
                        await self.send_ac_notification(info, sub)
                        
                        # 通知した中で最新のIDを保持
                        new_last_id = max(new_last_id, sub['id'])
                    
                    # 最後にまとめて「どこまで通知したか」を保存
                    if new_last_id > last_id:
                        self.user_data[key]['last_sub_id'] = new_last_id
                        self.save_to_sheets()
        except Exception as e:
            print(f"⚠️ process_submissions エラー ({key}): {e}")
            
    async def send_ac_notification(self, info, sub):
        channel = self.get_channel(info['channel_id'])
        if not channel: return
        prob_id, atcoder_id = sub['problem_id'], info['atcoder_id']
        prob_title = self.problems_map.get(prob_id, prob_id)
        difficulty = self.diff_map.get(prob_id, {}).get('difficulty')
        user = self.get_user(info['discord_user_id'])
        user_name = user.display_name if user else "Unknown"
        user_icon = user.display_avatar.url if user else None
        res = sub['result']
        emoji = EMOJI_MAP.get(res, "❓")
        def get_color(d):
            if d is None: return 0x808080
            colors = [(400, 0x808080), (800, 0x804000), (1200, 0x008000), (1600, 0x00C0C0), (2000, 0x0000FF), (2400, 0xFFFF00), (2800, 0xFF8000)]
            for limit, color in colors:
                if d < limit: return color
            return 0xFF0000
        embed = discord.Embed(title=prob_title, url=f"https://atcoder.jp/contests/{sub['contest_id']}/tasks/{prob_id}", color=get_color(difficulty))
        embed.set_author(name=f"{user_name}", icon_url=user_icon)
        exec_time = sub.get('execution_time') or 0
        desc = (f"user : [{atcoder_id}](https://atcoder.jp/users/{atcoder_id}) / result : {emoji} **[{res}]**\n"
                f"difficulty : {difficulty if difficulty is not None else '---'} / {exec_time}ms / score : {int(sub['point'])}\n"
                f"language : {sub['language']}\n\n"
                f"📄 [{atcoder_id}さんの提出を見る](https://atcoder.jp/contests/{sub['contest_id']}/submissions/{sub['id']})\n"
                f"🔍 [解説を読む](https://atcoder.jp/contests/{sub['contest_id']}/editorial)")
        embed.description = desc
        dt = datetime.fromtimestamp(sub['epoch_second'], JST)
        embed.set_footer(text=f"提出時刻 : {dt.strftime('%b %d, %Y (%a) %H:%M:%S')}")
        await channel.send(embed=embed)

    async def fetch_recent_announcements(self, session):
        results = {}
        try:
            # 日本語ページを強制
            async with session.get("https://atcoder.jp/home?lang=ja") as resp:
                soup = BeautifulSoup(await resp.text(), 'html.parser')
            
            for post in soup.find_all('div', class_='panel-default'):
                body = post.find('div', class_='panel-body blog-post')
                if not body: continue
                
                # コンテストURLの取得と正規化
                link_tag = body.find('a', href=re.compile(r'https://atcoder\.jp/contests/[^" \n]+'))
                if not link_tag: continue
                c_url = link_tag['href'].split('?')[0].rstrip('/')
                
                info = {"writer": "不明", "tester": "不明", "points": "未発表"}

                # 名前を抽出する専用ロジック (aタグの中身を拾う)
                def extract_users(keyword):
                    target = body.find(string=re.compile(keyword))
                    if not target: return None
                    # キーワードの親要素から /users/ リンクを持つaタグをすべて取得
                    links = target.parent.find_all('a', href=re.compile(r'/users/'))
                    return ", ".join([u.get_text(strip=True) for u in links]) if links else None

                info["writer"] = extract_users("Writer") or "不明"
                info["tester"] = extract_users("Tester") or "不明"

                # 配点のパース (テキストから取得)
                content_text = body.get_text("|", strip=True)
                for line in content_text.split("|"):
                    if "配点：" in line or "配点:" in line:
                        info["points"] = line.split("：")[-1].split(":")[-1].strip()
                
                results[c_url] = info
        except Exception as e:
            print(f"⚠️ 告知解析エラー: {e}")
        return results
        
    async def broadcast_contest(self, name, url, st, dur, rated, label, details, is_10min=False, is_start=False, is_end=False):
        # 終了通知(cend)の場合もユニークキーを作って二重送信防止
        key = f"{label}_{url}"
        if key in self.sent_notifications: return
        self.sent_notifications.add(key)
        embed = self.create_contest_embed(name, url, st, dur, rated, details, is_10min, is_start, is_end)
        for cid in self.news_config.values():
            channel = self.get_channel(cid)
            if channel: await channel.send(content=f"**{label}**", embed=embed)
                
    def create_status_embed(self, d, target):
        mode_label = "Algorithm" if d['mode'] == 'algo' else "Heuristic"
        def get_color(r):
            colors = [(2800, 0xFF0000), (2400, 0xFF8000), (2000, 0xFFFF00), (1600, 0x0000FF), (1200, 0x00C0C0), (800, 0x008000), (400, 0x804000)]
            for threshold, color in colors:
                if r >= threshold: return color
            return 0x808080

        embed = discord.Embed(color=get_color(d["rating"]))
        profile_url = f"https://atcoder.jp/users/{d['atcoder_id']}?contestType={'heuristic' if d['mode'] == 'heur' else 'algorithm'}"
        
        # [アイコン] Discord名 / AtCoderID (Mode)
        embed.set_author(name=f"{target.display_name} / {d['atcoder_id']} ({mode_label})", url=profile_url, icon_url=target.display_avatar.url)

        status_value = (
            f"**現在の順位:** `{d['rank_all']}`\n"
            f"**現在のレーティング:** `{d['rating']}` (前回比: {d['diff']})\n"
            f"**最高レーティング:** `{d['max_rating']}`\n"
            f"**出場数:** {d['contest_count']} / **所属:** {d['org']}\n"
            f"**誕生年:** {d['birth']}\n"
            f"**最終参加:** {d['last_date']}\n"
            f"└ [{d['last_contest']}]({d['last_contest_url']})"
        )
        embed.add_field(name="📊 現在のステータス", value=status_value, inline=False)

        if d["history"]:
            h_lines = [f"**{h['name']}** ({h['date']}) Perf: **{h['perf']}** → Rate: **{h['rate']}** ([{h['rank']}位]({h['url']}))" for h in d["history"]]
            embed.add_field(name="🏆 直近のコンテスト成績", value="\n".join(h_lines), inline=False)

        now = datetime.now(JST)
        wd_ja = ["月", "火", "水", "木", "金", "土", "日"]
        embed.set_footer(text=f"{now.strftime(f'%Y年%m月%d日({wd_ja[now.weekday()]}) %H:%M')} 時点")
        return embed
    
    def create_contest_embed(self, name, url, st, dur_min, rated, details, is_start=False):
        # Ratedに応じた色を取得
        color = self.get_rated_color(rated)
        embed = discord.Embed(title=name, url=url, color=color)
        unix_time = int(st.timestamp())

        # 【追加】時間を「1時間40分」形式に整形
        formatted_dur = self.format_duration(dur_min)

        if is_start:
            embed.description = f"🚀 **開始しました！**\n\n📈 [順位表]({url}/standings)\n📄 [解説]({url}/editorial)"
        else:
            embed.description = (
                f"**コンテストページ：** {url}\n"
                f"**開始時刻：** {st.strftime('%Y-%m-%d %H:%M')}\n"
                f"**コンテスト時間：** {formatted_dur}\n" # ← ここを dur_min から変更
                f"**Writer：** {details.get('writer', '不明')}\n"
                f"**Tester：** {details.get('tester', '不明')}\n"
                f"**Rated対象：** {rated}\n" # ← 名称を「Rated対象」に変更
                f"**配点：** {details.get('points', '未発表')}\n"
                f"**コンテスト開始まで：** <t:{unix_time}:R>"
            )
        embed.set_footer(text=f"AtCoder - {st.strftime('%Y/%m/%d')}")
        return embed
        
    async def check_immediate_announcement(self, channel_id):
        now = datetime.now(JST)
        channel = self.get_channel(channel_id)
        if not channel: return
        
        status_msg = await channel.send(f"⏳ 最終デプロイ確認中... ({now.strftime('%H:%M:%S')})")
        async with aiohttp.ClientSession() as session:
            recent_details = await self.fetch_recent_announcements(session)
            
            async with session.get("https://atcoder.jp/home?lang=ja") as resp:
                soup = BeautifulSoup(await resp.text(), 'html.parser')
                # 予定テーブル
                table = soup.find('div', id='contest-table-upcoming')
                if not table: return

                rows = table.find_all('tr')[1:]
                log_txt = "📊 **最終解析結果**\n```\n"
                found_any = False

                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 2: continue
                    
                    time_tag = row.find('time')
                    a_tag = cols[1].find('a')
                    if not time_tag or not a_tag: continue

                    c_url = "[https://atcoder.jp](https://atcoder.jp)" + a_tag['href'].split('?')[0].rstrip('/')
                    c_name = a_tag.text.strip()
                    
                    try:
                        st_dt = datetime.strptime(time_tag.text.strip(), '%Y-%m-%d %H:%M:%S%z').astimezone(JST)
                        diff = int((st_dt - now).total_seconds() / 60)

                        if 0 < diff <= 1440:
                            # 取得した本質データと合体
                            info = recent_details.get(c_url, {"writer":"確認中","tester":"確認中","points":"確認中"})
                            
                            # Embed送信で失敗してもループを止めないガード
                            try:
                                # 列の存在チェックを厳密に
                                duration = cols[2].text.strip() if len(cols) > 2 else "不明"
                                rated = cols[3].text.strip() if len(cols) > 3 else "不明"
                                
                                await self.broadcast_contest(c_name, c_url, st_dt, duration, rated, "⏰ 本日開催", info)
                                log_txt += f"・{c_name[:12]} | ✅ 送信成功\n"
                                found_any = True
                            except Exception as discord_e:
                                log_txt += f"・{c_name[:12]} | ❌ 400エラー: {str(discord_e)[:10]}\n"
                        else:
                            log_txt += f"・{c_name[:12]} | {diff}分前\n"
                    except: continue

                log_txt += "```"
                await status_msg.edit(content=log_txt)
                
    @tasks.loop(minutes=1)
    async def auto_contest_scheduler(self):
        now = datetime.now(JST)
        # 毎日6:00にリストを更新する（初回や時間のズレ対策）
        if now.hour == 6 and now.minute == 0:
            await self.daily_schedule_update()

        for c_id, data in list(self.pending_contests.items()):
            diff_st = (data['start'] - now).total_seconds() / 60
            diff_en = (data['end'] - now).total_seconds() / 60
            
            # 通知判定 (sentリストに入れて二重送信を防止)
            # 24時間前
            if 1439 <= diff_st <= 1440 and "24h" not in data['sent']:
                await self.broadcast_contest(data['name'], data['url'], data['start'], data['duration'], data['rated'], "⏰ 24時間前", data['details'])
                data['sent'].append("24h")
            # 15分前
            elif 14 <= diff_st <= 15 and "15m" not in data['sent']:
                await self.broadcast_contest(data['name'], data['url'], data['start'], data['duration'], data['rated'], "⚠️ 15分前", data['details'])
                data['sent'].append("15m")
            # 開始
            elif -1 <= diff_st <= 0 and "start" not in data['sent']:
                await self.broadcast_contest(data['name'], data['url'], data['start'], data['duration'], data['rated'], "🚀 開始！", data['details'], is_start=True)
                data['sent'].append("start")
            # 終了
            elif -1 <= diff_en <= 0 and "end" not in data['sent']:
                await self.broadcast_contest(data['name'], data['url'], data['start'], data['duration'], data['rated'], "🏁 終了！", data['details'])
                data['sent'].append("end")
                # 終了したコンテストはリストから削除
                del self.pending_contests[c_id]

bot = AtCoderBot()

@bot.tree.command(name="register", description="提出通知の登録")
async def register(interaction: discord.Interaction, discord_user: discord.Member, atcoder_id: str, channel: discord.TextChannel, only_ac: bool):
    try: await interaction.response.defer()
    except: return
    info = {"guild_id": interaction.guild_id, "discord_user_id": discord_user.id, "atcoder_id": atcoder_id, "channel_id": channel.id, "only_ac": only_ac, "last_sub_id": 0}
    bot.user_data[f"{interaction.guild_id}_{atcoder_id}"] = info
    bot.save_to_sheets()
    await interaction.followup.send(f"✅ `{atcoder_id}` 登録完了。")
    async with aiohttp.ClientSession() as session: await bot.process_submissions(session, info, lookback_seconds=86400)

@bot.tree.command(name="delete", description="提出通知の削除")
async def delete(interaction: discord.Interaction, atcoder_id: str):
    try: await interaction.response.defer()
    except: return
    key = f"{interaction.guild_id}_{atcoder_id}"
    if key in bot.user_data:
        del bot.user_data[key]; bot.save_to_sheets()
        await interaction.followup.send(f"🗑️ `{atcoder_id}` 削除。")
    else: await interaction.followup.send("未登録です。")

@bot.tree.command(name="notice_set", description="告知チャンネル設定")
async def notice_set(interaction: discord.Interaction, channel: discord.TextChannel):
    try: await interaction.response.defer()
    except: return
    bot.news_config[str(interaction.guild_id)] = channel.id
    bot.save_to_sheets()
    # 最初に「考え中」を消すための応答を返す
    await interaction.response.send_message(f"告知先を {channel.mention} に設定しました。", ephemeral=True)
    
    # その後に重たい処理（check_immediate_announcement）を実行する
    await bot.check_immediate_announcement(channel.id)

@bot.tree.command(name="notice_delete", description="告知削除")
async def notice_delete(interaction: discord.Interaction):
    try: await interaction.response.defer()
    except: return
    gid = str(interaction.guild_id)
    if gid in bot.news_config:
        del bot.news_config[gid]; bot.save_to_sheets()
        await interaction.followup.send("🗑️ 告知削除。")
    else: await interaction.followup.send("未設定。")

# --- コマンドセクションに追加 ---
@bot.tree.command(name="status", description="AtCoderステータスを表示")
async def status(interaction: discord.Interaction, member: discord.Member = None):
    # 【最優先】何よりも先にこれを実行して3秒制限を回避する
    try:
        await interaction.response.defer()
    except discord.errors.NotFound:
        # すでに有効期限が切れている場合は終了
        return

    target = member or interaction.user
    
    # ここからデータ取得（時間のかかる処理）
    atcoder_id = next((v['atcoder_id'] for v in bot.user_data.values() if v['discord_user_id'] == target.id), None)
    
    if not atcoder_id:
        return await interaction.followup.send(f"❌ {target.name} さんのIDが登録されていません。")

    async with aiohttp.ClientSession() as session:
        # AlgoとHeurを並列で取得して時短する（任意ですが推奨）
        import asyncio
        algo_task = bot.fetch_user_data(session, atcoder_id, mode='algo')
        heur_task = bot.fetch_user_data(session, atcoder_id, mode='heur')
        algo_d, heur_d = await asyncio.gather(algo_task, heur_task)

    embeds = []
    if algo_d: embeds.append(bot.create_status_embed(algo_d, target))
    if heur_d: embeds.append(bot.create_status_embed(heur_d, target))

    if not embeds:
        return await interaction.followup.send("データの取得に失敗しました。")
        
    await interaction.followup.send(embeds=embeds)


async def preview(interaction: discord.Interaction, type: str):
    try: await interaction.response.defer(ephemeral=True)
    except: return
    dummy_details = {"writer": "Staff", "tester": "Tester", "points": "100-200-300"}
    dummy_url = "https://atcoder.jp/contests/practice"
    dummy_st = datetime.now(JST)
    if type == "ac":
        await bot.send_ac_notification({'atcoder_id': 'atcoder', 'discord_user_id': interaction.user.id, 'channel_id': interaction.channel_id}, {'id': 0, 'problem_id': 'abc_a', 'contest_id': 'abc', 'result': 'AC', 'point': 100, 'language': 'Python', 'epoch_second': int(datetime.now().timestamp())})
    else:
        # 時間を "01:40" (文字列) から 100 (数値) に変更
        # かつ、不要な引数 (is_10min等) を削除
        if type == "c24": e = bot.create_contest_embed("Preview", dummy_url, dummy_st, 100, "All", dummy_details)
        elif type == "c30": e = bot.create_contest_embed("Preview", dummy_url, dummy_st, 100, "All", dummy_details)
        elif type == "cstart": e = bot.create_contest_embed("Preview", dummy_url, dummy_st, 100, "All", dummy_details, is_start=True)
        elif type == "cend": e = bot.create_contest_embed("Preview", dummy_url, dummy_st, 100, "All", dummy_details)
        msg = f"**Preview: {type}**"
        
    # 既に一度 response を使っている場合は followup を使う
    await interaction.followup.send(content=f"**Preview: {type}**", embed=e)


@bot.tree.command(name="recent_contests", description="過去1週間のコンテスト告知を表示")
async def recent_contests(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
    except:
        return

    now = datetime.now(JST)
    one_week_ago = now - timedelta(days=7)
    
    async with aiohttp.ClientSession() as session:
        # コンテスト一覧ページを取得
        async with session.get("https://atcoder.jp/contests/archive?lang=ja") as resp:
            if resp.status != 200:
                return await interaction.followup.send("コンテスト情報の取得に失敗しました。")
            soup = BeautifulSoup(await resp.text(), 'html.parser')

        table = soup.find('table')
        if not table:
            return await interaction.followup.send("コンテストテーブルが見つかりませんでした。")

        rows = table.find_all('tr')[1:] # ヘッダー除外
        found_contests = []

        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 4: continue
            
            # 開始時刻
            time_tag = cols[0].find('time')
            if not time_tag: continue
            st_dt = datetime.strptime(time_tag.text.strip(), '%Y-%m-%d %H:%M:%S%z').astimezone(JST)
            
            # 過去1週間以内か判定
            if one_week_ago <= st_dt <= now:
                a_tag = cols[1].find('a')
                c_id = a_tag['href'].split('/')[-1]
                c_name = a_tag.text.strip()
                duration = cols[2].text.strip()
                rated = cols[3].text.strip()
                c_url = f"https://atcoder.jp/contests/{c_id}"
                
                # 詳細(Writer/Tester等)を取得
                details = await bot.fetch_post_details(session, c_id)
                found_contests.append({
                    "name": c_name, "url": c_url, "st": st_dt, 
                    "dur": duration, "rated": rated, "details": details
                })

        if not found_contests:
            return await interaction.followup.send("過去1週間以内に開催されたコンテストはありません。")

        # 1つずつEmbedを送信
        for c in found_contests:
            # 既存の create_contest_embed を利用
            # 引数の型を調整 (dur を数値にする必要がある場合は parse_duration を通す)
            embed = bot.create_contest_embed(
                c['name'], c['url'], c['st'], c['dur'], c['rated'], c['details']
            )
            await interaction.followup.send(embed=embed)

if __name__ == "__main__":
    keep_alive(); bot.run(os.getenv("DISCORD_TOKEN"))
