import json
import os
import sqlite3
from tqdm import tqdm

class DB:
    def __init__(self, base_path, video_path, face_path, title_path, db_path, sql_path):
        self.base_path = base_path
        self.video_path = video_path
        self.face_path = face_path
        self.title_path = title_path
        self.db_path = db_path
        self.sql_path = sql_path

        self.init_db(sql_path, db_path)
        self.insert_base_data()


    def load_train_txt(self, path):
        with open(path, 'r') as f:
            while True:
                l = f.readline()
                if len(l) == 0:
                    return
                yield [int(d) for d in l[0:-1].split('\t')]

    def load_obj_txt(self, path):
        with open(path, 'r') as f:
            return [json.loads(l) for l in f.readlines()[0:100]]

    def count_lines(self, path):
        return int(os.popen('wc -l ' + path).read().split(' ')[0])

    def init_db(self, sql_path, db_path):
        if os.path.exists(db_path):
            os.remove(db_path)
        self.conn = sqlite3.connect(db_path)
        with open(sql_path, 'r') as f:
            sql = f.read()
            self.conn.cursor().executescript(sql)

    def insert_base_data(self):
        print('reading: ' + self.base_path)
        total = self.count_lines(self.base_path)
        base_feature = self.load_train_txt(self.base_path)

        cursor = self.conn.cursor()
        cursor.execute('PRAGMA synchronous = OFF') # 关闭同步写可以大幅提高写入速度
        cursor.execute('PRAGMA journal_mode = OFF')

        buffer_user = []
        buffer_author = []
        buffer_item = []
        buffer_history = []
        for i in tqdm(base_feature, desc='building db', total=total):
            [uid, user_city, item_id, author_id, item_city, channel, finish, like, music_id, device, time, duration_time] = i
            buffer_user.append((uid, user_city))
            buffer_author.append((author_id, ))
            buffer_item.append((item_id, item_city, duration_time, music_id))
            buffer_history.append((uid, item_id, author_id, channel, finish, like, device, time))

            if len(buffer_user) > 1000:
                cursor.executemany('replace into USER values (?, ?)', buffer_user)
                cursor.executemany('replace into AUTHOR values (?)', buffer_author)
                cursor.executemany(
                        'replace into ITEM values (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)',
                        buffer_item)
                cursor.executemany('insert into PLAY_HISTORY values (?, ?, ?, ?, ?, ?, ?, ?)',
                        buffer_history)
                self.conn.commit()
                buffer_user = []
                buffer_author = []
                buffer_item = []
                buffer_history = []
        cursor.close()


if __name__ == '__main__':
    ROOT = './dataset/'

    train_path = ROOT + 'final_track2_train.txt'
    video_path = ROOT + 'track2_video_features.txt'
    face_path = ROOT + 'track2_face_attrs.txt'
    title_path = ROOT + 'track2_title.txt'
    sql_path = './db_structure.sql'
    db_path = ROOT + 'icme.db'

    # base_feature = load_train_txt(train_path)
    # video_feature = load_obj_txt(video_path)
    # face_feature = load_obj_txt(face_path)
    # title_feature = load_obj_txt(title_path)
    # init_db('./db_structure.sql', ROOT + 'icme.db')

    db = DB(train_path, video_path, face_path, title_path, db_path, sql_path)
