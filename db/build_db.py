import json
import os
import pymongo
from tqdm import tqdm

from config import *

class DB:
    def __init__(self, base_path, video_path, face_path, title_path, db_path='mongodb://localhost:27017'):
        self.base_path = base_path
        self.video_path = video_path
        self.face_path = face_path
        self.title_path = title_path
        self.db_path = db_path

        self.init_db()

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

    def init_db(self):
        self.db_client = pymongo.MongoClient(self.db_path)
        self.db = self.db_client['icme']

    def insert_base_data(self):
        print('reading: ' + self.base_path)
        total = self.count_lines(self.base_path)
        base_feature = self.load_train_txt(self.base_path)

        user_table = self.db['user']
        author_table = self.db['author']
        item_table = self.db['item']
        history_table = self.db['history']

        buffer_user = []
        buffer_author = []
        buffer_item = []
        buffer_history = []
        for i in tqdm(base_feature, desc='building db', total=total):
            [uid, user_city, item_id, author_id, item_city, channel, finish, like, music_id, device, time, duration_time] = i
            buffer_user.append({
                'uid': uid,
                'user_city': user_city
                })
            buffer_author.append({
                'author_id': author_id
                })
            buffer_item.append({
                'item_id': item_id,
                'item_city': item_city,
                'duration_time': duration_time,
                'music_id': music_id
                })
            buffer_history.append({
                'uid': uid,
                'item_id': item_id,
                'author_id': author_id,
                'channel': channel,
                'finish': finish,
                'like': like,
                'device': device,
                'time': time
                })
            if len(buffer_user) > 2000:
                user_table.insert(buffer_user)
                author_table.insert(buffer_author)
                item_table.insert(buffer_item)
                history_table.insert(buffer_history)
                buffer_user = []
                buffer_author = []
                buffer_item = []
                buffer_history = []

if __name__ == '__main__':
    db = DB(train_path, video_path, face_path, title_path)
    # db.insert_base_data() # 取消这一行的注释，可以重建数据库
