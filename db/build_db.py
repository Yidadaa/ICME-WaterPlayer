import json
import os
import pymongo
from tqdm import tqdm
from bson.son import SON

import time

class DB:
    def __init__(self, base_path, video_path, face_path, title_path, db_path='mongodb://localhost:27017'):
        self.base_path = base_path
        self.video_path = video_path
        self.face_path = face_path
        self.title_path = title_path
        self.db_path = db_path
        self.keys = ['uid', 'author_id', 'user_city', 'item_city', 'channel', 'music_id', 'device']

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

        history_table = self.db['history']

        buffer_history = []
        for i in tqdm(base_feature, desc='building db', total=total):
            [uid, user_city, item_id, author_id, item_city, channel, finish, like, music_id, device, time, duration_time] = i
            buffer_history.append({
                'uid': uid,
                'user_city': user_city,
                'item_id': item_id,
                'item_city': item_city,
                'author_id': author_id,
                'channel': channel,
                'finish': finish,
                'like': like,
                'music_id': music_id,
                'time': time,
                'duration_time': duration_time,
                'device': device,
                })
            if len(buffer_history) > 2000:
                history_table.insert(buffer_history)
                buffer_history = []

    def build_item_table(self):
        print('正在处理item_table')
        st = time.time()
        item_group = self.db.history.aggregate([
            {
                '$group': {
                    '_id': '$item_id',
                    'play': {
                        '$sum': 1,
                    },
                    'likes': {
                        '$sum': '$like'
                    },
                    'finish': {
                        '$sum': '$finish'
                    }
                    }
            },
            {
                '$sort': {
                    'play': -1
                }
            },
            {
                '$out': 'item'
            }], allowDiskUse=True)
        ed = time.time()
        print('已完成, 耗时:', ed - st, 's, 条目:')

    def build_item_group_of(self, name, key):
        print('正在处理item_group_' + name)
        st = time.time()
        item_group = self.db.history.aggregate([
            {
                '$project': {
                    name: 1,
                    'item_id': 1
                }
            },
            {
                '$lookup': {
                    'from': 'item',
                    'localField': 'item_id',
                    'foreignField': '_id',
                    'as': 'item_info'
                }
            },
            {
                '$addFields': {
                    'item_info': {
                        '$arrayElemAt': ['$item_info', 0]
                    }
                }
            },
            {
                '$group': {
                    '_id': '$' + name,
                    name + '_item_count': {
                        '$sum': 1,
                    },
                    name + '_play_count': {
                        '$sum': '$item_info.play'
                    },
                    name + '_avg_play': {
                        '$avg': '$item_info.play'
                    },
                    name + '_finish_count': {
                        '$sum': '$item_info.finish'
                    },
                    name + '_like_count': {
                        '$sum': '$item_info.likes'
                    }
                }
            },
            {
                '$out': key
            }
        ], allowDiskUse=True)
        ed = time.time()
        print('已完成, 耗时:', ed - st, 's, 表:', key)

    def build_all_group_table(self):
        for name in tqdm(self.keys):
            self.build_item_group_of(name, name)

    def max_of(self, name, key):
        table = self.db[name]
        max_v = table.find().sort(key, pymongo.DESCENDING).limit(1)
        return list(max_v)[0][key]

    def min_of(self, name, key):
        table = self.db[name]
        min_v = table.find().sort(key, pymongo.ASCENDING).limit(1)
        return list(min_v)[0][key]

    def process_all_group_table(self):
        # 获取item group的最大值，最小值并归一化处理
        print('processing groups')
        for name in tqdm(self.keys):
            table = self.db[name]
            play_count_max = self.max_of(name, name + '_play_count')
            play_count_min = self.min_of(name, name + '_play_count')
            play_count_len = play_count_max - play_count_min + 1 # 防止除以0

            item_count_max = self.max_of(name, name + '_item_count')
            item_count_min = self.min_of(name, name + '_item_count')
            item_count_len = item_count_max - item_count_min + 1 # 防止除以0

            # 开始处理数据
            # 对item_count和play_count做归一化, 并计算好评率和完成率
            table.aggregate([
                {
                    '$addFields': {
                        name + '_item_norm': {
                            '$divide': ['$' + name + '_item_count', item_count_len]
                        },
                        name + '_play_norm': {
                            '$divide': ['$' + name + '_play_count', play_count_len]
                        },
                        name + '_like_rate': {
                            '$divide': ['$' + name + '_like_count', '$' + name + '_play_count']
                        },
                        name + '_finish_rate': {
                            '$divide': ['$' + name + '_finish_count', '$' + name + '_play_count']
                        }

                    }
                },
                {
                    '$out': name
                }
            ])
