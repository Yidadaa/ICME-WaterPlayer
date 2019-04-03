import json
import os
import pymongo
from tqdm import tqdm
from bson.son import SON
import copy

import time

class DB:
    def __init__(self, base_path, video_path, face_path, title_path, db_path='mongodb://localhost:27017'):
        self.base_path = base_path
        self.video_path = video_path
        self.face_path = face_path
        self.title_path = title_path
        self.db_path = db_path
        self.keys = ['uid', 'author_id', 'user_city', 'item_city', 'channel', 'music_id', 'device']

        self.feature_keys = None

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

            avg_count_max = self.max_of(name, name + '_avg_play')
            avg_count_min = self.min_of(name, name + '_avg_play')
            avg_count_len = avg_count_max - avg_count_min + 1 # 防止除以0


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
                        name + '_avg_norm': {
                            '$divide': ['$' + name + '_avg_play', avg_count_len]
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

    def process_item_table(self):
        # 处理item表的数据
        print('processing item table')
        play_len = self.max_of('item', 'play') - self.min_of('item', 'play') + 1
        self.db.item.aggregate([
            {
                '$addFields': {
                    'play_norm': {
                        '$divide': ['$' + 'play', play_len]
                    },
                    'like_rate': {
                        '$divide': ['$' + 'likes', '$' + 'play']
                    },
                    'finish_rate': {
                        '$divide': ['$' + 'finish', '$' + 'play']
                    }
                }
            },
            {
                '$out': 'item'
            }
        ])

    def get_data_count(self):
        # 获取数据总数
        return self.db['history'].count()

    def make_keys_of(self, name):
        # 生成某个表的key
        key_t = ['_finish_rate', '_item_norm', '_play_norm', '_avg_norm', '_like_rate']
        return [name + k for k in key_t]

    def fetch_according_to(self, obj):
        # 根据history中字段数据生成特征
        result = copy.copy(obj)
        # 得到5*7=35个特征
        for name in self.keys:
            the_feature = self.db[name].find_one(
                { '_id': obj[name] },
                projection={ '_id': False }
            )
            the_feature = the_feature or {}
            for k in self.make_keys_of(name):
                if k in the_feature:
                    result[k] = 1 if the_feature[k] > 1\
                            else 0 if the_feature[k] < 0\
                            else the_feature[k]
                else:
                    result[k] = 0
        # 得到item属性数据
        the_item = self.db.item.find_one({ '_id': obj['item_id'] })
        the_item = the_item or {}
        for name in ['play_norm', 'like_rate', 'finish_rate']:
            result[name] = the_item[name] if name in the_item else 0

        result['is_same_city'] = int(obj['item_city'] == obj['user_city'])

        return result

    def get_feature_keys(self, force_update=False):
        # 获取特征数据
        if self.feature_keys is None or force_update:
            h = self.db.history.find_one(projection={ '_id': False })
            self.feature_keys = list(self.fetch_according_to(h).keys())

        return self.feature_keys

    def dict2list(self, keys, dict_):
        result = []
        for k in keys:
            result.append(str(dict_[k]))
        return result

    def escape(self, keys, ekeys):
        return list(filter(lambda x: x not in ekeys, keys))

    def process_feature_table(self):
        # 生成特征表
        htable = self.db.history
        lookup = {}
        htable.aggregate([{
            '$'
        }])

    def fetch_data(self, path, escape=[]):
        # 从数据库中获取训练数据
        history_table = self.db.history
        N = self.get_data_count()
        fkeys = self.get_feature_keys()
        fkeys = self.escape(fkeys, escape) # 删除不要的字段
        f = open(path, 'w')

        list2str = lambda x: ','.join(x) + '\n'

        # 写入头
        f.write(list2str(fkeys))

        for h in tqdm(history_table.find(projection={ '_id': False }), total=N):
            the_feature = self.fetch_according_to(h)
            the_list = self.dict2list(fkeys, the_feature)
            f.write(list2str(the_list))

        f.close()

