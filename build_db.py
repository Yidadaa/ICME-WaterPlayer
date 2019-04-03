from tqdm import tqdm
from db.build_db import DB

from config import *

def item_table2file(c):
    with open('./dataset/item_id_play_likes_finish.txt', 'w') as f:
        for l in tqdm(c, total=c.count()):
            _id = l['_id']
            play = l['play']
            likes = l['likes']
            finish = l['finish']
            f.write(','.join(list(map(str, [_id, play, likes, finish]))) + '\n')

if __name__ == '__main__':
    db = DB(train_path, video_path, face_path, title_path)
    # db.insert_base_data()
    # db.build_item_table()
    # item_table2file(db.db.item.find())
    # db.build_all_group_table()
    db.process_all_group_table()
