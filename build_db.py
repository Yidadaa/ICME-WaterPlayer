from tqdm import tqdm
from db.build_db import DB

from config import *

def item_table2file(c):
    with open('./item_id_finish_play_likes.txt', 'w') as f:
        for l in tqdm(c, total=c.count()):
            f.write(','.join(list(map(str, l.values()))) + '\n')

if __name__ == '__main__':
    db = DB(train_path, video_path, face_path, title_path)
    # db.insert_base_data()
    # db.build_item_table()
    item_table2file(db.db.item.find())
