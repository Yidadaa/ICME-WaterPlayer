

from build_db import DB
from config import *

if __name__ == '__main__':
    db = DB(train_path, video_path, face_path, title_path).db
    item_table = db['item']
    res = item_table.find_one({ 'item_id': 43195 })
    hi_table = db['history']
    res2 = hi_table.find_one({ 'item_id': 28 })
    print(res, res2)

