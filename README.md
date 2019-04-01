# ICME-WaterPlayer
The repo for ICME2019 via HuaHuaGuai.

### 文件数据说明
#### `track2_train.txt`
数据头：`uid, user_city, item_id, author_id, item_city, channel(作品来源), finish, like, music_id, device, time, duration_time(作品时长)`
可以处理的特征工程：
1. item_id: 统计好评率和完成率，计算播放量，并归一化处理;
2. author_id: 统计该作者的视频数量并归一化处理，统计该作者视频的总播放量/平均播放量并归一化，统计该作者视频的平均好评率/完成率；
3. user_city: 判断用户城市与作品城市是否相等，作为一个特征
4. item_city: 考虑到网红城市的存在，可以用一系列指标来衡量某个城市的流行程度，统计某城市视频数量/总播放量/平均播放量/好评率/完成率；
5. channel: 暂不进行特殊处理；
6. finish, like: 暂时不进行处理；
7. music_id: 统计bgm频次并归一化，对应视频的平均播放量/平均好评率/完成率；
8. device: one-hot编码即可；
9. time: 暂时不用此数据；
10. duration_time: 归一化处理；
11. uid: 暂时不确定。

#### `track2_face_attrs.txt`
数据头：`gender, beauty, relative_position`
1. gender: 不进行处理；
2. beauty: 不进行处理；
3. relative_position: 暂不使用该特征；

#### `track2_title.txt`
数据头：`title_features.txt`
1. 用简单神经网络做降维；

#### `track2_video_features.txt`
数据头：`video_feature_dim_128`
1. 用简单神经网络做降维；

#### 特征工程
1. 完全不考虑时间信息；
2. 考虑时间信息做另一版；
