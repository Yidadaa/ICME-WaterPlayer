import json


def load_train_txt(path):
    print('processing ' + path)
    with open(path, 'r') as f:
        return [[int(d) for d in l[0:-1].split('\t')] for l in f.readlines()[0:100]]

def load_obj_txt(path):
    print('processing ' + path)
    with open(path, 'r') as f:
        return [json.loads(l) for l in f.readlines()[0:100]]


if __name__ == '__main__':
    ROOT = './dataset/'

    train_path = ROOT + 'final_track2_train.txt'
    video_path = ROOT + 'track2_video_features.txt'
    face_path = ROOT + 'track2_face_attrs.txt'
    title_path = ROOT + 'track2_title.txt'

    base_feature = load_train_txt(train_path)
    video_feature = load_obj_txt(video_path)
    face_feature = load_obj_txt(face_path)
    title_feature = load_obj_txt(title_path)


