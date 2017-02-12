from os.path import expanduser
import collections
import os


home = expanduser("~")
path = os.path.join(home +"/efs/etl_pipeline/files/")
prefix = "destfile_"
seed = "1092384956781341341234656953214543219"
words = open(path + "destfile_2.txt", "r").read().replace("\n", '').split()

def fdata():
    a = collections.deque(words)
    b = collections.deque(seed)
    while True:
        yield ' '.join(list(a)[0:1024])
        a.rotate(int(b[0]))
        b.rotate(1)

def gen_files():
    g = fdata()
    size = 10732  # 1gb
    for i in range(1, 31):
        fname = os.path.join(path, prefix + str(i) + ".txt")
        fh = open(fname, 'w+')
        while os.path.getsize(fname) < size:
            fh.write(g.next())

if __name__ == "__main__":
    gen_files()
