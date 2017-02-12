from os.path import expanduser
import collections
import os

home = expanduser("~")
path = os.path.join(home +"/efs/etl_pipeline/userconfig/")
prefix = "user"


def gen_files():
    for i in range(1, 31):
        fname = os.path.join(path, prefix + str(i) + ".yaml")
        fh = open(fname, 'w+')
        fh.write(("id: pipeline_"+str(i) +"\n"))
        fh.write(("desc: filterlines having IRAN in input"+str(i) +"\n"))
        fh.write(("input:  destfile_" + str(i) +".txt" + "\n"))
        fh.write(("output: output_" + str(i) + "\n"))
        fh.write("transforms:" + "\n")
        fh.write("        - textInputFormat" + "\n")
        fh.write("        - filterRecord Iran" + "\n")
        fh.close()


"""
id: pipeline_1
desc: filterlines having Iran in input
input:  destfile_1.txt
output: output_1
transforms:
        - textInputFormat
        - filterRecord Iran

"""

if __name__ == "__main__":
    gen_files()
