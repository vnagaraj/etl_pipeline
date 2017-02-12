import collections
import os
import sys

current_path = os.path.split(os.path.abspath(os.getcwd()))[0]
path = os.path.join(current_path, "etl_pipeline/userconfig/")
prefix = "user"

def delfiles(path):
    for subdir, dirs, files in os.walk(path):
    	for file in files:
                #print(subdir)
                #print(dirs)
  		#print(file)
      		os.remove(os.path.join(subdir,file))


def gen_files(count):
    for i in range(1, count+1):
        fname = os.path.join(path, prefix + str(i) + ".yaml")
        fh = open(fname, 'w+')
        fh.write(("id: pipeline_"+str(i) +"\n"))
        fh.write(("desc: filterlines having IRAN in input"+str(i) +"\n"))
        fh.write(("input:  inputfile_" + str(i) +".txt" + "\n"))
        fh.write(("output: output_" + str(i) + "\n"))
        fh.write("transforms:" + "\n")
        fh.write("        - textInputFormat" + "\n")
        fh.write("        - filterRecord Iran" + "\n")
        fh.close()


"""
id: pipeline_1
desc: filterlines having Iran in input
input:  inputfile_1.txt
output: output_1
transforms:
        - textInputFormat
        - filterRecord Iran

"""

if __name__ == "__main__":
    count = 1
    if len(sys.argv) == 2:
        count = int(sys.argv[1])
    delfiles(path)   
    gen_files(count)

