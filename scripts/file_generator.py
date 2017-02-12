import collections
import os
import sys


path = os.path.split(os.path.abspath(os.getcwd()))[0]
parent_path = os.path.join(path, "files/")
prefix = "inputfile_"
seed = "1092384956781341341234656953214543219"
first_file = os.path.join(parent_path, "inputfile_1.txt")

def delfiles(path):
    for subdir, dirs, files in os.walk(path):
    	for file in files:
                #print(subdir)
                #print(dirs)
  		#print(file)
      		os.remove(os.path.join(subdir,file))	

def create_file(first_file):
	file = open(first_file,"w+") 
 	file.write("Hello World") 
	file.write("This is our new text file") 
	file.write("and this is another line.") 
	file.write("Why? Because we can.") 
 	file.close()

def fdata(words):
    a = collections.deque(words)
    b = collections.deque(seed)
    while True:
        yield ' '.join(list(a)[0:1024])
        a.rotate(int(b[0]))
        b.rotate(1)

def gen_files(count):
    delfiles(parent_path)
    create_file(first_file)
    words = open(first_file, "r").read().replace("\n", '').split()
    g = fdata(words)
    size = 10732  # 1gb
    for i in range(1, count+1):
        fname = os.path.join(parent_path, prefix + str(i) + ".txt")
        print "Generated fname  " + fname
        fh = open(fname, 'w+')
        while os.path.getsize(fname) < size:
            fh.write(g.next())

if __name__ == "__main__":
    count = 1
    if len(sys.argv) == 2:
	count = int(sys.argv[1])	
    gen_files(count)
