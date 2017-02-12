import os
import sys


current_path = os.path.split(os.path.abspath(os.getcwd()))[0]
path = os.path.join(current_path, "etl_pipeline/airflow/")
fname = os.path.join(path, "pipelines")

def gen_pipelines(count):
        fh = open(fname, 'w+')
        for i in range(1, count+1):
            fh.write("etl" +str(i) + "\n")
        fh.close()

if __name__ == "__main__":
    count = 1
    if len(sys.argv) == 2:
        count = int(sys.argv[1])
    gen_pipelines(count)
