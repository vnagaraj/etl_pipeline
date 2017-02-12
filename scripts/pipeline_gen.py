from os.path import expanduser
import os

home = expanduser("~")
path = os.path.join(home +"/efs/etl_pipeline/airflow/")


def gen_pipelines():
	fname = os.path.join(path, "pipelines")
        fh = open(fname, 'w+')
        for i in range(1, 31):
            fh.write("etl" +str(i) + "\n")
        fh.close()

if __name__ == "__main__":
    gen_pipelines()
