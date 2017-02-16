__author__ = 'luke'

import sys
from dervish.dervish import Dervish
from amazon_kclpy import kcl

def main():
    kclprocess = kcl.KCLProcess(Dervish(table_name=sys.argv[1], s3bucket=sys.argv[2], s3path=sys.argv[3]))
    kclprocess.run()

if __name__ == "__main__":
    main()
