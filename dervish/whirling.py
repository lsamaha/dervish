__author__ = 'luke'

from dervish import Dervish
from amazon_kclpy import kcl

def main():
    kclprocess = kcl.KCLProcess(Dervish())
    kclprocess.run()

if __name__ == "__main__":
    main()