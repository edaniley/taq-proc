import sys
import pandas as pd

def main(filename):
    df = pd.read_hdf(filename)
    print (df.iloc[0])
    print (df.iloc[len(df)-1])

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main (sys.argv[1])
