import sys
from geochemxl.convert import main
import warnings

if __name__ == "__main__":
    warnings.filterwarnings('ignore')

    if len(sys.argv) < 2:
        print("No arguments supplied... so not doing anything")
    retval = main(sys.argv[1:])
    if retval is not None:
        sys.exit(retval)
