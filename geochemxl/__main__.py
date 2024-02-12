import sys
from geochemxl.converter import cli
import warnings


if __name__ == "__main__":
    warnings.filterwarnings('ignore')

    if len(sys.argv) < 2:
        print("No arguments supplied... so not doing anything")
    retval = cli(sys.argv[1:])
    if retval is not None:
        sys.exit(retval)
