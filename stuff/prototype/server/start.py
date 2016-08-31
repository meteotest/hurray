"""hfive

Usage:
    start [options]

Options:
  -h --help                Show this screen.
  --version                Show version.
  --host=<host>         hostname or IP [default: localhost].
  -p --port=<port>         TCP port [default: 2222].
  --procs=<no_processes>   number of processes [default: 2].
  --verbose                Debug mode

"""

from docopt import docopt

import os
import sys


def main():
    # add ../.. directory to python path such that we can import the main
    # module
    HERE = os.path.dirname(os.path.realpath(__file__))
    PROJ_PATH = os.path.abspath(os.path.join(HERE, '..'))
    sys.path.insert(0, PROJ_PATH)

    from hfive.server import Server

    args = docopt(__doc__, version='hfive')

    host = args["--host"]
    port = int(args["--port"])
    procs = int(args["--procs"])
    server = Server()
    server.start(host, port, procs)


if __name__ == '__main__':
    main()
