class File(object):
    """
    Provides service to download intermediate mappers file to workers
    when they run reduce jobs.
    """

    def __init__(self):
        pass

    def read(self, filename):
        with open(filename) as fd:
            chunk = fd.read()
            return chunk
