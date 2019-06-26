class JobFormatError(Exception):
    pass


class JobFormatWarning(Warning):
    def __init__(self, message, job):
        self.message = message
        self.job = job
