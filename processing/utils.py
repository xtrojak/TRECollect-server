import datetime


def save_file(filename, content):
    with open(filename, 'w') as f:
        f.write(content)


def read_file(filename):
    with open(filename, 'r') as f:
        return f.read()


def save_last_run_timestamp(now):
    save_file('last_run.txt', now.replace(microsecond=0).isoformat())


def get_last_run_timestamp():
    return datetime.datetime.fromisoformat(read_file('last_run.txt'))
