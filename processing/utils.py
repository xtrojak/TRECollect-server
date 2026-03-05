import datetime


def save_file(filename, content):
    with open(filename, 'w') as f:
        f.write(content)


def read_file(filename):
    with open(filename, 'r') as f:
        return f.read()


def save_last_data_timestamp(now):
    save_file('last_data.txt', now.replace(microsecond=0).isoformat())


def get_last_data_timestamp():
    return datetime.datetime.fromisoformat(read_file('last_data.txt'))


def save_last_config_timestamp(now):
    save_file('last_config.txt', now.replace(microsecond=0).isoformat())


def get_last_config_timestamp():
    return datetime.datetime.fromisoformat(read_file('last_config.txt'))
