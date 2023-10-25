import os


def to_flat_list(list):
    flat_list = [item for sublist in list for item in sublist]
    return flat_list


def read_file_content(*args):
    # Возвращает содержимое файла. На вход можно подать путь к файлу, или отдельно путь к папке,
    # в котором содержиться файл и его название

    if len(args) > 2:
        raise TypeError('Maximum 2 arguments')

    filepath = os.path.join(*args)

    with open(filepath, 'r', encoding='utf8') as f:
        content = f.read()
    return content
