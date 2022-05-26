from pathlib import Path


def get_path_as_string():
    """
    Gets the path of the file
    :return: path
    """
    p = Path(__file__)
    plist = str(p).split('/e2e/')
    return str(plist[0]) + '/'


def add_path_to_base_path(path_to_be_added):
    """
    adds path(param) to main path(../test/)
    :param path_to_be_added: should start from e2e/
    :return: new path with string format
    """
    return get_path_as_string() + path_to_be_added
