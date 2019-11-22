from functools import reduce

def deep_get(initial_dict, *path, default=None):
    def inner_dict_or_none(acc, step):
        try:
            return acc.get(step)
        except AttributeError:
            return None
    return reduce(inner_dict_or_none, path, initial_dict) or default