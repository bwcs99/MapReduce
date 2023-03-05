from L2.MapReducer import MapReducer
from L2.MapReduceSpecification import MapReduceSpecification
import hashlib


def map_function(key, value):
    words = value.split()

    key_value_pairs = []

    for word in words:
        key_value_pairs.append((word, 1))

    return key_value_pairs


def reduce_function(key, values):
    result_value = sum(values)
    return key, result_value


def hash_func(key):
    key_as_str = str(key)
    result = hashlib.md5(key_as_str.encode())

    return int(result.hexdigest(), 16)


if __name__ == "__main__":
    map_red_spec = MapReduceSpecification([f'test.txt'])

    map_red_obj = MapReducer(map_red_spec, 7)

    map_red_obj.set_mapper_function(map_function)
    map_red_obj.set_reducer_function(reduce_function)
    map_red_obj.set_hash_function(hash_func)

    map_red_obj.map_reduce()
