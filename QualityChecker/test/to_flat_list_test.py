import pytest


from utils.utils import to_flat_list

def test_to_flat_list_simple():
    assert to_flat_list([[1, 2], [3, 4]]) == [1, 2, 3, 4]
    assert to_flat_list([['a', 'b'], ['c', 'd']]) == ['a', 'b', 'c', 'd']

def test_to_flat_list_empty():
    assert to_flat_list([]) == []
    assert to_flat_list([[], []]) == []

def test_to_flat_list_single_element_lists():
    assert to_flat_list([[1], [2], [3]]) == [1, 2, 3]

def test_to_flat_list_nested_empty_lists():
    assert to_flat_list([[], [1, 2], []]) == [1, 2]