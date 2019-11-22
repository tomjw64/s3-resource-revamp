from src import utils


class TestUtils:
    def test_deep_get(self):
        test_dict = {
            'a': {
                'b': {
                    'c': 'You found me!'
                }
            }
        }
        result = utils.deep_get(test_dict, 'a', 'b', 'c')
        assert result == 'You found me!'

    def test_deep_get_returns_none_when_not_found(self):
        test_dict = {
            'a': {
                'b': {
                    'c': 'You found me!'
                }
            }
        }
        result = utils.deep_get(test_dict, 'a', 'NOPE', 'c')
        assert result is None

    def test_deep_get_default_used_if_not_found(self):
        test_dict = {
            'a': {
                'b': {
                    'c': 'You found me!'
                }
            }
        }
        result = utils.deep_get(test_dict, 'a', 'NOPE', 'c', default='Maybe next time!')
        assert result == 'Maybe next time!'

    def test_deep_get_default_unused_if_found(self):
        test_dict = {
            'a': {
                'b': {
                    'c': 'You found me!'
                }
            }
        }
        result = utils.deep_get(test_dict, 'a', 'b', 'c', default='Maybe next time!')
        assert result == 'You found me!'
