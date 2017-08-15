import unittest
import os
import sys

base_path = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, base_path + '/../')
from impulsare_job import Db
from impulsare_config import utils


# https://docs.python.org/3/library/unittest.html#assert-methods
class TestReader(unittest.TestCase):
    def test_overriden(self):
        with self.assertRaisesRegex(IOError, 'Missing config file: "/does/not/exist" does not exist'):
            Db('/does/not/exist')


    def test_valid_config(self):
        config_file = base_path + '/static/config_valid.yml'

        db = Db(config_file)
        self.assertIs(type(db), Db)
        self.assertIsInstance(db._config, dict)

        self.assertIn('logger', db._config)
        self.assertIn('level', db._config['logger'])
        self.assertEqual('DEBUG', db._config['logger']['level'])

        self.assertIn('job', db._config)
        self.assertIn('db', db._config['job'])
        self.assertEqual('/tmp/test.db', db._config['job']['db'])


    def test_empty_config(self):
        config_file = base_path + '/static/config_empty.yml'

        with self.assertRaisesRegex(ValueError, "Your config is not valid: 'job' is a required property"):
            Db(config_file)


    def test_invalid_config(self):
        config_file = base_path + '/static/config_invalid.yml'

        with self.assertRaisesRegex(ValueError, "Your config is not valid: True is not of type 'object'"):
            Db(config_file)


if __name__ == "__main__":
    unittest.main()
