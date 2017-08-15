import os
import sys
import unittest

from impulsare_job import Reader, Writer
base_path = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, base_path + '/../')


# https://docs.python.org/3/library/unittest.html#assert-methods
class TestReader(unittest.TestCase):

    _job_name = 'test'
    _job_desc = 'My Job from pytest'
    _job_prio = 10
    _job_input = 'csv'
    _job_input_params = {'headers': False, 'delimiter': ';'}
    _job_output = 'rest'
    _job_output_params = {
        'base_url': 'https://jsonplaceholder.typicode.com',
        'endpoint': '/posts'
        }
    _job_mode = 'cu'


    def test_new_job(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        # Test
        writer = Writer(base_path + '/static/config_valid.yml')

        # Name + Desc + Prio
        writer.set_prop('name', self._job_name)
        writer.set_prop('description', self._job_desc)
        writer.set_prop('priority', self._job_prio)
        writer.set_prop('mode', self._job_mode)
        writer.set_prop('active', True)
        writer.set_prop('input', self._job_input)
        writer.set_prop('input_parameters', self._job_input_params)
        writer.set_prop('output', self._job_output)
        writer.set_prop('output_parameters', self._job_output_params)
        job = writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        reader = Reader(base_path + '/static/config_valid.yml', job.name)
        job = reader.get_job()
        self.assertEqual(self._job_name, job.name)
        self.assertEqual(self._job_desc, job.description)
        self.assertEqual(self._job_prio, job.priority)
        self.assertEqual(self._job_mode, job.mode)
        self.assertTrue(job.active)
        self.assertEqual(self._job_input, job.input)
        self.assertEqual(self._job_input_params, job.input_parameters)
        self.assertEqual(self._job_output, job.output)
        self.assertEqual(self._job_output_params, job.output_parameters)
        self.assertEqual(self._job_prio, job.priority)


    def test_new_job_with_fields_and_rules(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('input_parameters', {'delimiter': ';'})
        writer.set_prop('output', self._job_output)
        writer.fields_writer.add_field('input_test', 'output_test')
        writer.fields_writer.add_rule(output_field='output_test', name='test', method='my_method', params={'a': 'b'}, blocking=True, priority=1)
        writer.fields_writer.add_rule(output_field='output_test', name='test2', method='my_method2', params={'b': 'c'}, blocking=False, priority=100)
        job = writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        reader = Reader(base_path + '/static/config_valid.yml', job.name)
        job = reader.get_job()
        self.assertEqual(job.input_parameters, {'delimiter': ';'})

        fields = reader.get_fields()
        self.assertIs(len(fields), 1)
        self.assertEqual(fields[0].output, 'output_test')

        rules = fields[0].rules
        self.assertIs(len(rules), 2)
        self.assertEqual(rules[0].name, 'test')
        self.assertEqual(rules[1].name, 'test2')
