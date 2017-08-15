import json
import os
import sqlite3
import sys
import unittest

from impulsare_job import Writer
base_path = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, base_path + '/../')


# https://docs.python.org/3/library/unittest.html#assert-methods
class TestWriter(unittest.TestCase):

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
        # mode : u=update | c=create | cu=create+update | d=delete
        writer.set_prop('mode', self._job_mode)

        # Inactive
        writer.set_prop('active', True)

        # Input type + Parameters
        writer.set_prop('input', self._job_input)
        writer.set_prop('input_parameters', self._job_input_params)
        # Output type + Parameters
        writer.set_prop('output', self._job_output)
        writer.set_prop('output_parameters', self._job_output_params)

        writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        job = self._get_job()
        self.assertEqual(self._job_name, job['name'])
        self.assertEqual(self._job_name, writer.get_prop('name'))

        self.assertEqual(self._job_desc, job['description'])
        self.assertEqual(self._job_desc, writer.get_prop('description'))

        self.assertEqual(self._job_prio, job['priority'])
        self.assertEqual(self._job_mode, job['mode'])
        self.assertTrue(job['active'])
        self.assertEqual(self._job_input, job['input'])
        self.assertEqual(self._job_input_params, json.loads(job['input_parameters']))
        self.assertEqual(self._job_output, job['output'])
        self.assertEqual(self._job_output_params, json.loads(job['output_parameters']))
        self.assertEqual(self._job_prio, job['priority'])


    def test_new_job_missing_data(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        writer = Writer(base_path + '/static/config_valid.yml')

        with self.assertRaisesRegex(ValueError, "Property name is required"):
            writer.save()

        writer.set_prop('name', 'test')
        with self.assertRaisesRegex(ValueError, "Property input is required"):
            writer.save()

        writer.set_prop('input', 'test')
        with self.assertRaisesRegex(ValueError, "Property output is required"):
            writer.save()

        writer.set_prop('output', 'test')
        writer.save()
        self.assertTrue(os.path.isfile('/tmp/test.db'))


    def test_get_non_existing_prop(self):
        writer = Writer(base_path + '/static/config_valid.yml')
        with self.assertRaisesRegex(KeyError, 'toto is not a valid property'):
            writer.get_prop('toto')


    def test_set_non_existing_prop(self):
        writer = Writer(base_path + '/static/config_valid.yml')
        with self.assertRaisesRegex(KeyError, "Can't set toto as it does not exist in our dict"):
            writer.set_prop('toto', 'test')


    def test_set_bad_type_prop(self):
        writer = Writer(base_path + '/static/config_valid.yml')
        with self.assertRaisesRegex(ValueError, "name must be of type <class 'str'>"):
            writer.set_prop('name', 32)


    def test_new_job_bad_mode(self):
        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('output', self._job_output)
        with self.assertRaisesRegex(ValueError, 'wrong is not a valid mode.+'):
            writer.set_prop('mode', 'wrong')


    def test_new_job_then_update(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        # Test
        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('input_parameters', self._job_input_params)
        writer.set_prop('output', self._job_output)
        writer.set_prop('output_parameters', self._job_output_params)
        job = writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        jobInDb = self._get_job()
        self.assertEqual(self._job_name, jobInDb['name'])
        self.assertTrue(jobInDb['active'])
        self.assertEqual(self._job_input, jobInDb['input'])
        self.assertEqual(self._job_output, jobInDb['output'])

        writer = Writer(base_path + '/static/config_valid.yml', job.name)
        job = writer.get_job()

        self.assertEqual(job.name, jobInDb['name'])
        self.assertTrue(job.active)
        self.assertEqual(job.input, jobInDb['input'])
        self.assertEqual(job.output, jobInDb['output'])

        writer = Writer(base_path + '/static/config_valid.yml', job.name)
        writer.set_prop('active', False)
        job = writer.save()
        jobInDb = self._get_job()
        self.assertEqual(self._job_name, jobInDb['name'])
        self.assertFalse(jobInDb['active'])


    def test_new_job_then_delete(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        # Test
        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', 'To Delete')
        writer.set_prop('input', self._job_input)
        writer.set_prop('output', self._job_output)
        job = writer.save()

        writer = Writer(base_path + '/static/config_valid.yml', job.name)
        writer.delete()

        with self.assertRaisesRegex(ValueError, "Can't retrieve Job .+"):
            Writer(base_path + '/static/config_valid.yml', job.name)


    def test_get_add_delete_hooks(self):
        writer = Writer(base_path + '/static/config_valid.yml')

        with self.assertRaisesRegex(KeyError, "Hook test does not exist"):
            writer.get_hook('test')

        self.assertFalse(writer.hook_exists('test'))
        writer.add_hook(name='test', method='method', when='never')
        self.assertTrue(writer.hook_exists('test'))
        hook = writer.get_hook('test')
        self.assertEqual(hook, {'name': 'test', 'active': True, 'description': None,
                                'method': 'method', 'priority': 1, 'when': 'never'})

        with self.assertRaisesRegex(KeyError, "Hook test already exists. Delete it first"):
            writer.add_hook('test', 'method', 'never')

        writer.del_hook('test')
        self.assertFalse(writer.hook_exists('test'))

        with self.assertRaisesRegex(KeyError, "Hook test does not exist"):
            writer.del_hook('test')


    def test_get_add_delete_fields(self):
        writer = Writer(base_path + '/static/config_valid.yml')

        with self.assertRaisesRegex(KeyError, "Field output_test does not exist"):
            writer.get_field('output_test')

        self.assertFalse(writer.field_exists('output_test'))
        writer.add_field(input='input_test', output='output_test')
        self.assertTrue(writer.field_exists('output_test'))
        field = writer.get_field('output_test')
        self.assertEqual(field, {'input': 'input_test', 'output': 'output_test', 'rules': {}})

        with self.assertRaisesRegex(KeyError, "Field output_test already exists. Delete it first"):
            writer.add_field(input='input_test', output='output_test')

        writer.del_field('output_test')
        self.assertFalse(writer.field_exists('output_test'))

        with self.assertRaisesRegex(KeyError, "Field output_test does not exist"):
            writer.del_field('output_test')


    def test_get_add_delete_rules(self):
        writer = Writer(base_path + '/static/config_valid.yml')

        with self.assertRaisesRegex(KeyError, "'Field output_test does not exists. Cannot get rules"):
            writer.get_rules('output_test')

        writer.add_field(input='input_test', output='output_test')
        self.assertFalse(writer.rule_exists('output_test', 'test'))

        writer.add_rule(output_field='output_test', name='test', method='my_method', params={'a': 'b'}, blocking=True)
        self.assertTrue(writer.rule_exists('output_test', 'test'))
        rules = writer.get_rules('output_test')
        self.assertTrue('test' in rules)
        self.assertEqual(rules['test'], {
            'active': True, 'blocking': True, 'description': None, 'method': 'my_method',
            'name': 'test', 'params': {'a': 'b'}, 'priority': 1})

        with self.assertRaisesRegex(KeyError, "Rule test already exists for output_test. Delete it first"):
            writer.add_rule(output_field='output_test', name='test', method='my_method', params={'a': 'b'})

        writer.del_rule('output_test', 'test')
        self.assertFalse(writer.rule_exists('output_test', 'test'))
        rules = writer.get_rules('output_test')
        self.assertEqual(rules, {})

        with self.assertRaisesRegex(KeyError, "Rule test does not exist for output_test"):
            writer.del_rule('output_test', 'test')


    def test_add_hook_to_db_new_job(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        # Test
        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('output', self._job_output)
        writer.add_hook(name='test', method='method', when='never', description='Hook from Test', priority=10)
        writer.add_hook(name='test2', method='method2', when='always', active=False)
        writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        jobInDb = self._get_job()
        self.assertEqual(self._job_name, jobInDb['name'])
        self.assertEqual(self._job_name, writer.get_prop('name'))

        hooksInDb = self._get_hooks(jobInDb['id'], 2)
        self.assertEqual('test', hooksInDb[0]['name'])
        self.assertEqual('Hook from Test', hooksInDb[0]['description'])
        self.assertIs(10, hooksInDb[0]['priority'])
        self.assertEqual('method', hooksInDb[0]['method'])
        self.assertEqual(1, hooksInDb[0]['active'])
        self.assertEqual('never', hooksInDb[0]['when'])

        self.assertEqual('test2', hooksInDb[1]['name'])
        self.assertIs(None, hooksInDb[1]['description'])
        self.assertIs(1, hooksInDb[1]['priority'])
        self.assertEqual('method2', hooksInDb[1]['method'])
        self.assertEqual(0, hooksInDb[1]['active'])
        self.assertEqual('always', hooksInDb[1]['when'])


    def test_add_job_without_hook_then_retrieve(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        # Test
        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('output', self._job_output)
        writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        jobInDb = self._get_job()

        # Reinit the writer now to make sure I have no error
        writer = Writer(base_path + '/static/config_valid.yml', jobInDb['name'])
        with self.assertRaisesRegex(KeyError, "Hook test does not exist"):
            writer.get_hook('test')


    def test_add_hook_to_db_existing_job(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        # Test
        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('output', self._job_output)
        writer.add_hook(name='test', method='method', when='never', description='Hook from Test', priority=10)
        writer.add_hook(name='test2', method='method2', when='always', active=False)
        writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        jobInDb = self._get_job()
        self._get_hooks(jobInDb['id'], 2)

        # Reinit the writer now to see if I have my job
        writer = Writer(base_path + '/static/config_valid.yml', jobInDb['name'])
        hook_test = writer.get_hook('test')
        self.assertEqual('test', hook_test['name'])
        self.assertEqual('Hook from Test', hook_test['description'])
        self.assertIs(10, hook_test['priority'])
        self.assertEqual('method', hook_test['method'])
        self.assertEqual(1, hook_test['active'])
        self.assertEqual('never', hook_test['when'])

        hook_test2 = writer.get_hook('test2')
        self.assertEqual('test2', hook_test2['name'])
        self.assertIs(None, hook_test2['description'])
        self.assertIs(1, hook_test2['priority'])
        self.assertEqual('method2', hook_test2['method'])
        self.assertEqual(0, hook_test2['active'])
        self.assertEqual('always', hook_test2['when'])

        # Now add a hook
        writer.add_hook(name='test3', method='method3', when='sometimes')
        writer.save()

        hooksInDb = self._get_hooks(jobInDb['id'], 3)
        self.assertEqual('test2', hooksInDb[1]['name'])

        self.assertEqual('test3', hooksInDb[2]['name'])
        self.assertIs(None, hooksInDb[2]['description'])
        self.assertIs(1, hooksInDb[2]['priority'])
        self.assertEqual('method3', hooksInDb[2]['method'])
        self.assertEqual('sometimes', hooksInDb[2]['when'])

        writer = Writer(base_path + '/static/config_valid.yml', jobInDb['name'])
        hooksInObj = writer.get_hooks()
        self.assertEqual(3, len(hooksInObj))
        self.assertEqual('test', hooksInObj['test']['name'])
        self.assertEqual('test2', hooksInObj['test2']['name'])
        self.assertEqual('test3', hooksInObj['test3']['name'])


    def test_add_rules_to_db_new_job(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        # Test
        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('output', self._job_output)
        writer.add_field('input_test', 'output_test')
        writer.add_field('input_test2', 'output_test2')
        writer.add_rule(output_field='output_test', name='lowercase', method='lwrcs', description='Lower the case !', active=False)
        writer.add_rule(output_field='output_test', name='uppercase', method='uprc', params={'a': 'b'}, blocking=True, priority=10)
        job = writer.save()
        writer = Writer(base_path + '/static/config_valid.yml', job.name)

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        jobInDb = self._get_job()
        self.assertEqual(self._job_name, jobInDb['name'])
        self.assertEqual(self._job_name, writer.get_prop('name'))

        fieldsInDb = self._get_fields(jobInDb['id'], 2)
        self.assertEqual('input_test', fieldsInDb[0]['input'])
        self.assertEqual('output_test', fieldsInDb[0]['output'])

        rulesInDb = self._get_rules(fieldsInDb[0]['id'], 2)
        self.assertEqual('lowercase', rulesInDb[0]['name'])
        self.assertEqual('Lower the case !', rulesInDb[0]['description'])
        self.assertEqual(0, rulesInDb[0]['active'])
        self.assertEqual('lwrcs', rulesInDb[0]['method'])
        self.assertEqual({}, json.loads(rulesInDb[0]['params']))
        self.assertIs(0, rulesInDb[0]['blocking'])
        self.assertIs(1, rulesInDb[0]['priority'])

        self.assertEqual('uppercase', rulesInDb[1]['name'])
        self.assertIs(None, rulesInDb[1]['description'])
        self.assertEqual(1, rulesInDb[1]['active'])
        self.assertEqual('uprc', rulesInDb[1]['method'])
        self.assertEqual({'a': 'b'}, json.loads(rulesInDb[1]['params']))
        self.assertIs(1, rulesInDb[1]['blocking'])
        self.assertIs(10, rulesInDb[1]['priority'])

        job = writer.get_job()
        self.assertEqual(self._job_name, job.name)
        fields = writer.get_fields()
        self.assertTrue('output_test' in fields)
        self.assertTrue('output' in fields['output_test'])
        self.assertEqual(fields['output_test']['output'], 'output_test')
        self.assertTrue('rules' in fields['output_test'])
        self.assertTrue('lowercase' in fields['output_test']['rules'])
        self.assertTrue('method' in fields['output_test']['rules']['lowercase'])
        self.assertTrue('params' in fields['output_test']['rules']['lowercase'])
        self.assertEqual(fields['output_test']['rules']['lowercase']['params'], {})

        rules = writer.get_rules('output_test')
        self.assertTrue('lowercase' in rules)
        self.assertTrue('method' in rules['lowercase'])
        self.assertTrue('params' in rules['lowercase'])
        self.assertEqual({}, rules['lowercase']['params'])

        self.assertTrue('uppercase' in rules)
        self.assertTrue('method' in rules['uppercase'])
        self.assertTrue('params' in rules['uppercase'])
        self.assertEqual({'a': 'b'}, rules['uppercase']['params'])


    def test_add_job_without_field_then_retrieve(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        # Test
        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('output', self._job_output)
        writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        jobInDb = self._get_job()

        # Reinit the writer now to make sure I have no error
        writer = Writer(base_path + '/static/config_valid.yml', jobInDb['name'])
        with self.assertRaisesRegex(KeyError, "Field test does not exist"):
            writer.get_field('test')


    def test_add_rule_to_db_existing_job(self):
        if os.path.isfile('/tmp/test.db'):
            os.remove('/tmp/test.db')

        writer = Writer(base_path + '/static/config_valid.yml')
        writer.set_prop('name', self._job_name)
        writer.set_prop('input', self._job_input)
        writer.set_prop('output', self._job_output)
        writer.add_field('input_test', 'output_test')
        writer.add_field('input_test2', 'output_test2')
        writer.add_rule(output_field='output_test', name='test', method='my_method', params={'a': 'b'}, blocking=True)
        writer.add_rule(output_field='output_test', name='test2', method='my_method2', params={'b': 'c'}, blocking=False)
        writer.save()

        self.assertTrue(os.path.isfile('/tmp/test.db'))

        jobInDb = self._get_job()
        fieldsInDb = self._get_fields(jobInDb['id'], 2)
        self._get_rules(fieldsInDb[0]['id'], 2)

        # Reinit the writer now to see if I have my job
        writer = Writer(base_path + '/static/config_valid.yml', jobInDb['name'])
        field_w_rules = writer.get_field('output_test')
        self.assertEqual(field_w_rules['input'], 'input_test')

        # Read Rules
        rules = writer.get_rules('output_test')
        rule_test = rules['test']
        self.assertEqual('test', rule_test['name'])
        self.assertEqual('my_method', rule_test['method'])
        self.assertTrue(rule_test['blocking'])
        self.assertEqual({'a': 'b'}, rule_test['params'])
        rule_test2 = rules['test2']
        self.assertEqual('test2', rule_test2['name'])
        self.assertEqual('my_method2', rule_test2['method'])
        self.assertFalse(rule_test2['blocking'])

        # Now add more rules / fields
        writer.add_rule(output_field='output_test', name='test3', method='my_method3', blocking=False)

        writer.add_field(input='input_test3', output='output_test3')
        writer.add_rule(output_field='output_test3', name='test4', method='my_method4', blocking=True)
        writer.save()

        fieldsInDb = self._get_fields(jobInDb['id'], 3)

        # for the previous fields, saved the first time
        rulesInDb = self._get_rules(fieldsInDb[0]['id'], 3)
        self.assertEqual('test', rulesInDb[0]['name'])
        self.assertEqual('test2', rulesInDb[1]['name'])
        self.assertEqual('test3', rulesInDb[2]['name'])
        self.assertIs(None, rulesInDb[2]['description'])
        self.assertIs(1, rulesInDb[2]['priority'])
        self.assertEqual('my_method3', rulesInDb[2]['method'])

        # For the new field
        rulesInDb = self._get_rules(fieldsInDb[2]['id'], 1)
        self.assertEqual('test4', rulesInDb[0]['name'])

        # Get the same from the writer
        writer = Writer(base_path + '/static/config_valid.yml', jobInDb['name'])
        rulesInObj = writer.get_rules('output_test')
        self.assertEqual(len(rulesInObj), 3)
        self.assertEqual('test', rulesInObj['test']['name'])
        self.assertEqual('test2', rulesInObj['test2']['name'])
        self.assertEqual('test3', rulesInObj['test3']['name'])

        rulesInObj = writer.get_rules('output_test3')
        self.assertEqual(len(rulesInObj), 1)
        self.assertEqual('test4', rulesInObj['test4']['name'])

        # Get a field to make sure we also have rules with it
        fieldInObj = writer.get_field('output_test')
        self.assertEqual(len(fieldInObj['rules']), 3)


    def test_bad_job_prop(self):
        writer = Writer(base_path + '/static/config_valid.yml')
        with self.assertRaisesRegex(KeyError, 'does_not_exist is not a valid job property'):
            writer.get_job_prop_type('does_not_exist')


    def _get_job(self):
        conn = sqlite3.connect('/tmp/test.db')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM job WHERE name = 'test'")
        data = cursor.fetchall()
        self.assertEqual(1, len(data))

        job = dict()
        i = 0
        for col in cursor.description:
            job[col[0]] = data[0][i]
            i += 1

        return job


    def _get_hooks(self, job_id: int, num: int):
        conn = sqlite3.connect('/tmp/test.db')
        cursor = conn.cursor()

        # Create the table
        from impulsare_job.models import Hook
        Hook()

        cursor.execute("SELECT * FROM hook WHERE job_id = {} ORDER BY name ASC".format(job_id))
        data = cursor.fetchall()
        self.assertEqual(num, len(data))

        hooks = list()
        for hook in data:
            cols = dict()
            i = 0
            for col in cursor.description:
                cols[col[0]] = hook[i]
                i += 1

            hooks.append(cols)

        return hooks


    def _get_fields(self, job_id: int, num: int):
        conn = sqlite3.connect('/tmp/test.db')
        cursor = conn.cursor()

        # Create the table
        from impulsare_job.models import Job
        Job()

        cursor.execute("SELECT * FROM field WHERE job_id = {} ORDER BY output ASC".format(job_id))
        data = cursor.fetchall()
        self.assertEqual(num, len(data))

        fields = list()
        for field in data:
            cols = dict()
            i = 0
            for col in cursor.description:
                cols[col[0]] = field[i]
                i += 1

            fields.append(cols)

        return fields


    def _get_rules(self, field_id: int, num: int):
        conn = sqlite3.connect('/tmp/test.db')
        cursor = conn.cursor()

        # Create the table
        from impulsare_job.models import Job
        Job()

        cursor.execute("SELECT * FROM rule WHERE field_id = {} ORDER BY name ASC".format(field_id))
        data = cursor.fetchall()
        self.assertEqual(num, len(data))

        rules = list()
        for rule in data:
            cols = dict()
            i = 0
            for col in cursor.description:
                cols[col[0]] = rule[i]
                i += 1

            rules.append(cols)

        return rules


if __name__ == "__main__":
    unittest.main()
