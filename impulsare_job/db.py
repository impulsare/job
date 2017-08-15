import os

from impulsare_logger import Logger
from impulsare_config import Reader as ConfigReader
from .models import open_db


class Db():
    """Main DB Interactions"""

    _job_props_type = {
        'name': str,
        'active': bool,
        'description': str,
        'priority': int,
        'input': str,
        'input_parameters': dict,
        'output': str,
        'output_parameters': dict,
        'mode': str
        }


    def __init__(self, config_file: str = None):
        """Write logs to a file, with the app's name"""

        self._logger = Logger('job', config_file)

        base_path = os.path.abspath(os.path.dirname(__file__))
        config_specs = base_path + '/static/specs.yml'
        config_default = base_path + '/static/default.yml'

        self._config = ConfigReader().parse(config_file, config_specs, config_default)
        self._logger.log.info('Opening SQLite DB "{}"'.format(self._config.get('job')['db']))

        self._db = open_db(self._config.get('job')['db'])


    def get_job_prop_type(self, prop: str):
        if prop not in self._job_props_type:
            raise KeyError('{} is not a valid job property'.format(prop))

        return self._job_props_type[prop]
