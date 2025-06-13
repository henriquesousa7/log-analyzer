# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['log_analyzer',
 'log_analyzer.analysis',
 'log_analyzer.utils',
 'log_analyzer.ingest',
 'log_analyzer.visualization']

package_data = \
{'': ['*']}

install_requires = \
['pandas>=2.3.0,<3.0.0', 'pyspark>=4.0.0,<5.0.0', 'streamlit>=1.45.1,<2.0.0']

setup_kwargs = {
    'name': 'log_analyzer',
    'version': '1.0.0',
    'description': '',
    'long_description': '# Log Analyzer\n> Last Version: __1.0.0__',
    'author': 'Henrique de Sousa',
    'author_email': 'None',
    'maintainer': 'None',
    'maintainer_email': 'None',
    'url': 'None',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.12,<3.14',
}


setup(**setup_kwargs)

