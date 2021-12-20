#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', 'networkx~=2.5', 'colorama~=0.4.4', 'pydot~=1.4.1',
                'rich~=9.11.0', 'more_itertools~=8.7.0']

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Jerry Vinokurov",
    author_email='grapesmoker@gmail.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Yet 'Nother Taskrunner",
    entry_points={
        'console_scripts': [
            'yenta=yenta.cli:yenta',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    long_description_content_type="text/x-rst",
    include_package_data=True,
    keywords='yenta',
    name='yenta',
    packages=find_packages(include=['yenta', 'yenta.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/grapesmoker/yenta',
    version='0.3.2',
    zip_safe=False,
)
