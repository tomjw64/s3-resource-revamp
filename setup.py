from setuptools import setup
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent


def read_readme():
    with open(PROJECT_DIR / 'README.md') as f:
        return f.read()


def parse_requirements(req_file):
    """Parse a requirements.txt file to a list of requirements"""
    with open(PROJECT_DIR / req_file, 'r') as fb:
        reqs = [
            req for req in fb.readlines()
            if req.strip() and not req.startswith('#')
        ]
    return list(reqs)


main_requires = parse_requirements('requirements/main.txt')
test_requires = parse_requirements('requirements/test.txt')

setup(
    name="s3-resource",
    version='0.1.0',
    description='Concourse CI resource for s3 compatible object storage',
    long_description=read_readme(),
    url='',
    author='OpenStax Content Engineering',
    license='AGPLv3.0',
    packages=['test', 'src'],
    python_requires='>=3.6',
    install_requires=main_requires,
    extras_require={
        'dev': test_requires
    },
    tests_require=test_requires,
    test_suite='test',
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'check = src.check:main',
            'in = src.in_:main',
            'out = src.out:main',
        ]
    }
)