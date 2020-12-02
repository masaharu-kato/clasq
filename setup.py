from setuptools import setup, find_packages

setup(
    name='libsql',
    version='0.1.0',
    url='https://github.com/masaharu-kato/libsql',
    author='Masaharu Kato',
    author_email='fudai.mk@gmail.com',
    description='Python SQL Utilities',
    packages=['libsql'],
    package_dir={'libsql': 'src'},
    install_requires=[
        'pyyaml',
        'mysql-connector-python',
    ],
)
