# coding=utf-8

from setuptools import setup, find_packages

setup(
    name='PyJsEngine',
    version=0.1,
    description=(
        'A DIY scripting engine supporting most common customizations. '
        'This engine connect JavaScript with Python functions you customized by powerful functions provided by \'js2py\'. '
        'Now with this engine, you can distribute your doing-many-things scripts separated from your main program. '
    ),
    long_description=open('README.rst').read(),
    author='DJun',
    author_email='djunxp@gmail.com',
    maintainer='DJun',
    maintainer_email='djunxp@gmail.com',
    license='GNU General Public License v3 (GPLv3)',
    packages=find_packages(),
    platforms=["all"],
    url='https://github.com/djun/PyJsEngine',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Software Development :: Libraries',
    ],
    install_requires=[
        'js2py',
        'jinja2',
    ],
)
