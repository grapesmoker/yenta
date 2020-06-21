=====
Yenta
=====


.. image:: https://img.shields.io/pypi/v/yenta.svg
        :target: https://pypi.python.org/pypi/yenta

.. image:: https://img.shields.io/travis/grapesmoker/yenta.svg
        :target: https://travis-ci.com/grapesmoker/yenta

.. image:: https://readthedocs.org/projects/yenta/badge/?version=latest
        :target: https://yenta.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




Yet 'Nother Taskrunner


* Free software: MIT license
* Documentation: https://yenta.readthedocs.io.

Introduction
------------

Yenta is YEt 'Nother TAskrunner; it executes a pipeline, defined by a series of tasks and dependencies among them.
The goal of Yenta is to provide a reasonable feature set while maintaining simplicity and usability. Yenta tasks are
simply functions decorated with the :code:`@task` decorator and complying with a specific format for their arguments.

Yenta is inspired in part by the functional state management pattern used in projects like Redux. Although I would
hesitate to call Yenta "functional" in the strict sense, it does use a caching logic according to which identical
inputs are assumed to produce identical outputs under default conditions. This means that Yenta will automatically
reuse the cached output of a task if nothing about the inputs has changed.

The name "Yenta" is an old Yiddish name; in the American Yiddish theater of the 30s, a character named Yenta
was depicted as a busy-body, so the name became a byword for someone who won't mind their own business.

Features
--------

* Graph-based task execution
* Lightweight
* Idempotent tasks
* Simple, intuitive API

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
