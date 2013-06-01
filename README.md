[Documentation](https://pikachewie.readthedocs.org/)

PikaChewie
==========
PikaChewie is your [pika](https://pika.readthedocs.org/) co-pilot,
providing [RabbitMQ](http://www.rabbitmq.com/) messaging tools with
[bandoliers](http://www.angelfire.com/pa2/crash19/bandolier.html) included.

Lineage
-------
PikaChewie is an extensive reworking of [Gavin M. Roy](https://github.com/gmr)'s
[rejected](https://github.com/gmr/rejected) framework, and is intended as a
lighter-weight alternative to its predecessor.  A significant amount of code from
rejected has been ported into PikaChewie, either as-is or in refactored form.

Installation
------------
To install PikaChewie from source:

```bash
  $ git clone git@github.com:bdeeney/PikaChewie.git
  $ cd PikaChewie
  $ virtualenv env
  $ . env/bin/activate
  $ pip install -r requirements.pip
```

Running the Test Suite
----------------------
After installing the prequisites (previous section), run tox from the project root:

```bash
  $ tox
```

Development
-----------
[GitFlow]: http://nvie.com/posts/a-successful-git-branching-model/ "A successful Git branching model"
[HubFlow]: http://datasift.github.com/gitflow/GitFlowForGitHub.html "Using GitFlow With GitHub"

PikaChewie is developed using [HubFlow], DataSift's
[fork](https://github.com/datasift/gitflow) of the [GitFlow] git extensions.


Building Docs
-------------
To generate and view the docs:

```bash
  $ python setup.py develop
  $ python setup.py build_sphinx
  $ open doc/html/index.html
```
