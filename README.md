[Documentation](http://pikachewie.readthedocs.org/)

PikaChewie
==========
PikaChewie is your [pika](https://pika.readthedocs.org/en/latest/) co-pilot,
providing [RabbitMQ](http://www.rabbitmq.com/) messaging tools with
[bandoliers](http://www.angelfire.com/pa2/crash19/bandolier.html) included.

Installation
------------
To install PikaChewie from source:

```bash
  $ git clone git@github.com:bdeeney/PikaChewie.git
  $ cd pikachewie
  $ make dev
```

Running Test Suite
------------------
To run the test suite:

```bash
  $ make test
```

Development
-----------
[GitFlow]: http://nvie.com/posts/a-successful-git-branching-model/ "A successful Git branching model"
[HubFlow]: http://datasift.github.com/gitflow/GitFlowForGitHub.html "Using GitFlow With GitHub"

PikaChewie is developed using [HubFlow], DataSift's
[fork](https://github.com/datasift/gitflow) of the [GitFlow] git extensions.
The working branch is 'develop' and 'master' should match what is deployed
in production.

Building Docs
-------------
To generate and view the docs:

```bash
  $ make doc
  $ open doc/html/index.html
```
