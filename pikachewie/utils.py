"""
===========================================================
pikachewie.utils -- Utilities used internally by PikaChewie
===========================================================

"""

__all__ = ['Missing', 'cached_property', 'delegate']

# singleton representing an unspecified parameter value
Missing = object()


class cached_property(object):
    """A decorator that converts a function into a lazy property.

    The function wrapped is called the first time to retrieve the result and
    then that calculated result is used the next time you access the value::

        class Foo(object):

            @cached_property
            def foo(self):
                # calculate something important here
                return 42

    The class has to have a `__dict__` in order for this property to work.

    Cloned from `werkzeug.utils.cached_property`.

    ref: https://github.com/mitsuhiko/werkzeug/blob/master/LICENSE

    """

    # implementation detail: this property is implemented as non-data
    # descriptor.  non-data descriptors are only invoked if there is
    # no entry with the same name in the instance's __dict__.
    # this allows us to completely get rid of the access function call
    # overhead.  If one choses to invoke __get__ by hand the property
    # will still work as expected because the lookup logic is replicated
    # in __get__ for manual invocation.

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __get__(self, obj, type_=None):
        if obj is None:
            return self
        value = obj.__dict__.get(self.__name__, Missing)
        if value is Missing:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value
        return value


def delegate(obj, attr):
    """Define an instance property that is delegated to self.<obj>.<attr>.

    Example::

        >>> class MyClass(object):
        ...
        ...     def __init__(self, obj):
        ...         self.obj = obj
        ...
        ...     foo = delegate('obj', 'count')
        ...
        >>> a = MyClass(obj=list())
        >>> a.foo
        <built-in method count of list object at 0x104ad15f0>
        >>> a.obj.count
        <built-in method count of list object at 0x104ad15f0>

    """
    return property(lambda self: getattr(getattr(self, obj), attr))


def import_namespaced_class(namespaced_class):
    """Import and return a handle to the `namespaced_class`.

    :param namespaced_class: namespaced class (e.g., `"foo.bar.Baz"`)
    :type namespaced_class: str

    :returns: a handle to the class
    :rtype: class:`type`

    """
    # Split up our string containing the import and class
    parts = namespaced_class.split('.')

    # Build our strings for the import name and the class name
    import_name = '.'.join(parts[0:-1])
    class_name = parts[-1]

    import_handle = __import__(import_name, fromlist=class_name)

    # Return the class handle
    return getattr(import_handle, class_name)
