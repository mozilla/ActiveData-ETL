
Consistent dicts, lists and Nones
=================================

This library is solving the problem of consistency (closure) under the dot(.)
and slice [::] operators.  The most significant difference is in the dealing
with None, missing keys, and missing items in lists.

Struct replaces dict
--------------------

```Struct``` is used to declare an instance of an anonymous type, and has good
features for manipulating JSON.  Anonymous types are necessary when
writing sophisticated list comprehensions, or queries, and to keep them
readable.  In many ways, dict() can act as an anonymous type, but it does
not have the features listed here.

 1. ```a.b == a["b"]```
 2. missing keys are handled gracefully, which is beneficial when being used in
    set operations (database operations) without raising exceptions <pre>
a = wrap({})
&gt;&gt;&gt; a == {}
a.b == None
&gt;&gt;&gt; True
a.b.c == None
&gt;&gt;&gt; True
a[None] == None
&gt;&gt;&gt; True</pre>
    missing keys are common when dealing with JSON, which is often almost anything.
    Unfortunately, you do loose the ability to perform <code>a is None</code>
    checks:  You must always use <code>a == None</code> instead.
 3. remove an attribute by assigning `None` (eg ```a.b = None```)
 4. you can access paths as a variable:  ```a["b.c"] == a.b.c```.  Of course,
 this creates a need to refer to literal dot (.), which can be done by
 escaping with backslash: ```a["b\\.c"] == a["b\.c"]```
 5. you can set paths to values, missing dicts along the path are created:<pre>
a = wrap({})
&gt;&gt;&gt; a == {}
a["b.c"] = 42   # same as a.b.c = 42
&gt;&gt;&gt; a == {"b": {"c": 42}}</pre>
 6. path assignment also works for the `+=` operator <pre>
a = wrap({})
&gt;&gt;&gt; a == {}
a.b.c += 1
&gt;&gt;&gt; a == {"b": {"c": 1}}
a.b.c += 42
&gt;&gt;&gt; a == {"b": {"c": 43}}
</pre>
 7. attribute names (keys) are corrected to unicode - it appears Python
 object.getattribute() is called with str() even when using ```from __future__
 import unicode_literals```
 8. by allowing dot notation, the IDE does tab completion and my spelling
 mistakes get found at "compile time"

### Examples in the wild###

```Struct``` is a common pattern in many frameworks even though it goes by
different names, some examples are:

 * ```jinja2.environment.Environment.getattr()```  to allow convenient dot notation
 * ```argparse.Environment()``` - code performs ```setattr(e, name, value)``` on
  instances of Environment to provide dot(.) accessors
 * ```collections.namedtuple()``` - gives attribute names to tuple indicies
  effectively providing <code>a.b</code> rather than <code>a["b"]</code>
     offered by dicts
 * C# Linq requires anonymous types to avoid large amounts of boilerplate code.
 * D3 has many of these conventions ["The function's return value is
  then used to set each element's attribute. A null value will remove the
  specified attribute."](https://github.com/mbostock/d3/wiki/Selections#attr)

### Notes ###
 * More on missing values: [http://www.np.org/NA-overview.html](http://www.np.org/NA-overview.html)
it only considers the legitimate-field-with-missing-value (Statistical Null)
and does not look at field-does-not-exist-in-this-context (Database Null)
 * [Motivation for a 'mutable named tuple'](http://www.saltycrane.com/blog/2012/08/python-data-object-motivated-desire-mutable-namedtuple-default-values/)
(aka anonymous class)

Null is the new None
--------------------
```None``` is a primitive that can not be extended, so we create a new type,
```NullType``` and instances, ```Null```, which are closed under the dot(.)
and slice [::] operators.  In many ways, ```Null``` acts as both an impotent
list and an impotent dict.

 1. ```a[Null] == Null```
 2. ```Null.a == Null```
 3. ```Null[a] == Null```
 4. ```Null[a:b:c] == Null```
 5. ```a[Null:b:c] == Null```
 6. ```a[b:Null:c] == Null```

To minimize the use of ```Null``` in our code we let comparisons
with ```None``` succeed. The right-hand-side of the above comparisons can be
replaced with ```None``` in all cases.

NullTypes can also perform lazy assignment for increased expressibility.

```python
    a = wrap({})
    x = a.b.c
    x == None
    >>> True
    x = 42
    a.b.c == 42
    >>> True
```
in this case, specific ```Nulls```, like  ```x```, keep track of the path
assignment so it can be used in later programming logic.  This feature proves
useful when transforming hierarchical data; adding deep children to an
incomplete tree.

### Null Arithmetic ###

When `Null` is part of arithmetic operation (boolean or otherwise) it results in ```Null```:

 * ```a ∘ Null == Null```
 * ```Null ∘ a == Null```

where `∘` is any binary operator.



Motivation for StructList
-------------------------

```StructList``` is the final type required to to provide closure under the
dot(.) and slice [::] operators.  Not only must ```StructList``` deal with
```Nulls``` (and ```Nones```) but also provide fixes to Python's inconsistent
slice operator.

###The slice operator in Python2.7 is inconsistent###

At first glance, the python slice operator ```[:]``` is elegant and powerful.
Unfortunately it is inconsistent and forces the programmer to write extra code
to work around these inconsistencies.

```python
    my_list = ['a', 'b', 'c', 'd', 'e']
```

Let us iterate through some slices:

```python
    my_list[4:] == ['e']
    my_list[3:] == ['d', 'e']
    my_list[2:] == ['c', 'd', 'e']
    my_list[1:] == ['b', 'c', 'd', 'e']
    my_list[0:] == ['a', 'b', 'c', 'd', 'e']
```

Looks good, but this time let's use negative indices:

```python
    my_list[-4:] == ['b', 'c', 'd', 'e']
    my_list[-3:] == ['c', 'd', 'e']
    my_list[-2:] == ['d', 'e']
    my_list[-1:] == ['e']
    my_list[-0:] == ['a', 'b', 'c', 'd', 'e']  # [] is expected
```

Using negative indices ```[-num:]``` allows the programmer to slice relative to
the right rather than the left.  When ```num``` is a constant this problem is
never revealed, but when ```num``` is a variable, then the inconsistency can
reveal itself.

```python
    def get_suffix(num):
        return my_list[-num:]   # wrong
```

So, clearly, ```[-num:]``` can not be understood as a suffix slice, rather
something more complicated; especially considering that ```num``` could be
negative.

I advocate never using negative indices in the slice operator.  Rather, use the
```right()``` method instead which is consistent for a range ```num```:

```python
    def right(_list, num):
        if num <= 0:
            return []
        return _list[-num:]
```

###Python 2.7 ```__getslice__``` is broken###

It would be nice to have our own list-like class that implements slicing in a
way that is consistent.  Specifically, we expect to solve the inconsistent
behaviour seen when dealing with negative indices.

As an example, I would like to ensure my over-sliced-to-the-right and over-
sliced-to-the-left  behave the same.  Let's look at over-slicing-to-the-right,
which behaves as expected on a regular list:

```python
    assert 3 == len(my_list[1:4])
    assert 4 == len(my_list[1:5])
    assert 4 == len(my_list[1:6])
    assert 4 == len(my_list[1:7])
    assert 4 == len(my_list[1:8])
    assert 4 == len(my_list[1:9])
```

Any slice that requests indices past the list's length is simply truncated.
I would like to implement the same for over-slicing-to-the-left:

```python
    assert 2 == len(my_list[ 1:3])
    assert 3 == len(my_list[ 0:3])
    assert 3 == len(my_list[-1:3])
    assert 3 == len(my_list[-2:3])
    assert 3 == len(my_list[-3:3])
    assert 3 == len(my_list[-4:3])
    assert 3 == len(my_list[-5:3])
```

Here is an attempt:

```python
    class MyList(list):
        def __init__(self, value):
            self.list = value

        def __getslice__(self, i, j):
            if i < 0:  # CLAMP i TO A REASONABLE RANGE
                i = 0
            elif i > len(self.list):
                i = len(self.list)

            if j < 0:  # CLAMP j TO A REASONABLE RANGE
                j = 0
            elif j > len(self.list):
                j = len(self.list)

            if i > j:  # DO NOT ALLOW THE IMPOSSIBLE
                i = j

            return [self.list[index] for index in range(i, j)]

        def __len__(self):
            return len(self.list)
```

Unfortunately this does not work.  When the ```__len__``` method is defined
```__getslice__``` defines ```i = i % len(self)```: Which
makes it impossible to identify if a negative value is passed to the slice
operator.

The solution is to implement Python's extended slice operator ```[::]```,
which can be implemented using ```__getitem__```; it does not suffer from this
wrap-around problem.

```python
    class BetterList(list):
        def __init__(self, value):
            self.list = value

        def __getslice__(self, i, j):
            raise NotImplementedError

        def __len__(self):
            return len(self.list)

        def __getitem__(self, item):
            if not isinstance(item, slice):
                # ADD [] CODE HERE

            i = item.start
            j = item.stop
            k = item.step

            if i < 0:  # CLAMP i TO A REASONABLE RANGE
                i = 0
            elif i > len(self.list):
                i = len(self.list)

            if j < 0:  # CLAMP j TO A REASONABLE RANGE
                j = 0
            elif j > len(self.list):
                j = len(self.list)

            if i > j:  # DO NOT ALLOW THE IMPOSSIBLE
                i = j

            return [self.list[index] for index in range(i, j)]
```

