pyLibrary.convert
-----------------

General conversion library with functions in the form `<from_type> "2" <to_type>`.
Most of these functions are simple wrappers around common Python functions,
some are more esoteric.  A few are special implementations.

pyLibrary.jsons
---------------

Fast JSON encoder used in `convert.value2json()`

pyLibrary.net_json
------------------

A JSON-like storage format intended for configuration files

Load your settings easily:

    settings = get(url):

The file format is JSON, with some important features.

###Allow Comments###

End-of-line Comments are allowed, using either `#` or `//` prefix:

```javascript
    {
        "key1": "value1",  //Comment 1
    }
```
```python
        "key1": "value1",  #Comment 1
```

Multiline comments are also allowed, using either Python's triple-quotes
(`""" ... """`) or Javascript's block quotes `/*...*/`

```javascript
    {
        "key1": /* Comment 1 */ "value1",
    }
```
```python
        "key1": """Comment 1""" "value1",
```

###Reference other JSON###

The `$ref` key is special.  Its value is interpreted as a URL, which is loaded
and expected to be more JSON

**Absolute Internal References**

The simplest form of URL is an absolute reference to a node in the same
document:


```python
    {
        "message": "Hello world",
        "repeat": {"$ref": "message"}
    }
```

The object with the `$ref` is replaced with the value it points to:

```python
    {
        "message": "Hello world",
        "repeat": "Hello world"
    }
```

**Relative Internal References**

References that start with dot (`.`) are relative, with each additional dot
referring to successive parents.   In this case the `..` refers to the
ref-object's parent, and expands just like the pevious example:

```python
    {
        "message": "Hello world",
        "repeat": {"$ref": "..message"}
    }
```

**File References**

Configuration is often stored on the local filesystem.  You can in line the
JSON fouhnd in a file by using the `file://` scheme:

It is good practice to store sensitive data in a secure place...

```python
    {# LOCATED IN C:\users\kyle\password.json
        "host": "database.example.com",
        "username": "kyle",
        "password": "pass123"
    }
```
...and then refer to it in your configuration file:

```python
    {
        "host": "example.com",
        "port": "8080",
        "$ref": "file:///C:/users/kyle/password.json"
    }
```

which will be expanded at run-time to:

```python
    {
        "host": "example.com",
        "port": "8080",
        "username": "kyle",
        "password": "pass123"
    }
```

Please notice the trimple slash (`///`) is referring to an absolute file
reference.

**Object References**

Ref-objects that point to other objects (dicts) are not replaced completely,
but rather are merged with the target; with the ref-object
properties taking precedence.   This is seen in the example above: The "host"
property is not overwritten by the target's.

**Relative File Reference**

Here is the same, using a relative file reference; which is relative to the
file that contains

```python
    {#LOCATED IN C:\users\kyle\config.json
        "host": "example.com",
        "port": "8080",
        "$ref": "file://password.json"
    }
```

**Home Directory Reference**

You may also use the tilde (`~`) to refer to the current user's home directory.
Here is the same again, but this example can be anywhere in the filesystem.

```python
    {
        "host": "example.com",
        "port": "8080",
        "$ref": "file://~/password.json"
    }
```

**HTTP Reference**

Configuration can be stored remotly, especially in the case of larger
configurations which are too unwieldy to inline:

```python
    {
        "schema":{"$ref": "http://example.com/sources/my_db.json"}
    }
```

**Scheme-Relative Reference**

You are also able to leave the scheme off, so that whole constellations of
configuration can refer to each other no matter if they are on the local
filesystem, or remote:

```python
    {# LOCATED AT SOMEWHERE AT http://example.com
        "schema":{"$ref": "///sources/my_db.json"}
    }
```

And, of course, relative references are also allowed:

```python
    {# LOCATED AT http://example.com/sources/config.json
        "schema":{"$ref": "//sources/my_db.json"}
    }
```

**Fragment Reference**

Some remote configuration files are quite large...

```python
    {# LOCATED IN C:\users\kyle\password.json
        "database":{
            "username": "kyle",
            "password": "pass123"
        },
        "email":{
            "username": "ekyle",
            "password": "pass123"
        }
    }
```

... and you only need one fragment.  For this use the hash (`#`) followed by
the dot-delimited path into the document:

```python
    {
        "host": "mail.example.com",
        "username": "ekyle"
        "password": {"$ref": "//~/password.json#email.password"}
    }
```


pyLibrary.strings.expand_template()
-----------------------------------

    expand_template(template, value)

A simple function that replaces variables in `template` with the properties
found in `value`. Variables are indicated by the double mustaches;
`{{example}}` is a variable.

Properties are converted to `unicode()` before replacing variables.  In the case
of complex properties; converted to JSON.  Further string manipulation can be
performed by feeding properties to functions using the pipe (`|`) symbol:

```python
    >>> from pyLibrary.strings import expand_template
    >>> total = 123.45
    >>> some_list = [10, 11, 14, 80]

    >>> print expand_template("it is currently {{now|datetime}}", {"now": 1420119241000})
    it is currently 2015-01-01 13:34:01

    >>> print expand_template("Total: {{total|right_align(20)}}", {"total": total})
    Total:               123.45

    >>> print expand_template("Summary:\n{{list|json|indent}}", {"list": some_list})
    Summary:
            [10, 11, 14, 80]
```

Look into the `pyLibrary.strings.py` to see a full list of transformation
functions.

Variables are not limited to simple names: You may use dot (`.`) to specify
paths into the properties
```python
    >>> details = {"person":{"name":"Kyle Lahnakoski", "age":40}}
    >>> print expand_template("{{person.name}} is {{person.age}} years old", details)
    Kyle Lahnakoski is 40 years old
```
Templates are not limited to strings, but can also be queries to expand lists
found in property paths:

<incomplete>
