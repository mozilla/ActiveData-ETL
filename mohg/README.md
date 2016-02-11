

MoHg
====

Uses ElasticSearch as a fast cache for Mozilla's Mercurial repository.

Usage
-----

Make an instance of the cache, modifying the parameters to fit your use case


```python
	hg = HgMozillaOrg({
		"use_cache": true,
		"hg":{
			"url": "https://hg.mozilla.org"
		},
		"branches": {
			"host": "http://localhost",
			"port": 9200,
			"index": "branches",
			"type": "branch",
			"timeout": 300,
			"schema": {
				"$ref": "//../resources/branch.json"
			},
			"debug": false,
			"limit_replicas": false
		},
		"repo": {
			"host": "http://localhost",
			"port": 9200,
			"index": "repo",
			"type": "revision",
			"timeout": 300,
			"schema": {
				"$ref": "//../resources/revision.json"
			},
			"debug": false,
			"limit_replicas": false
		}
	})
```

then call `get_revision()` when you need a revision:

```python
	rev = hg.get_revision({
		"changeset":{"id": "b3649fd5cd7a76506d2cf04f45e39cbc972fb553"},
		"branch": {"name": "mozilla-inbound"}
	})
```

... and yes, the call really does require a "complicated" dict parameter:  The intent was to convert partial revision objects into completed revision objects. 