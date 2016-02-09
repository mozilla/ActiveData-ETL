The Transforms
==============

Each of these modules is reponsible for a particular data transform.  The main 
function is conventionally called `process`, and must have the following 
parameters:

* `source_key` - The key given to the data intended for transform:  This is 
usually the prefix of an S3 bucket item.  It is usually not used, rather only 
when reporting a problem.
* `source` - the data to be transformed; assume it is a generator of unicode 
lines.
* `dest_bucket` - The destination for the transformed data.  Use `dest_bucket.extend()` 
to add lists of key-value pairs in form of `{"id": key, "value": value}`
* `resources` - Additional objects that can help with the transform.  Usually 
`resources.hg` is defined as the Mozilla Mercurial cache for annotating data 
with repository information. 
* `please_stop` - This method will be run in a thread, so this is the stop 
signal.  Check it often

