

Structured Logging integrated with Chained Exceptions is Good
-------------------------------------------------------------

Exception handling and logging are undeniably linked.  There are many instances
where exceptions are raised and must be logged, except when they are
appropriately handled.  The greatness of exception handling semantics comes from
decoupling the cause from the solution, but this is at odds with clean logging -
which couples raising and handling to make appropriate decisions about what to
ultimately emit to the log.  For this reason, the logging module is responsible
for collecting the trace and context, raising the exception, and then deducing
if there is something that will handle it (so it can be ignored), or if it
really must be logged.

This library also expects all log messages and exception messages to have named
parameters so they can be stored in easy-to-digest JSON, which can be processed
by downstream tools.
