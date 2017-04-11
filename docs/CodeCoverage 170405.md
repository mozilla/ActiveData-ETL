This is the second monthly update on our CodeCoverage effort:


## The Progress to Date

* Coverage is scheduled to run daily on central. https://treeherder.mozilla.org/#/jobs?repo=mozilla-central&filter-searchStr=cov
* The UCOSP[1] students have just about completed their term, and can show aggregate statistics by folder with the code coverage ui [5]. 
* `grcov`[4] was written mcastelluccio to process coverage artifacts coming coverage test runs much faster, reducing processing time from hours to days. `grcov` also outputs multiple formats, including for coveralls.io
* There is exploration into using coveralls.io: https://coveralls.io/jobs/24085087, the main page is https://coveralls.io/github/marco-c/gecko-dev.
* c/c++ coverage is being ETL'ed into our unified coverage datastore, but does not yet show in the UI


## The Blockers

* Getting Rust to emit coverage artifacts is important: https://bugzilla.mozilla.org/show_bug.cgi?id=1335518
* C/C++ coverage 
* Continuing the hard work of enabling codecoverage on all our tests: https://bugzilla.mozilla.org/show_bug.cgi?id=1301170
* Add per line coverage: Work has already started [5]. The aggregate numbers are good for identifying sections of code that are not tested, but we need line-level details to inform action. https://github.com/ericdesj/moz-codecover-ui/issues/17


## The Biggest Problem

**Our test suites are unstable**. Many of you may already know that many tests fail intermittently; these "intermittents" impact code coverage; either by breaking the test run, and preventing coverage collection; or but changing code paths. The challenge is automating the metatdata collection and having a strategy to mitigate the loss of data. [8] https://bugzilla.mozilla.org/show_bug.cgi?id=1337241

Even so, **test runs are not deterministic**, so we expect fluctuation in the aggregate coverage statistics; How large this fluctuation is we do not know.


## The Current Plan

For the next month we will be continuing work on:

* validating the data and process, including adding Rust and more test suites. 
* Adding per-line coverage to the CoCo UI, and explore how coveralls.io can be used to make that job easier  
* Explore how to leverage Taskcluster-generated coverage artifacts with local coverage tools [9] https://bugzilla.mozilla.org/show_bug.cgi?id=1350446

## Motivation

*Unchanged from last email* - Knowing code coverage statistics and code coverage specifics can help evaluate risk in code: It can show what test suites cover what lines, it can show if new code is covered by an existing test.  There are many other exciting insights we can extract from code coverage. But first, we must collect it.

## Reference

[1] UCOSP (Undergraduate Capstone Open Source Projects) http://ucosp.ca/

[2] The actual plan: https://docs.google.com/document/d/1dOWi18qrudwaOThNAYoCMS3e9LzhxGUiMLLrQ_WVR9w/edit#

[3] Kyle Lahnakoski email: klahnakoski@mozilla.org  irc: ekyle on #ateam@irc.mozilla.org

[4] grcov https://github.com/marco-c/grcov

[5] (Co)de (Co)verage UI preview - https://ericdesj.github.io/moz-coco-w17-preview/

[5b] CoCo is on Github - https://github.com/mozilla/moz-coco

[6] CoCo relay - https://github.com/ericdesj/moz-coco-relay

[7] Rust Coverage Tracking https://bugzilla.mozilla.org/show_bug.cgi?id=1335518

[8] Knowing when the coverage is complete: https://bugzilla.mozilla.org/show_bug.cgi?id=1337241

[9] Coverage with local tools: https://bugzilla.mozilla.org/show_bug.cgi?id=1350446
