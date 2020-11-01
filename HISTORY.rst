=======
History
=======

0.1.0 (2020-05-27)
------------------

* First release on PyPI.
* Working pipeline execution with caching and reuse of results

0.2.0 (2020-07-01)
------------------

* Selector functionality implemented

0.2.1 (2020-07-10)
------------------

* Write intermediate pipeline results to a temp file and catch the case
  where a non-serializable object is returned

0.2.2 (2020-07-10)
------------------

* Append running dir to sys.path

0.2.3 (2020-07-10)
------------------

* Initialize artifact date_created by default

0.2.4 (2020-07-16)
------------------

* FileArtifacts can be directories
* Artifacts now have a `meta` field to which arbitrary JSON-serializable information can be attached
* If an exception is raised during task execution, the stack trace is printed
* pydot is now used for graph generation
* Artifact equality is checked by comparing location and hash

0.3.0 (2020-10-31)
------------------

* Serialization scheme moved from JSON to pickle to improve performance.
* "Values" no longer explicitly exist; anything that can be pickled is a valid value.
* Multiple pipelines with different names can be used in the same project.
