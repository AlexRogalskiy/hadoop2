# Lab4: spring-data-hadoop
**Target:** code less, gain productivity and produce easier to maintain sources.

**Start point:** Non Spring project.

## Follow TODOs
1. Edit pom.xml and add a dependency on *spring-hadoop* (managed from parent poms).
2. Add Spring nature to your project (right click on eclipse project).
3. Create a *job-context.xml* configuration file in *src/main/resources*:
  * right click on resources directory and choos "New → Other... → Spring → Spring Bean Configuration File"
  * enable "hadoop" namespace
  * declare required beans:
    - *configuration*
    - *job*: get inspiration from TP3 for attributes values. Put *CsvFieldCountMapper.idx=2* as tag value
    - *job-runner*: reference job bean by id and enable *run-at-startup*
    - add following javascript code as job-runner *pre-action*: `fs['delete']("target/test-classes/output", true)` (this trashes job output directory before each run)
4. Create a *postcodejob.properties* test resource file and move there all hard-coded values from job-context
5. Edit job-context, enable *context* namespace and declare a *property-placeholder* bean with *location="postcodejob.properties"*
6. Run the tests

## Note
Spring-hadoop does much more than what we use it here for. 
It can for instance chain Hive, Pig and Hbase jobs, integrate with spring-batch and so on. Please refer to [project website](http://projects.spring.io/spring-hadoop/) for detailed and up to date info.
