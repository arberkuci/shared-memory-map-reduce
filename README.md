# Shared memory MapReduce.

This is a multi-core/shared-memory implementation of the very well-known programming model called MapReduce.
[MapReduce](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/16cb30b4b92fd4989b8619a61752a2387c6dd474.pdf)
was widely popularized in the 2000s by a paper that was written by some engineers at Google. This paper also inspired
the widely used open-source framework [Hadoop](http://hadoop.apache.org/) for implementing distributed processing of
large amounts of data. The paper introduced a novel approach to compute large amounts of data (in terms of petabytes) in
a scalable and resilient manner.

In this project, the MapReduce programming model is implemented, in the context of multi-core computers. A fixed set
of (a cached pool of) threads, created and managed by the environment is used to run the various taks of the
computation.

This particular implementation goes through the following phases:

1. **Loading/Splitting data phase**: This is the initial phase, where the data are loaded in memory. It goes through the
   following two steps:
    1. Reading the data from a specified source. The source of you data can either be:
        1. From a file. If you want the source of the data to be from a file, use the
           class [FileDataSouce.java](/src/mapreduce/datasource/FileDataSource.java).
        2. Or if your data are not in a file, you can implement the
           interface [DataSource.java](/src/mapreduce/datasource/DataSource.java) to retrieve the data from anywhere you
           need.
    2. After the data are loaded, the specific implementation
       of [DataSource.java](/src/mapreduce/datasource/DataSource.java) should split the input into as many pieces as
       there are available cores/threads in the machine.


2. **Mapping phase**: The developer defines an implementation of the
   interface [MapperFunc](/src/mapreduce/mapper/MapperFunc.java) which simply takes a piece of the data from the
   splitting phase (a key and a value) and maps that to an intermediate key/value. Each piece of the data from the
   splitting phase is processed in parallel.


3. **Shuffling phase**: As the tasks from the mapping phase are completed, the shuffling phase starts. The shuffling
   phase is responsible for grouping together each intermediate key `<InterKey, InterVal>` (that came out of the mapping
   phase) and associate the key with all its `InterVal` values producing a map of the form `<InterKey, list(InterVal)>`.
   The shuffling phase does not finish until the last task from the mapping phase finishes, and the computation does not
   proceed until all shuffling tasks are completed.


4. **Reducing phase**: This is the last phase of the computation. The developer should provide an implementation of the
   interface [ReducerFunc.java](src/mapreduce/reducer/ReducerFunc.java) which takes as input the intermediate
   key `InterKey` and the list of intermediate values `InterVal` and produces as output a tuple of the
   form `<OutKey, OutVal>`. The previous shuffling phase groups `list<InterKey, InterVal>` (the output of the mapping
   phase)
   into `list<InterKey, list<InterVal>>`. The class [MapReduce.java](src/mapreduce/MapReduce.java)
   makes sure that each task of this phase takes approximately the same number of elements
   from `list<InterKey, list<InterVal>>` to process. Each task of this phase is carried in parallel. After each task of
   this phase is completed the output (`<OutKey, OutVal>`) is grouped into `list(<OutKey, OutVal>)` and returned to the
   caller of the method `compute` of the class `MapReduce.java`.

