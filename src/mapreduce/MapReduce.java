package mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import mapreduce.mapper.MapperTask;
import mapreduce.mapper.MapperFunc;
import mapreduce.datasource.FileDataSource;
import mapreduce.datasource.DataSource;
import mapreduce.reducer.ReducerFunc;
import mapreduce.reducer.ReducerTask;
import mapreduce.util.Tuple;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static mapreduce.util.Constants.THREAD_COUNT;

public class MapReduce<K, V, InterKey, InterVal, OutKey, OutVal> {

    private DataSource<K, V> dataSource;
    private MapperFunc<K, V, InterKey, InterVal> mapperFunc;
    private ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc;
    private ExecutorService executorService;
    private List<List<Tuple<OutKey, OutVal>>> finalOutput;

    public MapReduce() {
        this.executorService = Executors.newCachedThreadPool();
        this.finalOutput = new ArrayList<>();
    }

    public MapReduce(String dataSourceFilePath) throws IOException {
        this();
        this.dataSource = (DataSource<K, V>) new FileDataSource(dataSourceFilePath);
    }

    public void supplyDataSource(DataSource<K, V> dataSource) {
        this.dataSource = dataSource;
    }

    public void supplyMapper(MapperFunc<K, V, InterKey, InterVal> mapperFunc) {
        this.mapperFunc = mapperFunc;
    }

    public void supplyReducer(ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc) {
        this.reducerFunc = reducerFunc;
    }

    public void compute() throws Exception {
        long start = System.currentTimeMillis();
        Objects.requireNonNull(dataSource);
        Objects.requireNonNull(mapperFunc);

        //Split phase
        List<Tuple<K, V>> dataSplits = dataSource.read();

        //Mapper phase.
        var mapperTasks = new ArrayList<Future<List<Tuple<InterKey, InterVal>>>>();
        dataSplits.forEach(dataSplit -> mapperTasks.add(executorService.submit(createMapperTask(dataSplit))));
        var mapPhaseOutputs = new ArrayList<List<Tuple<InterKey, InterVal>>>();
        for (var mapperRes : mapperTasks) mapPhaseOutputs.add(mapperRes.get());

        //Shuffle phase
        Shuffler shuffler = new Shuffler(mapPhaseOutputs, reducerFunc).shuffleData();
        List<ReducerTask<InterKey, InterVal, OutKey, OutVal>> reducerTasks = shuffler.createReducerTasks();

        //Reduce phase
        var allReducersResults = new ArrayList<Future<List<Tuple<OutKey, OutVal>>>>();
        reducerTasks.forEach(reducer -> allReducersResults.add(executorService.submit(reducer)));
        for (Future<List<Tuple<OutKey, OutVal>>> reducer : allReducersResults) finalOutput.add(reducer.get());
        executorService.shutdown();
    }

    public List<Tuple<OutKey, OutVal>> collectOutput() {
        var joinedOutput = new ArrayList<Tuple<OutKey, OutVal>>();
        finalOutput.forEach(list -> joinedOutput.addAll(list));
        return joinedOutput;
    }

    private MapperTask<K, V, InterKey, InterVal> createMapperTask(Tuple<K, V> dataSplit) {
        return new MapperTask<>(mapperFunc, dataSplit);
    }

    private class Shuffler {

        private List<List<Tuple<InterKey, InterVal>>> data;
        private ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc;
        private Map<InterKey, List<InterVal>> helperMap;

        public Shuffler(List<List<Tuple<InterKey, InterVal>>> data, ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc) {
            this.data = data;
            this.reducerFunc = reducerFunc;
            this.helperMap = new HashMap<>();
        }

        public Shuffler shuffleData() {
            data.forEach(list -> list.forEach(tuple -> {
                InterKey key = tuple.getKey();
                InterVal value = tuple.getValue();
                if (helperMap.containsKey(key)) {
                    helperMap.get(key).add(value);
                } else {
                    var newList = new ArrayList<InterVal>();
                    newList.add(value);
                    helperMap.put(key, newList);
                }
            }));
            return this;
        }

        public List<ReducerTask<InterKey, InterVal, OutKey, OutVal>> createReducerTasks() {
            var reducerTasks = new ArrayList<ReducerTask<InterKey, InterVal, OutKey, OutVal>>();
            IntStream.range(0, THREAD_COUNT).forEach((i) -> reducerTasks.add(new ReducerTask<>(reducerFunc)));
            helperMap.keySet().stream().forEach((interKey) -> {
                int reducerTaskPos = Math.abs(interKey.hashCode() % THREAD_COUNT);
                reducerTasks.get(reducerTaskPos).insertTuple(new Tuple<>(interKey, helperMap.get(interKey)));
            });
            return reducerTasks;
        }

    }

}
