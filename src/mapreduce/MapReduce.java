package mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import mapreduce.mapper.MapperTask;
import mapreduce.mapper.MapperFunc;
import mapreduce.datasource.FileDataSource;
import mapreduce.datasource.DataSource;
import mapreduce.reducer.ReducerFunc;
import mapreduce.reducer.ReducerTask;
import mapreduce.shuffler.Shuffler;

import java.io.IOException;

import static mapreduce.util.Constants.THREAD_COUNT;

public class MapReduce<K, V, InterKey, InterVal, OutKey, OutVal> {

    private DataSource<K, V> dataSource;
    private MapperFunc<K, V, InterKey, InterVal> mapperFunc;
    private ReducerFunc<InterKey, InterVal, OutKey, OutVal> reducerFunc;
    private ExecutorService executorService;
    private List<List<Entry<OutKey, OutVal>>> finalOutput;

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

    public List<Entry<OutKey, OutVal>> compute() throws Exception {
        Objects.requireNonNull(dataSource);
        Objects.requireNonNull(mapperFunc);
        Objects.requireNonNull(reducerFunc);
        //Split phase
        Map<K, V> dataSets = dataSource.read();

        //Mapping phase.
        var mapperTaskFutures = new ArrayList<Future<List<Entry<InterKey, InterVal>>>>();
        dataSets.forEach((id, dataset) -> mapperTaskFutures.add(executorService.submit(new MapperTask<>(mapperFunc, id, dataset))));
        //Shuffle data as they are processed by mapping phase.
        ConcurrentMap<InterKey, List<InterVal>> shuffledData = new ConcurrentHashMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(mapperTaskFutures.size());
        for (var mapperRes : mapperTaskFutures)
            executorService.submit(new Shuffler<>(shuffledData, mapperRes.get(), countDownLatch));

        //Wait until all shufflers, shuffle their data.
        countDownLatch.await();

        //Create reducer tasks.
        List<ReducerTask<InterKey, InterVal, OutKey, OutVal>> reducerTasks = createReducerTasks(shuffledData);

        //Reduce phase
        var allReducersResults = new ArrayList<Future<List<Entry<OutKey, OutVal>>>>();
        reducerTasks.forEach(reducer -> allReducersResults.add(executorService.submit(reducer)));
        for (Future<List<Entry<OutKey, OutVal>>> reducer : allReducersResults) finalOutput.add(reducer.get());

        executorService.shutdown();
        return this.collectOutput();
    }

    private List<ReducerTask<InterKey, InterVal, OutKey, OutVal>> createReducerTasks(ConcurrentMap<InterKey, List<InterVal>> groupedData) {
        var reducerTasks = new ArrayList<ReducerTask<InterKey, InterVal, OutKey, OutVal>>();
        IntStream.range(0, THREAD_COUNT).forEach((i) -> reducerTasks.add(new ReducerTask<>(reducerFunc)));
        groupedData.keySet().forEach((interKey) -> {
            int reducerTaskPos = Math.abs(interKey.hashCode() % THREAD_COUNT);
            reducerTasks.get(reducerTaskPos).insertEntry(Map.entry(interKey, groupedData.get(interKey)));
        });
        return reducerTasks;
    }

    private List<Entry<OutKey, OutVal>> collectOutput() {
        var joinedOutput = new ArrayList<Entry<OutKey, OutVal>>();
        finalOutput.forEach(list -> joinedOutput.addAll(list));
        return joinedOutput;
    }

}
