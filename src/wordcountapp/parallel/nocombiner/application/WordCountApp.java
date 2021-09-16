package wordcountapp.parallel.nocombiner.application;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.StringTokenizer;
import mapreduce.MapReduce;

public class WordCountApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java <<>filePath>");
            return;
        }
        long start = System.currentTimeMillis();
        var mapReduce = new MapReduce<Long, String, String, Integer, String, Integer>(args[0]);
        mapReduce.supplyMapper((id, docSplit) -> {
            var wordCountRes = new ArrayList<Entry<String, Integer>>();
            StringTokenizer stringTokenizer = new StringTokenizer(docSplit, " \\t\\n\\r\\f,.!?;:][()_=+-<>'”");
            while (stringTokenizer.hasMoreTokens()) wordCountRes.add(Map.entry(stringTokenizer.nextToken(), 1));
            return wordCountRes;
        });
        mapReduce.supplyReducer((word, counts) -> Map.entry(word, counts.size()));
        List<Entry<String, Integer>> finalOutput = mapReduce.compute();
        long end = System.currentTimeMillis();
        System.out.println("Took in total: " + (end - start) / 1000);
        System.out.println(finalOutput);
    }


}
