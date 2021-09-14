package mapreduce.util;

import java.util.concurrent.ForkJoinPool;

public class Constants {

    public final static int THREAD_COUNT = ForkJoinPool.getCommonPoolParallelism();

    private Constants() {
    }

}
