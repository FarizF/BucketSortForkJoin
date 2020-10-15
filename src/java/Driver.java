import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

public class Driver {

    private static int processors = 1;

    public static final Path USER_DIR_PATH = Paths.get(System.getProperty("user.dir"));
    public static final FileSystem FILE_SYSTEM = USER_DIR_PATH.getFileSystem();
    public static final File OUTPUT_FILE_SEQUENTIAL = new File(System.getProperty("user.dir") + FILE_SYSTEM.getSeparator() + "bucketSortAWFFoutputSeq.txt");
    public static final File OUTPUT_FILE_PARALLEL = new File(System.getProperty("user.dir") + FILE_SYSTEM.getSeparator() + "bucketSortAWFFoutputPar.txt");
    public static ForkJoinPool forkJoinPool;
    public static int staticBucketCount;

    private static HashMap<String, TreeMap<String, Long[]>> resultAggregatesMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        int runCount;
        staticBucketCount = 0;
        try {
            runCount = Integer.parseInt(args[0]);
            staticBucketCount = Integer.parseInt(args[1]);
        } catch (NumberFormatException|ArrayIndexOutOfBoundsException e) {
            runCount = Integer.parseInt(args[0]);
        }

        // Clear files
        Files.writeString(OUTPUT_FILE_SEQUENTIAL.toPath(), "");
        Files.writeString(OUTPUT_FILE_PARALLEL.toPath(), "");

        System.out.println("Output file location: " + OUTPUT_FILE_SEQUENTIAL.toPath().toString());
        System.out.println("Output file location: " + OUTPUT_FILE_PARALLEL.toPath().toString());

        for (int i = 0; i < runCount; i++) {
            System.out.println("Starting global run: " + (i + 1)  + "/" + runCount + ".");
            processors = 1;

            for (int k = 0; k < 4; k++) {
                forkJoinPool = new ForkJoinPool(processors);

                System.out.println("Using " + processors + " threads to sort(parallel only).");
                var ref = new Object() { int max = 50000; };
                System.out.println("Using " + bucketAmountResolver(ref.max, 10) + " buckets to sort.");

                for (int j = 0; j < 4; j++) {

                    long startTime = System.currentTimeMillis();
                    System.out.print("Generating input("+ ref.max +"): ");
                    int[] formattedInput = generateInput(ref.max);
                    long duration;
                    System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

                    String currentRunString = (k+1) + " - " + (j+1);
                    System.out.println("Run #: " + currentRunString + "\n");

                    BucketSort bucketSort = new BucketSort();
                    System.out.println("Sequential bucketsort: ");
                    startTime = System.currentTimeMillis();
                    String sortedResultSequential = Arrays.toString(bucketSort.sequentialBucketSort(formattedInput.clone(), ref.max, processors));
                    duration = System.currentTimeMillis() - startTime;
                    System.out.println("Sequential bucketsort took " + duration + " milliseconds.");
                    addToResultsMap("Sequential bucketsort", currentRunString, runCount, duration, i);

                    Files.writeString(OUTPUT_FILE_SEQUENTIAL.toPath(),
                            "Run #: " + (k+1) + " - " + (j+1) + "\n" + sortedResultSequential + "\n", StandardOpenOption.APPEND
                    );

                    int[] inputClone = formattedInput.clone();

                    currentRunString = (k+1) + " - " + (j+1);
                    startTime = System.currentTimeMillis();
                    System.out.println("Parallel bucketsort: ");
                    Files.writeString(OUTPUT_FILE_PARALLEL.toPath(), "Run #: " + (k+1) + " - " + (j+1) + "\n", StandardOpenOption.APPEND);
                    ForkJoinTask task = forkJoinPool.submit(() -> bucketSort.parallelBucketSort(inputClone, ref.max, processors));
                    try {
                        task.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    duration = System.currentTimeMillis() - startTime;
                    System.out.println("Parallel bucketsort took " + duration + " milliseconds.\n");
                    addToResultsMap("Parallel bucketsort", currentRunString, runCount, duration, i);

                    currentRunString = (k+1) + " - " + (j+1);
                    startTime = System.currentTimeMillis();
                    System.out.println("ForkJoin Sequential bucketsort:");
                    task = forkJoinPool.submit(() -> bucketSort.sequentialBucketSort(formattedInput.clone(), ref.max, processors));
                    String sortedResultForkJoinSequential;
                    try {
                        sortedResultForkJoinSequential = String.valueOf(task.get());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    duration = System.currentTimeMillis() - startTime;
                    System.out.println("ForkJoin sequential bucketsort took " + duration + " milliseconds.\n");
                    addToResultsMap("ForkJoin sequential bucketsort", currentRunString, runCount, duration, i);

                    ref.max = ref.max * 2;
                }

                if (processors * 2 > Runtime.getRuntime().availableProcessors()) {
                    System.out.println("Cannot use " + processors * 2 + " threads or more.");
                    break;
                } else {
                    processors = processors * 2;
                }
            }
        }

        printResultAverages();

        System.out.println("If assessment was finished, please delete the output files situated in the root folder of this project.");
    }

    public static int bucketAmountResolver(int inputSize, int preferedBucketSize) {
        if (staticBucketCount == 0) {
            if (inputSize < preferedBucketSize) return 1;
            return inputSize / preferedBucketSize;
        } else {
            return staticBucketCount;
        }
    }

    /**
     *
     * @return Generated collection of randomized primitive integers
     */
    private static int[] generateInput(int max) {
        Random ran = new Random();
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < max; i++) {
            int min = 0;
            stringBuilder.append(min + ran.nextInt(max - min + 1));

            if (i != max - 1) {
                stringBuilder.append(",");
            }
        }

        String longString = stringBuilder.toString();

        String[] splitInput = longString.split(",");
        int[] formattedInput = new int[splitInput.length];

        for (int i = 0; i < splitInput.length; i++)
            formattedInput[i] = Integer.parseInt(splitInput[i]);

        return formattedInput;
    }

    private static void addToResultsMap(String algorithm, String currentRunString, int runCount, long duration, int i) {
        if (resultAggregatesMap.get(algorithm) != null) {
            TreeMap<String, Long[]> currentInnerMap = resultAggregatesMap.get(algorithm);
            if (currentInnerMap.get(currentRunString) != null) {
                Long[] newLongArray = currentInnerMap.get(currentRunString);
                newLongArray[i] = duration;

            } else {
                Long[] newLongArray = new Long[runCount];
                newLongArray[0] = duration;
                currentInnerMap.put(currentRunString, newLongArray);
            }
        } else {
            TreeMap<String, Long[]> runTreeMap = new TreeMap<>();
            Long[] newLongArray = new Long[runCount];
            newLongArray[0] = duration;
            runTreeMap.put(currentRunString, newLongArray);
            resultAggregatesMap.put(algorithm, runTreeMap);
        }
    }

    private static void printResultAverages() {
        for (Map.Entry<String, TreeMap<String, Long[]>> algorithmMap : resultAggregatesMap.entrySet()) {
            System.out.println(algorithmMap.getKey() + " result averages:");

            for (Map.Entry<String, Long[]> runResultsMapEntrySet : algorithmMap.getValue().entrySet()) {
                System.out.print(runResultsMapEntrySet.getKey());
                double runAverage = 0;
                for (Long runResult : runResultsMapEntrySet.getValue())
                    runAverage += runResult;

                System.out.println(": " + (int) runAverage / runResultsMapEntrySet.getValue().length + "ms");
            }

            System.out.println();
        }
    }
}
