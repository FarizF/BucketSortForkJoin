import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class BucketSort {

    public volatile ArrayList<Integer>[] buckets;

    // TODO: REFACTOR VARS
    public int N;
    public int K;
    public int M;

    public int[] sequentialBucketSort(int[] intArr, int max, int processors) {
        long startTime = System.currentTimeMillis();
        System.out.print("\tCreating buckets: ");

        int noOfBuckets = Driver.bucketAmountResolver(max, processors);
        // Create bucket array
        ArrayList<Integer>[] buckets = new ArrayList[noOfBuckets];
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tAssign a new ArrayList object to all indices of buckets: ");
        // Associate a list with each index
        // in the bucket array
        for (int i = 0; i < noOfBuckets; i++) {
            buckets[i] = new ArrayList<>();
        }
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tAssigning numbers from array to the proper bucket by hash: ");
        // Assign numbers from array to the proper bucket
        // by using hashing function
        for (int num : intArr) {
            buckets[hashBucketIndex(num, max, noOfBuckets)].add(num);
        }
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tSorting the individual buckets: ");
        // sort buckets
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = insertionSort(buckets[i]);
        }
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tMerging the buckets to get sorted array: ");
        int i = 0;
        // Merge buckets to get sorted array
        for (List<Integer> bucket : buckets) {
            for (int num : bucket) {
                intArr[i++] = num;
            }
        }
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        return intArr;
    }

    public void parallelBucketSort(int[] formattedInput, int max, int processors) {
        //TODO: refactor vars
        N = formattedInput.length;
        K = processors;
        M = Driver.bucketAmountResolver(N, 10);

        long startTime = System.currentTimeMillis();
        System.out.print("\tInitializing buckets: ");

        // STEP 1
        initBuckets(M);

        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tInitializing partitions: ");

        // STEP 2
        AtomicReference<ArrayList<int[]>> partitions = new AtomicReference<>();

        final double N_DIV_K = Math.floor((double) N / K); // PARTITION SIZE OF N/K ELEMENTS
        partitions.set(initPartitions(formattedInput, K, N_DIV_K));

        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tPartitioning buckets(parallel): ");

        // STEP 3 + 4
        runParallelPartitionBucketer(partitions.get(), max);

        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        // STEP 5
        startTime = System.currentTimeMillis();
        System.out.print("\tSorting partitions containing buckets(parallel): ");

        runBucketSortTasks();
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

    }

    public ArrayList<Integer> insertionSort(ArrayList<Integer> array) {
        int n = array.size();
        for (int j = 1; j < n; j++) {
            int key = array.get(j);
            int i = j - 1;
            while ((i > -1) && (array.get(i) > key)) {
                array.set(i + 1, array.get(i));
                i--;
            }
            array.set(i + 1, key);
        }
        return array;
    }

    /**
     * \
     *
     * @param bucketsM Size of initial collection of buckets at post partitioning stage
     */
    private void initBuckets(int bucketsM) {
        buckets = new ArrayList[bucketsM];

        for (int i = 0; i < bucketsM; i++) {
            buckets[i] = new ArrayList<>();
        }
    }

    private ArrayList<int[]> initPartitions(int[] formattedInput, int threadsK, double partitionLengthNdivK) {
        ArrayList<int[]> partitions = new ArrayList<>();

        int lengthCounter = 0;

        for (int i = 0; i < threadsK; i++) {
            int startIndex = i * (int) partitionLengthNdivK;
            int endIndex = (i * (int) partitionLengthNdivK) + (int) partitionLengthNdivK;

            int[] partition = null;

            if (i == threadsK-1 && lengthCounter < formattedInput.length) {
                partition = Arrays.copyOfRange(formattedInput, startIndex, formattedInput.length);
            } else {
                partition = Arrays.copyOfRange(formattedInput, startIndex, endIndex);
            }
            lengthCounter = lengthCounter + partition.length;
            partitions.add(partition);
        }

        return partitions;
    }

    private void runParallelPartitionBucketer(ArrayList<int[]> partitions, int max) {

        ForkJoinTask forkJoinTask = Driver.forkJoinPool.submit( () -> {
            partitions.forEach(partition -> {
                for (int i : partition) {

                    double targetIndex = BucketSort.hashBucketIndex(i, max, buckets.length);

                    if (targetIndex > buckets.length) {
                        System.out.println(targetIndex);
                    }

                    try {
                        buckets[(int) targetIndex].add(i);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        System.out.println("index length " + targetIndex + "\t buckets length:" + buckets.length + " " + i);
                    }
                }
            });
        });

        try {
            forkJoinTask.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    private void runBucketSortTasks() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");

        for (int i = 0; i < buckets.length; i++) {

            // STEP 6
            int finalI = i;
            ForkJoinTask forkJoinTask = Driver.forkJoinPool.submit( () -> {
                buckets[finalI] = insertionSort(buckets[finalI]);
            });

            if (buckets[finalI] != null) {
                try {
                    forkJoinTask.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }

                boolean isEmpty = buckets[finalI].isEmpty();

                if (finalI == buckets.length - 1 && !isEmpty) {
                    stringBuilder.append(Arrays.toString(buckets[finalI].toArray())
                            .replace("[", "")
                            .replace("]", ""));
                } else if (!isEmpty){
                    stringBuilder.append(Arrays.toString(buckets[finalI].toArray())
                            .replace("[", "")
                            .replace("]", ", "));
                }
            }

        }

        stringBuilder.append("]");
        try {
            Files.writeString(Driver.OUTPUT_FILE_PARALLEL.toPath(), stringBuilder.toString() + "\n", StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Shit's whack, cannot write to file.");
        }
    }

    /**
     * Used to retrieve index of bucket an input value will be put in. In scenario of 24 threads, a max of 10000,
     * denominator is 10000 / 24 = 416. bucketIndex is retrieved by dividing the input value by this denominator.
     *
     * @param value
     * @param max
     * @param bucketLength
     * @return Index of the bucket an input value will be sorted in.
     */
    public static int hashBucketIndex(double value, double max, double bucketLength) {
        double bucketDenominator = max / bucketLength;
        int bucketIndex = (int) Math.floor(value / bucketDenominator);

        // If index (in event when value close to max) equals bucketLength, default to index of last bucket
        // (e.g. Math.floor(10000 / 416) == 24 && bucketLength == 24)
        if (bucketIndex >= bucketLength) bucketIndex = (int) bucketLength - 1;

        return bucketIndex;
    }
}
