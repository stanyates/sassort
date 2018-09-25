import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Prototype3.java - parallel sort utility
 * <p>
 * Coding requirement: Java program: Use multiple threads to sort an array of numbers.
 * <p>
 * This is based on the description of java.util.Arrays.parallelSort() at:
 * {@link https://stackoverflow.com/questions/17328077/difference-between-arrays-sort-and-arrays-parallelsort}.
 * <p>
 * This describes parallelSort as recursively breaking up the array into segments that are
 * smaller than a threshold size, sorting each segment with Arrays.sort(), then merging pairs of
 * consecutive segments.  Segment sorts and merges are performed on multiple threads using the
 * Fork/Join framework - {@link https://docs.oracle.com/javase/tutorial/essential/concurrency/forkjoin.html}.
 * <p>
 * It turns out, at least in Java 10, that whereas the algorithm here recurses to create segments
 * until a threshold size is reached, comments in java.util.ArraysParallelSortHelpers.java
 * suggest that the array is segmented into quarters only with only two levels of merges.  I was
 * concerned that repeated / recursive merging would affect performance, and the performance data
 * I'm getting supports that (very large data sizes perform much worse than ParallelSort, smaller
 * sizes perform better).
 * <p>
 * Something else to profile is seeing if the scratch buffer getting allocated per request is an issue
 * for large data sets.
 * <p>
 * This would be something to investigate in a fourth prototype.
 * <p>
 * A fifth prototype could implement its own threading model in order to demonstrate how that might work.
 */
public class Prototype3
{
    static final private int DEFAULT_SEGMENT_MAX_SIZE = 10000;
    
    private int segment_max_size = DEFAULT_SEGMENT_MAX_SIZE;
    private ForkJoinPool pool = new ForkJoinPool();
    public boolean debug = false;
    
    public Prototype3()
    {
    }
    
    public void setDebug(boolean debug)
    {
        this.debug = debug;
    }
    
    public void setSegmentSize(int size)
    {
        segment_max_size = size;
    }
    
    /**
     * Sorts an array using multiple threads via the Fork/Join framework.
     * @param arr the array to sort
     */
    public void psort(int[] arr)
    {
        int[] buffer = new int[arr.length];
        pool.invoke(new Worker(arr, 0, arr.length, segment_max_size, buffer, debug));
    }
    
    static private class Worker extends RecursiveAction
    {
        private static final long serialVersionUID = 2330451448017834196L;
        
        private int seg_max_size;
        private int[] buffer;
        boolean debug;
        
        private int[] arr;
        private int offset;
        private int len;
        
        public Worker(int[] arr, int offset, int len, int seg_max_size, int[] buffer, boolean debug)
        {
            this.arr = arr;
            this.offset = offset;
            this.len = len;
            
            this.seg_max_size = seg_max_size;
            this.buffer = buffer;
            this.debug = debug;
        }
    
        public void compute()
        {
            if (debug)
            {
                System.out.println("compute: offset=" + offset + ", len=" + len);
            }

            if (len <= seg_max_size)
            {
                if (debug)
                {
                    System.out.println("compute: directly calling Arrays.sort, offset=" + offset + ", len=" + len);
                }
                Arrays.sort(arr, offset, offset + len);
            }
            else
            {
                int offset1 = offset;
                int len1 = len / 2;
                int offset2 = offset1 + len1;
                int len2 = len - len1;
                
                if (debug)
                {
                    System.out.println("compute: running a fork-join segmented sort");
                }
                
                invokeAll( new Worker(arr, offset1, len1, seg_max_size, buffer, debug), new Worker(arr, offset2, len2, seg_max_size, buffer, debug) );
                
                doMerge(arr, offset1, len1, offset2, len2);
            }

            if (debug)
            {
                System.out.print("compute complete: ");
                for (int i = 0; i < len; i++)
                {
                    System.out.print((offset + i) + ": " + arr[offset + i] + ", ");
                }
                System.out.println();
            }
        }

        private void doMerge(int[] arr, int offset1, int len1, int offset2, int len2)
        {
            if (debug)
            {
                System.out.println("doMerge: offset1=" + offset1 + ", len1=" + len1 + ", offset2=" + offset2 + ", len2=" + len2);
            }

            int i = 0;
            int j = 0;
            int k = 0;

            while ( k < len1 + len2 )
            {
                if (i < len1 && j < len2)
                {
                    if ( arr[offset1 + i] < arr[offset2 + j] )
                    {
                        buffer[offset1 + k++] = arr[offset1 + i++];
                    }
                    else
                    {
                        buffer[offset1 + k++] = arr[offset2 + j++];
                    }                
                }

                else if (i < len1)
                {
                    buffer[offset1 + k++] = arr[offset1 + i++];                
                }

                else
                {
                    buffer[offset1 + k++] = arr[offset2 + j++];                                
                }
            }

            System.arraycopy( buffer, offset1, arr, offset1, len1 + len2 );
        }
    }
}
