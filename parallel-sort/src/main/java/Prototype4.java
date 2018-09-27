import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Prototype4.java - parallel sort utility
 * <p>
 * Coding requirement: Java program: Use multiple threads to sort an array of numbers.
 * <p>
 * This follow-on to Prototype3 is based on comments about java.util.Arrays.parallelSort() in
 * the source code for java.util.ArraysParallelSortHelpers.java in jdk-10.0.2.
 * There it says:
 * <p>
 * <pre>
 * Basic algorithm:
 * if array size is small, just use a sequential quicksort (via Arrays.sort)
 *         Otherwise:
 *         1. Break array in half.
 *         2. For each half,
 *             a. break the half in half (i.e., quarters),
 *             b. sort the quarters
 *             c. merge them together
 *         3. merge together the two halves.
 *
 * One reason for splitting in quarters is that this guarantees that
 * the final sort is in the main array, not the workspace array.
 * (workspace and main swap roles on each subsort step.)  Leaf-level
 * sorts use the associated sequential sort.
 * </pre>
 * In other words, it looks like the recursive algorithm that was described at
 * {@link https://stackoverflow.com/questions/17328077/difference-between-arrays-sort-and-arrays-parallelsort}
 * is not used at all.  Instead, 4 segments are sorted then merged and merged again.  This reduces the buffer copy
 * overhead that I suspected with the recursive approach.
 * <p>
 * Also since this approach isn't recursive, instead of using the Fork/Join framework, this uses a fixed thread pool
 * of size 4, and runs the 4 sort tasks in parallel to completion, then the 2 first merges in parallel.
 * <p>
 * What isn't done yet is to eliminate copies from the workspace array back into the source array.
 * Performance testing shows significantly better performance for the very large 10M array, and I predict
 * that eliminating the copy back during the merge would address the current performance difference with java.util.Arrays.parallelSort().
 */
public class Prototype4
{
    static final private int MINIMUM_SIZE = 1000;
    
    private ExecutorService pool = Executors.newFixedThreadPool(4);
    public boolean debug = false;
    
    public Prototype4()
    {
    }
    
    public void shutdown()
    {
        pool.shutdown();
    }
    
    public void setDebug(boolean debug)
    {
        this.debug = debug;
    }
    
    /**
     * Sorts an array using multiple threads via ExecutorService.
     * @param arr the array to sort
     */
    public void psort(int[] arr)
    {
        if ( arr.length < MINIMUM_SIZE )
        {
            Arrays.sort( arr );
            return;
        }
        
        int[] buffer = new int[arr.length];
        
        // Logically divide the array into 4 segments and sort each segment.
        
        int offset1 = 0;
        int offset3 = arr.length / 2;
        int offset2 = offset3 / 2;
        int offset4 = offset3 + offset2;
        int len1 = offset2;
        int len2 = offset3 - offset2;
        int len3 = offset4 - offset3;
        int len4 = arr.length - offset4;
        
        ArrayList<Callable<Object>> workers = new ArrayList<>();
        
        workers.add( new Sorter(arr, offset1, len1, debug ) );
        workers.add( new Sorter(arr, offset2, len2, debug ) );
        workers.add( new Sorter(arr, offset3, len3, debug ) );
        workers.add( new Sorter(arr, offset4, len4, debug ) );
        
        try
        {
            List<Future<Object>> firstSortResults = pool.invokeAll( workers );
            
            for (Future<Object> result : firstSortResults)
            {
                result.get();  // check for error; raises an exception if the worker raised an exception
            }
        }
        catch ( Exception ex )
        {
            // TODO development only
            throw new RuntimeException("One of the first sort tasks threw an exception: " + ex, ex);
        }
        
        workers.clear();
        
        // Merge the first and second segment, and the third and fourth segment in parallel.
        
        workers.add( new Merger(arr, buffer, debug, offset1, len1, offset2, len2) );
        workers.add( new Merger(arr, buffer, debug, offset3, len3, offset4, len4) );
        
        try
        {
            List<Future<Object>> firstMergeResults = pool.invokeAll( workers );
            
            for (Future<Object> result : firstMergeResults)
            {
                result.get();  // check for error; raises an exception if the worker raised an exception
            }
        }
        catch ( Exception ex )
        {
            // TODO development only
            throw new RuntimeException("One of the first merge tasks threw an exception: " + ex, ex);
        }
        
        // Merge the two merged segments on the current thread.
        new Merger(arr, buffer, debug, offset1, len1 + len2, offset3, len3 + len4).call();
    }
    
    static private class Sorter implements Callable<Object>
    {
        boolean debug;
        
        private int[] arr;
        private int offset;
        private int len;
        
        public Sorter(int[] arr, int offset, int len, boolean debug)
        {
            this.arr = arr;
            this.offset = offset;
            this.len = len;
            
            this.debug = debug;
        }
    
        public String call()
        {
            if (debug)
            {
                System.out.println("Sorter: offset=" + offset + ", len=" + len);
            }

            Arrays.sort(arr, offset, offset + len);
            
            if ( debug )
            {
                System.out.print("Sorter complete, offset=" + offset + ", len=" + len + ": " );
                for (int i = offset; i < len; i++)
                {
                    System.out.print((offset + i) + ": " + arr[offset + i] + ", ");
                }
                System.out.println();
            }
            
            return null;
        }
    }

    static private class Merger implements Callable<Object>
    {
        private int[] buffer;
        boolean debug;
        
        private int[] arr;
        private int offset1;
        private int len1;
        private int offset2;
        private int len2;

        public Merger(int[] arr, int[] buffer, boolean debug, int offset1, int len1, int offset2, int len2)
        {
            this.arr = arr;
            this.offset1 = offset1;
            this.len1 = len1;
            this.offset2 = offset2;
            this.len2 = len2;
            
            this.buffer = buffer;
            this.debug = debug;
        }
        
        public String call()
        {
            if (debug)
            {
                System.out.println("Merger: offset1=" + offset1 + ", len1=" + len1 + ", offset2=" + offset2 + ", len2=" + len2);
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

            // The merge still copies the local buffer back to the source array for each merge.  However
            // the JDK parallelSort merges the first results into the local buffer then on the final merge
            // merges the local buffer back into the source array.  Try that approach in another prototype.
            
            System.arraycopy( buffer, offset1, arr, offset1, len1 + len2 );
            
            if ( debug )
            {
                System.out.print("Merger complete, offset=" + offset1 + ", len=" + (len1 + len2) + ": " );
                for (int o = offset1; o < len1 + len2; o++)
                {
                    System.out.print((offset1 + o) + ": " + arr[offset1 + o] + ", ");
                }
                System.out.println();
            }
            
            return null;
        }
    }
}
