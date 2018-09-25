/**
 * Performance test program for Prototype3.java.
 * 
 * Runs Prototype3.psort() with different segment sizes as well as Arrays.sort() and Arrays.parallelSort()
 * with various input array sizes.  The test data is a reverse-ordered array of integers.
 */
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class Test3
{
    static final private int TEST_REPEAT_COUNT = 5;
    
    public void perfTest()
    {
        // Hackish: <0 for Arrays.parallelSort(), 0 for Arrays.sort(), >0 for our psort()
        int[] segmentSizes = { -1, 0, 100, 1000, 10000, 100000 };
        int[] dataSizes = { 100, 1000, 10000, 100000, 1000000, 10000000 };
        
        TreeMap<Integer, TreeMap<Integer, Long>> stats = new TreeMap<>();
        
        Prototype3 p = new Prototype3();
        
        for (int i = 0; i < dataSizes.length; i++)
        {
            int dataSize = dataSizes[i];
            
            for (int j = 0; j < segmentSizes.length; j++)
            {
                int segmentSize = segmentSizes[j];
                
                String strategy;
                
                if (segmentSize > 0)
                    strategy = "psort" + segmentSize;
                else if (segmentSize == 0)
                    strategy = "Arrays.sort";
                else
                    strategy = "Arrays.parallelSort";
                
                System.out.println("strategy=" + strategy + ", data size=" + dataSize);
                                        
                p.setSegmentSize( segmentSize );
                
                // Sequential, reverse-sorted values for test data.
                int[] data = new int[dataSize];
                int start = data.length;
                for (int d = 0; d < data.length; d++)
                {
                    data[d] = start--;
                }
                
                // Clone and sort with Arrays.sort to create a known good result to verify result against.
                int[] target = data.clone();
                Arrays.sort( target );
                for (int t = 0; t < target.length; t++)
                {
                    if (target[t] != t + 1)
                    {
                        throw new RuntimeException("unexpected value at index=" + t + ": " + data[t]);
                    }
                }
                
                // Repeat a given test multiple times and report the average time.
                
                long elapsedTime = 0;
                
                for ( int n = 0; n < TEST_REPEAT_COUNT; n++ )
                {
                    long startTime = System.nanoTime();
                    if ( segmentSize > 0 )
                    {
                        p.psort( data );
                    }
                    else if ( segmentSize < 0 )
                    {
                        Arrays.parallelSort( data );
                    }
                    else
                    {
                        Arrays.sort( data );
                    }
                    elapsedTime += System.nanoTime() - startTime;
                    // Check the result data against the expected result.
                    if ( !Arrays.equals( data, target ) )
                    {
                        throw new RuntimeException( "bad data in sorted result, dataSize=" + dataSize + ", segmentSize=" + segmentSize );
                    } 
                }
                
                elapsedTime /= (long)TEST_REPEAT_COUNT;
                
                // Store elapsed time for the run, indexed by strategy then by data set size.
                TreeMap<Integer, Long> optionToElapsedTimeMap = stats.get( dataSize );
                if (optionToElapsedTimeMap == null)
                {
                    optionToElapsedTimeMap = new TreeMap<Integer, Long>();
                    stats.put( dataSize, optionToElapsedTimeMap );
                }
                optionToElapsedTimeMap.put( segmentSize, elapsedTime );
                
                //System.out.println(segmentSize + ", " + dataSize + ", " + String.format("%.6f", (double)elapsedTime / 1000000000));
            }
        }
        
        // Write CSV for import into Excel, one row per data set size, one column per sort strategy
        // (Arrays.parallelSort, Arrays.sort, various segment sizes for our own psort).
        
        String line = "";
        
        for ( Integer segmentSize : segmentSizes )
        {
            line += ",";
            
            if (segmentSize > 0)
                line += "psort" + segmentSize;
            
            else if (segmentSize < 0)
                line += "Arrays.parallelSort";
            
            else if (segmentSize == 0)
                line += "Arrays.sort";
        }
        
        System.out.println();
        System.out.println(line);
        
        for (Map.Entry<Integer, TreeMap<Integer, Long>> byDataSize : stats.entrySet())
        {
            line = byDataSize.getKey().toString();
            
            for (Map.Entry<Integer, Long> byStrategy : byDataSize.getValue().entrySet())
            {
                line += ",";
                long elapsedTime = byStrategy.getValue().longValue();
                line += String.format("%.6f", (double)elapsedTime / 1000000000);
            }
            
            System.out.println(line);
        }
    }
    
    static public void main(String[] args)
    {
        new Test3().perfTest();
    }
}
