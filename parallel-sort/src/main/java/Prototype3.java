import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class Prototype3
{
    static final private int DEFAULT_SEGMENT_MAX_SIZE = 10;
    
    private int segment_max_size;
    private int[] buffer;
    public boolean debug = false;
    
    public Prototype3()
    {
        this.segment_max_size = DEFAULT_SEGMENT_MAX_SIZE;        
    }
    
    public Prototype3(int segment_max_size)
    {
        this.segment_max_size = segment_max_size;
    }
    
    public void setDebug(boolean debug)
    {
        this.debug = debug;
    }
    
    public void psort(int[] arr)
    {
        buffer = new int[arr.length];
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(new Worker(arr, 0, arr.length));
    }
    
    private class Worker extends RecursiveAction
    {
        private static final long serialVersionUID = 2330451448017834196L;
        
        private int[] arr;
        private int offset;
        private int len;
        
        public Worker(int[] arr, int offset, int len)
        {
            this.arr = arr;
            this.offset = offset;
            this.len = len;
        }
    
        public void compute()
        {
            if (debug)
            {
                System.out.println("compute: offset=" + offset + ", len=" + len);
            }

            if (len <= segment_max_size)
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
                
                invokeAll( new Worker(arr, offset1, len1), new Worker(arr, offset2, len2) );
                
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
