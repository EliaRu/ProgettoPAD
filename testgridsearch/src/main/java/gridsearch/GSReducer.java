package gridsearch;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

//Il framework utilizza un singolo reducer
public abstract class GSReducer<TValueIn, TValueOut>
    extends Reducer<IntWritable, TValueIn, NullWritable, TValueOut>
{
    public final void reduce( 
        IntWritable key, Iterable<TValueIn> values, Context context 
    ) throws IOException, InterruptedException
    {
        TValueOut result = chooseBestParameters( values, context );
        context.write( NullWritable.get( ), result );
    }

    //Le implementazioni devono sovrascrivere questo metodo per scegliere
    //l'esecuzione del programma che ha dato il risultato migliore
    public abstract TValueOut chooseBestParameters( 
        Iterable<TValueIn> list, Context context
    ) throws IOException, InterruptedException;
        
}
