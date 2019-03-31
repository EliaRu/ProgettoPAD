package gridsearch;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public abstract class GSMapper<TKeyIn, TValueIn, TValueOut> 
    extends Mapper<TKeyIn, TValueIn, IntWritable, TValueOut> 
{
    //Valore condiviso da tutti i mapper per inviare i risultati
    //allo stesso reducer
    private static final IntWritable magicNumber = new IntWritable( 1 );

    public final void map( TKeyIn key, TValueIn value, Context context )
        throws IOException, InterruptedException
    {
        TValueOut out = launchProgram( key, value, context );
        context.write( magicNumber, out );
    } 

    //Le implementazioni devono sovrascrivere questo metodo per 
    //lanciare il programma che deve essere eseguito sui mapper
    public abstract TValueOut launchProgram( 
        TKeyIn key, TValueIn value, Context context 
    ) throws IOException, InterruptedException;
}
