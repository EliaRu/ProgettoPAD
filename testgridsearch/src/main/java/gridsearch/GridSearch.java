package gridsearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileSystem;

import java.lang.Runtime;
import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;

public class GridSearch<T extends GSMapper, S extends GSReducer> {

    private InputReaderFeeder input;
    private Path inputFilename;
    private Path outputFilename;
    private FileSystem fs;
    private Class<T> mapperClass;
    private Class<S> reducerClass;

    private Job job;

    /**
     *@param input          Implementa le operazioni di traduzione tra file
     *                      contenente lo spazio dei parametri e quello che
     *                      contiene l'input dei mapper
     *@param job            Il job Hadoop corrispondente all grid search. 
     *                      Deve essere già configurato con le informazioni 
     *                      relative all'implementazione dell'utente
     *@param inputFilename  Percorso del file contenente lo spazio dei 
     *                      parametri
     *@param outputFilename Percorso del file nel quale verrà scritto 
                            l'output
     *@param mapperClass    Oggetto di tipo Class contenente 
     *                      l'implementazione che estende GSMapper
     *@param reducerClass   Oggetto di tipo Class contenente 
     *                      l'implementazione che estende GSReducer
     */
    public GridSearch( 
        InputReaderFeeder input,
        Job job,
        Path inputFilename,
        Path outputFilename,
        Class<T> mapperClass,
        Class<S> reducerClass
    ) throws IOException
    {
        this.input = input;
        this.job = job;
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.mapperClass = mapperClass;
        this.reducerClass = reducerClass;
        fs = FileSystem.get( job.getConfiguration( ) );
    }

    public final boolean run( ) throws Exception {
        if( input == null || job == null )
        {
            return false;
        } else {
            //Termina la configurazione del job Hadoop aggiungendo le
            //classi fornite dall'utente
            job.setMapperClass( mapperClass );
            job.setReducerClass( reducerClass );
            
            //Quindi aggiunge i tipi utilizzati dalle classi astratte
            //GSMapper e GSReducer
            job.setOutputKeyClass( NullWritable.class );
            job.setMapOutputKeyClass( IntWritable.class );
            
            Path mapperInputFilename = 
                input.translate( job.getConfiguration( ), inputFilename );
            //Termina la configurazione del job con il file intermedio
            //e l'output. setup( ) può essere sovrascritta per impostare
            //il proprio file di output
            setOutputFile( mapperInputFilename );

            boolean result = job.waitForCompletion( true );

            fs.delete( mapperInputFilename );
            return result;
        }
    }
    
    public final boolean setOutputFile( Path tmpPath ) throws IOException 
    {
        if( tmpPath == null || mapperClass == null 
            || reducerClass == null ) 
        {
            return false;
        }
        FileInputFormat.addInputPath( job, tmpPath );
        FileOutputFormat.setOutputPath( job, outputFilename );
        return true;
    }
}
