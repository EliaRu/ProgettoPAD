package testgridsearch;

import gridsearch.GSMapper;
import gridsearch.GSReducer;
import gridsearch.GridSearch;
import gridsearch.InputReaderFeeder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.filecache.DistributedCache;

import java.lang.Runtime;
import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;
import java.net.URI;
import java.util.Scanner;

public class Main {

    public static class GSMapperImpl 
        extends GSMapper<Object, Text, Text>
    {
        private ArrayList<String> cmd;
        private Path[] cachedFiles;
        private Text mapOutput;

        public void setup( Context context )  {
            cmd = new ArrayList<String>( );
            mapOutput = new Text( );
        }

        public Text launchProgram( 
            Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            //Recupera dalla cache distribuita il programma da eseguire
            try {
                cachedFiles = DistributedCache.getLocalCacheFiles( 
                    context.getConfiguration( )
                );
            } catch( IOException e ) {
                //Scrive l'eccezione ottenuta nel log
                System.out.println( e.toString( ) );
                mapOutput.clear( );
                return mapOutput;
            }

            cmd.clear( );
            cmd.add( cachedFiles[0].toString( ) );

            //Prende la combinazione contenente i valori dei parametri
            //e costruisce il comando per eseguire il programma
            //del tipo: <programma> [-[a-zA-Z] <valore>]*
            StringTokenizer tokenizer =
                 new StringTokenizer( value.toString( ) );
            while( tokenizer.hasMoreTokens( ) ) {
                String option = "-" + tokenizer.nextToken( );
                String arg = tokenizer.nextToken( );
                cmd.add( option );
                cmd.add( arg );
            }

            int programOutput = 0;
            try {
                //Avvia il processo con il comando costruito e ne ottiene
                //l'output, infine aspetta che termini
                ProcessBuilder pb = 
                    new ProcessBuilder( cmd );
                pb.redirectOutput( ProcessBuilder.Redirect.PIPE );
                Process p = pb.start( );
                Scanner programStdout = new Scanner( p.getInputStream( ) );
                programOutput = programStdout.nextInt( );
                p.waitFor( );
            } catch( Exception e ) {
                System.out.println( e.toString( ) );
                mapOutput.clear( );
                return mapOutput;
            }

            //Preperara il messaggio in output
            StringBuilder result = new StringBuilder( );
            for( String i : cmd ) {
                result.append( i );
                result.append( " " );
            }
            result.append( '\n' );


            result.append( "Risultato: " );
            result.append( programOutput );
            mapOutput.set( result.toString( ) );
            return mapOutput;
        }
    }

    public static class GSReducerImpl
        extends GSReducer<Text, Text> 
    {
        private Text bestResult;

        public void setup( Context context ) {
            bestResult = new Text( );
        }
        
        public Text chooseBestParameters( 
            Iterable<Text> values, Context context 
        ) throws IOException, InterruptedException
        {
            //Parsa il messaggio ricevuto 
            int min = Integer.MAX_VALUE;
            String[] lines;

            //Parsa il messaggio ricevuto e sceglie il minimo per risultato
            //del programma
            for( Text t : values ) {
                lines = t.toString( ).split( "\n" );
                Integer i = new Integer( lines[1].split( " " )[1] );
                if( i <= min ) {
                    min = i;
                    bestResult.set( t );
                }
            }
            return bestResult;
        }
    }

    public static void main( String args[] ) throws Exception
    {
        Configuration conf = new Configuration( );
        DistributedCache.addCacheFile( 
            new URI( args[0] ), conf
        );
            
        Job job = new Job( conf, "testgridsearch" );
        job.setJarByClass( Main.class );

        job.setOutputValueClass( Text.class );
        job.setMapOutputValueClass( Text.class );

        InputReaderFeeder input = new IRFeederImpl( );
        GridSearch searcher = 
            new GridSearch<GSMapperImpl, GSReducerImpl>(
                input,
                job,
                new Path( args[1] ),
                new Path( args[2] ),
                GSMapperImpl.class,
                GSReducerImpl.class
            );
        System.exit( searcher.run( ) ? 0 : 1 );
    }
}
