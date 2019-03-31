package testgridsearch;

import gridsearch.InputReaderFeeder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.File;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class IRFeederImpl implements InputReaderFeeder {

    private class Parameter {
        String name;
        ArrayList<String> values;
    }

    @Override
    public Path translate( 
        Configuration configuration, Path filename 
    ) throws IOException
    {
        FileSystem fs = FileSystem.get( configuration );
        int noParameters = 0;
        int noCombinations = 1;
        ArrayList<Parameter> parameters = new ArrayList<Parameter>( );

        try( 
            BufferedReader inputFile = new BufferedReader( 
                new InputStreamReader( fs.open( filename ) ) 
            );
        )
        {
            //Il formato del file in questione è:
            //<parametro> [<valore> ]*\n
            String line;
            while( ( line = inputFile.readLine( ) ) != null ) {
                StringTokenizer strtok = new StringTokenizer( line, " " );
                Parameter p = new Parameter( );
                noParameters++;
                p.name = strtok.nextToken( );
                p.values = new ArrayList<String>( );
                while( strtok.hasMoreTokens( ) ) {
                    p.values.add( strtok.nextToken( ) );
                }
                noCombinations *= p.values.size( );
                parameters.add( p );
            }
        } 

        ArrayList<ArrayList<String>> combinations = 
            new ArrayList<ArrayList<String>>( noParameters );
        for( int i = 0; i < noParameters; i++ ) {
            combinations.add( new ArrayList<String>( ) );
        }
        int runSize = noCombinations;

        //Vengono generate tutte le possibili combinazioni dei valori
        //dei parametri
        for( int i = 0; i < noParameters; i++ ) {
            runSize = 
                runSize / parameters.get( i ).values.size( );
            int value = 0;
            for( int j = 0; j < noCombinations; ) {
                for( int k = 0; k < runSize; k++ ) {
                    combinations.get( i ).add( 
                        parameters.get( i ).values.get( value ) );      
                    j++;
                }
                value = ( value + 1 ) % parameters.get( i ).values.size( );
            }
        }

        Path outputFilename = new Path( "mapperinput.in" );
        try( PrintWriter outputFile = 
            new PrintWriter( fs.create( outputFilename ) ) ) 
        {
            //Le combinazioni vengono scritte una per riga come:
            //[<parametro> <valore> ]*\n
            for( int i = 0; i < noCombinations; i++ ) {
                for( int j = 0; j < noParameters; j++ ) {
                    outputFile.printf( 
                        "%s %s ", 
                        parameters.get( j ).name,
                        combinations.get( j ).get( i ) 
                    );
                }
                outputFile.printf( "\n" );
            }
        }
        return outputFilename;
    }
}

