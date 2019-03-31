package gridsearch;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public interface InputReaderFeeder {

    public Path translate( 
        Configuration configuration, Path filename 
    ) throws IOException;
}

