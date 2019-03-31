#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

int main( int argc, char *argv[] ) 
{
    char *options = "a:b:c:";
    char *arguments[3];
    int option; 
    while( ( option = getopt( argc, argv, options ) ) != -1 ) {
        switch( option ) {
        case 'a':
            arguments[0] = optarg;
            break;
        case 'b':
            arguments[1] = optarg;
            break;
        case 'c':
            arguments[2] = optarg;
            break;
        default:
            printf( "Error\n" );
            return -1;
        }
    }

    int result = 
        atoi( arguments[0] ) * 
        atoi( arguments[1] ) * 
        atoi( arguments[2] );

    printf( "%d\n", result  );
    
    //printf( "1 %s -a %s -b %s -c %s %f \n", 
        //argv[0], arguments[0], arguments[1], arguments[2], result );
    return 0;
}
