#include <tcutil.h>
#include <tcddb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

int main(int argc, char **argv){
    TCDDB *ddb;
    /*  BDBCUR *cur;*/
    int ecode;

    /* create the object */
    ddb = tcddbnew();
    ddb->dimensions = 2;
	ddb->arity = 10;

    /* open the database */
    if(!tcddbopen(ddb, "test.tcb", DDBOWRITER | DDBOCREAT)){
        ecode = tcddbecode(ddb);
        fprintf(stderr, "open error: %s\n", tcddberrmsg(ecode));
        return 0;
    }


    tcddbput2(ddb,"11","x1");
    tcddbput2(ddb,"A1","yA");
    tcddbput2(ddb,"71","z7");
    tcddbput2(ddb,"91","l9");
    tcddbput2(ddb,"31","m3");
    tcddbput2(ddb,"41","n4");


    char *x = tcddbsearch(ddb,"11",2,10);
    printf("%s\n",x);
    x = tcddbsearch(ddb,"A1",2,10);
    printf("%s\n",x);
    x = tcddbsearch(ddb,"71",2,10);
    printf("%s\n",x);
    x = tcddbsearch(ddb,"91",2,10);
    printf("%s\n",x);
    x = tcddbsearch(ddb,"31",2,10);
    printf("%s\n",x);
    x = tcddbsearch(ddb,"41",2,10);
    printf("%s\n",x);
	tcddbclose(ddb);
	return 1;

    printf("##### Similarity search : \n");
    x = tcddbsearch(ddb,"5",1,2);
	if (x!= NULL)
	{
	    printf("%s\n",x);				
	}
	else 
    {
    	printf("NULL\n");	
    }
	
    return 1;
}
