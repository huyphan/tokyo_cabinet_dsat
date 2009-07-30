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

	printf("11\n");
    tcddbput2(ddb,"11","x1");
	printf("A1\n");
    tcddbput2(ddb,"A1","yA");
	printf("71\n");
    tcddbput2(ddb,"71","z7");
	printf("91\n");
    tcddbput2(ddb,"91","l9");
    tcddbput2(ddb,"31","m3");
    tcddbput2(ddb,"41","n4");
	printf("OK\n");

    char *x = tcddbsearch2(ddb,"11",10);
	if (x != NULL)
	    printf("%s\n",x);	
	else printf("%s\n","NULL");
    x = tcddbsearch2(ddb,"A1",10);
    printf("%s\n",x);
    x = tcddbsearch2(ddb,"71",10);
    printf("%s\n",x);
    x = tcddbsearch2(ddb,"91",10);
    printf("%s\n",x);
    x = tcddbsearch2(ddb,"31",10);
    printf("%s\n",x);
    x = tcddbsearch2(ddb,"41",10);
    printf("%s\n",x);
	tcddbclose(ddb);

    printf("##### Similarity search : \n");
    x = tcddbsearch2(ddb,"92",1);
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
