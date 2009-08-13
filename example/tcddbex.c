#include <tcutil.h>
#include <tcdsadb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

int main(int argc, char **argv){
    TCDSADB *dsadb;
    /*  BDBCUR *cur;*/
    int ecode;
    int sp;
    char *x;

    /* create the object */
    dsadb = tcdsadbnew();
    dsadb->dimensions = 2;
	dsadb->arity = 10;

    /* open the database */
    if(!tcdsadbopen(dsadb, "test.tcb", DSADBOWRITER | DSADBOCREAT)){
        ecode = tcdsadbecode(dsadb);
        fprintf(stderr, "open error: %s\n", tcdsadberrmsg(ecode));
        return 0;
    }

    printf("**** INSERT *****\n");

    tcdsadbput2(dsadb,"01","so 0 ne");
    tcdsadbput2(dsadb,"z1","chu a ne");
    tcdsadbput2(dsadb,"K1","z7");
    tcdsadbput2(dsadb,"m1","l9");
    tcdsadbput2(dsadb,"L1","m3");
    tcdsadbput2(dsadb,"u1","u4");
    tcdsadbput2(dsadb,"y1","y4");
    tcdsadbput2(dsadb,"d1","d4");
    tcdsadbput2(dsadb,"i1","i4");
    tcdsadbput2(dsadb,"l1","l4");
    tcdsadbput2(dsadb,"v1","l4");
	printf("OK\n");

/*	printf("**** EXACT SEARCH *****\n");
    x = tcdsadbsearch2(dsadb,"01",10);
    printf("%s\n",x);
	tcdsadbclose(dsadb);
*/

	printf("**** SIMILARITY SEARCH *****\n");
    x = tcdsadbsearch2(dsadb,"h1",3);
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

