#include "tcutil.h"
#include "tchdb.h"
#include "tcddb.h"
#include "myconf.h"

TCDDB *tcddbnew(void){
  TCDDB *ddb;
  TCMALLOC(ddb, sizeof(*ddb));
  return ddb;
}

/* END OF FILE */
