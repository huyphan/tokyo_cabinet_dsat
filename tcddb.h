#ifndef _TCDDB_H                         /* duplication check */
#define _TCDDB_H

#if defined(__cplusplus)
#define __TCBDB_CLINKAGEBEGIN extern "C" {
#define __TCBDB_CLINKAGEEND }
#else
#define __TCBDB_CLINKAGEBEGIN
#define __TCBDB_CLINKAGEEND
#endif
__TCBDB_CLINKAGEBEGIN

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <limits.h>
#include <math.h>
#include <tcutil.h>
#include <tchdb.h>

/*************************************************************************************************
 * API
 *************************************************************************************************/


typedef struct {                         /* type of structure for a DSA tree database */
	
} TCDDB;

/* Create a DSA-tree database object.
   The return value is the new DSA-tree database object. */
TCDDB *tcddbnew(void);

__TCBDB_CLINKAGEEND
#endif                                   /* duplication check */
/* END OF FILE */
