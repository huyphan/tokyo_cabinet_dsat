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
  void *mmtx;                            /* mutex for method */
  void *cmtx;                            /* mutex for cache */
  TCHDB *hdb;                            /* internal database object */
  bool open;                             /* whether the internal database is opened */
  bool wmode;                            /* whether to be writable */
  uint32_t arity;                        /* max number of children for each node */
  uint8_t opts;                          /* options */
  uint64_t first;                        /* ID number of the first leaf */
  uint64_t nnum;                         /* number of nodes */
  uint64_t npage;                        /* number of pages */
  TCMAP *leafc;                          /* cache for leaves */
  TCMAP *valuec;                          /* cache for nodes */
  TCMAP *pagec;                          /* cache for pages */
  uint32_t vcnum;                        /* maximum number of cached values */
  uint32_t pcnum;                        /* maximum number of cached pages */
  uint64_t root_offset;                 /* offset of root node in its page */
  uint64_t root_pid;                    /* page ID of root node */
  bool tran;                             /* whether in the transaction */
  char *rbopaque;                        /* opaque for rollback */
  uint64_t clock;                        /* logical clock */
  int64_t cnt_saveleaf;                  /* tesing counter for leaf save times */
  int64_t cnt_loadleaf;                  /* tesing counter for leaf load times */
  int64_t cnt_killleaf;                  /* tesing counter for leaf kill times */
  int64_t cnt_adjleafc;                  /* tesing counter for node cache adjust times */
  int64_t cnt_savenode;                  /* tesing counter for node save times */
  int64_t cnt_loadnode;                  /* tesing counter for node load times */
  int64_t cnt_adjnodec;                  /* tesing counter for node cache adjust times */

  uint8_t dimensions;                    /* number of dimension */
} TCDDB;

enum {                                   /* enumeration for additional flags */
  DDBFOPEN = HDBFOPEN,                   /* whether opened */
  DDBFFATAL = HDBFFATAL                  /* whetehr with fatal error */
};

enum {                                   /* enumeration for tuning options */
  DDBTLARGE = 1 << 0,                    /* use 64-bit bucket array */
  DDBTDEFLATE = 1 << 1,                  /* compress each page with Deflate */
  DDBTBZIP = 1 << 2,                     /* compress each record with BZIP2 */
  DDBTTCBS = 1 << 3,                     /* compress each page with TCBS */
  DDBTEXCODEC = 1 << 4                   /* compress each record with outer functions */
};

enum {                                   /* enumeration for open modes */
  DDBOREADER = 1 << 0,                   /* open as a reader */
  DDBOWRITER = 1 << 1,                   /* open as a writer */
  DDBOCREAT = 1 << 2,                    /* writer creating */
  DDBOTRUNC = 1 << 3,                    /* writer truncating */
  DDBONOLCK = 1 << 4,                    /* open without locking */
  DDBOLCKNB = 1 << 5,                    /* lock without blocking */
  DDBOTSYNC = 1 << 6                     /* synchronize every transaction */
};

/* Get the last happened error code of a B+ tree database object. */
int tcddbecode(TCDDB *ddb);

/* Get the message string corresponding to an error code. */
const char *tcddberrmsg(int ecode);

/* Create a DSA tree database object.
   The return value is the new DSA-tree database object. */
TCDDB *tcddbnew(void);

/* Open a database file and connect a DSA tree database object. */
bool tcddbopen(TCDDB *ddb, const char *path, int omode);

/* Store a record into a DSA tree database object. */
bool tcddbput(TCDDB *ddb, const void *kbuf, int ksiz, const void *vbuf, int vsiz);

/* Store a string record into a DSA tree database object. */
bool tcbdbput2(TCDDB *ddb, const char *kstr, const char *vstr);

/* Search for a record in a DSA tree database object. */
void *tcddbsearch(TCDDB *ddb, const void *kbuf, int ksiz, int64_t r);

void *tcddbsearch2(TCDDB *ddb, const char *kbuf, int64_t r);

/* Close a DSA tree database object. */
bool tcddbclose(TCDDB *ddb);

__TCBDB_CLINKAGEEND
#endif                                   /* duplication check */
/* END OF FILE */
