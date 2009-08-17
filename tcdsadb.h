/*************************************************************************************************
 * The DSA tree database API of Tokyo Cabinet
 *                                                      Copyright (C) 2006-2009 Mikio Hirabayashi
 * This file is part of Tokyo Cabinet.
 * Tokyo Cabinet is free software; you can redistribute it and/or modify it under the terms of
 * the GNU Lesser General Public License as published by the Free Software Foundation; either
 * version 2.1 of the License or any later version.  Tokyo Cabinet is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * You should have received a copy of the GNU Lesser General Public License along with Tokyo
 * Cabinet; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307 USA.
 *************************************************************************************************/


#ifndef _TCDSADB_H                         /* duplication check */
#define _TCDSADB_H

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

typedef unsigned short DSADBDIST;
typedef unsigned char DSADBCORD;

#define TCDSADBDIST(TC_dim, TC_p1, TC_p2,TC_res) \
  do { \
          TC_res = 0;\
          int i = 0;\
          for (; i < TC_dim; ++i) {\
              if (TC_p1[i] > TC_p2[i]) TC_res = TC_res + TC_p1[i] - TC_p2[i];\
              else TC_res = TC_res + TC_p2[i] - TC_p1[i];\
          }\
  } while(false)

typedef struct {                         /* type of structure for a DSA tree database */
  void *mmtx;                            /* mutex for method */
  void *cmtx;                            /* mutex for cache */
  TCHDB *hdb;                            /* internal database object */
  bool open;                             /* whether the internal database is opened */
  bool wmode;                            /* whether to be writable */
  uint32_t arity;                        /* max number of children for each node */
  uint8_t opts;                          /* options */
  uint64_t first;                        /* ID number of the first leaf */
  uint64_t nnode;                         /* number of nodes */
  uint64_t npage;                        /* number of pages */
  TCMAP *leafc;                          /* cache for leaves */
  TCMAP *nodec;                          /* cache for nodes */
  TCMAP *pagec;                          /* cache for pages */
  uint32_t ncnum;                        /* maximum number of cached values */
  uint32_t pcnum;                        /* maximum number of cached pages */
  uint64_t root_offset;                 /* offset of root node in its page */
  uint64_t root_pid;                    /* page ID of root node */
  bool tran;                             /* whether in the transaction */
  char *opaque;                        /* opaque for rollback */
  uint64_t clock;                        /* logical clock */
  int64_t cnt_saveleaf;                  /* tesing counter for leaf save times */
  int64_t cnt_loadleaf;                  /* tesing counter for leaf load times */
  int64_t cnt_killleaf;                  /* tesing counter for leaf kill times */
  int64_t cnt_adjleafc;                  /* tesing counter for node cache adjust times */
  int64_t cnt_savenode;                  /* tesing counter for node save times */
  int64_t cnt_loadnode;                  /* tesing counter for node load times */
  int64_t cnt_adjnodec;                  /* tesing counter for node cache adjust times */
  uint32_t dimensions;                    /* number of dimension */
} TCDSADB;

enum {                                   /* enumeration for additional flags */
  DSADBFOPEN = HDBFOPEN,                   /* whether opened */
  DSADBFFATAL = HDBFFATAL                  /* whetehr with fatal error */
};

enum {                                   /* enumeration for tuning options */
  DSADBTDEFLATE = 1 << 1,                  /* compress each page with Deflate */
  DSADBTBZIP = 1 << 2,                     /* compress each record with BZIP2 */
  DSADBTTCBS = 1 << 3,                     /* compress each page with TCBS */
  DSADBTEXCODEC = 1 << 4                   /* compress each record with outer functions */
};

enum {                                   /* enumeration for open modes */
  DSADBOREADER = 1 << 0,                   /* open as a reader */
  DSADBOWRITER = 1 << 1,                   /* open as a writer */
  DSADBOCREAT = 1 << 2,                    /* writer creating */
  DSADBOTRUNC = 1 << 3,                    /* writer truncating */
  DSADBONOLCK = 1 << 4,                    /* open without locking */
  DSADBOLCKNB = 1 << 5,                    /* lock without blocking */
  DSADBOTSYNC = 1 << 6                     /* synchronize every transaction */
};

/* Get the last happened error code of a B+ tree database object. */
int tcdsadbecode(TCDSADB *dsadb);

/* Get the message string corresponding to an error code. */
const char *tcdsadberrmsg(int ecode);

/* Create a DSA tree database object.
   The return value is the new DSA-tree database object. */
TCDSADB *tcdsadbnew(void);

/* Open a database file and connect a DSA tree database object. */
bool tcdsadbopen(TCDSADB *dsadb, const char *path, int omode);

/* Store a record into a DSA tree database object. */
bool tcdsadbput(TCDSADB *dsadb, const void *kbuf, int ksiz, const void *vbuf, int vsiz);

/* Store a string record into a DSA tree database object. */
bool tcdsadbput2(TCDSADB *dsadb, const char *kstr, const char *vstr);

/* Search for a record in a DSA tree database object. */
void *tcdsadbsearch(TCDSADB *dsadb, const void *kbuf, int ksiz, int64_t r, int *sp);

void *tcdsadbsearch2(TCDSADB *dsadb, const char *kbuf, int64_t r);

/* Close a DSA tree database object. */
bool tcdsadbclose(TCDSADB *dsadb);



/*************************************************************************************************
 * features for experts
 *************************************************************************************************/

/* Retrieve a record in a DSA tree database object. */
void *tcdsadbget(TCDSADB *dsadb, const DSADBCORD *kbuf, uint64_t ksiz, int *sp);

/* Set the file descriptor for debugging output. */
void tcdsadbsetdbgfd(TCDSADB *dsadb, int fd);

/* Get the file descriptor for debugging output. */
int tcdsadbdbgfd(TCDSADB *dsadb);

/* Set mutual exclusion control of a DSA tree database object for threading. */
bool tcdsadbsetmutex(TCDSADB *dsadb);

/* Set the size of the extra mapped memory of a B+ tree database object. */
bool tcdsadbsetxmsiz(TCDSADB *dsadb, int64_t xmsiz);

/* Set the unit step number of auto defragmentation of a B+ tree database object. */
bool tcdsadbsetdfunit(TCDSADB *dsadb, int32_t dfunit);

/* Delete a DSA tree database object. */
void tcdsadbdel(TCDSADB *dsadb);

/* Set the tuning parameters of a DSA tree database object. */
bool tcdsadbtune(TCDSADB *dsadb, int32_t dimnum, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts);

/* Set the caching parameters of a DSA tree database object. */
bool tcdsadbsetcache(TCDSADB *dsadb, int32_t pcnum, int32_t ncnum);

__TCBDB_CLINKAGEEND

#endif                                   /* duplication check */
/* END OF FILE */
