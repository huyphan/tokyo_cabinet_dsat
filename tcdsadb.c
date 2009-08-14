#include "tcutil.h"
#include "tchdb.h"
#include "tcdsadb.h"
#include "myconf.h"
#include "time.h"

#define DSADBPAGEBUFSIZ       ((1LL<<15)+1)     /* size of a buffer to read each page */
#define DSADBNODEIDBASE       ((1LL<<63)+1)
#define DSADBPAGEIDBASE       1
#define DSADBDEFNCNUM         100               /* default number of node cache */
#define DSADBDEFPCNUM         2048              /* default number of page cache */
#define DSADBMAXDISKPAGESIZE  (1LL<<15)         /* maximum size of disk page (1LL<<12) */
#define DSADBMAXNODECOUNT     (1LL<<15)         /* maximum size of disk page */
#define DSADBMAXNODECACHE     512               /* maximum number or node to be cached */
#define DSADBMAXPAGECACHE     512               /* maximum number or page to be cached */
#define DSADBINVPAGEID       -1                 /* invalid page id */
#define DSADBINVOFFSETID     -1                 /* invalid offset id */
#define DSADBMAXDIST          (300LL*81)        /* specific to Image Mark case */
#define DSADBMAXDIMENSION     2000              /* max dimenstion of one node */
#define DSADBCACHEOUT         8                 /* number of pages in a process of cacheout  */

#define DSDDBDEFARITY         10                /* default number of maxarity */

/* Tuning param for Hash DB */
#define DSADBDEFBNUM          32749             /* default bucket number */
#define DSADBDEFAPOW          8                 /* default alignment power */
#define DSADBDEFFPOW          10                /* default free block pool power */

typedef struct {
    uint64_t pid;
    uint64_t offset;
} DSADBFPTR; /* Far pointer */

typedef struct {
    uint64_t offset;
} DSADBLPTR; /* Local page pointer */

typedef struct {
    uint64_t time;
    /*  uint64_t id; */
    DSADBFPTR child;
    DSADBLPTR sibling;
    DSADBDIST radius;
    DSADBCORD *point;
    uint32_t depth;
} DSADBNODE; /* DSAT node */

typedef struct {
    uint64_t id;
    bool dirty;
    uint32_t subtree_with_diff_parent_count;
    uint64_t size;
    TCPTRLIST *nodes;
} DSADBPAGE; /* Page structure */

enum { /* enumeration for duplication behavior */
    DSADBPDOVER, /* overwrite an existing value */
    DSADBPDKEEP, /* keep the existing value */
    DSADBPDCAT, /* concatenate values */
    DSADBPDDUP, /* allow duplication of keys */
    DSADBPDDUPB, /* allow backward duplication */
    DSADBPDADDINT, /* add an integer */
    DSADBPDADDSADBL, /* add a real number */
    DSADBPDPROC
/* process by a callback function */
};

#define DSADBLOCKMETHOD(TC_dsadb, TC_wr) \
  ((TC_dsadb)->mmtx ? tcdsadblockmethod((TC_dsadb), (TC_wr)) : true)

#define DSADBUNLOCKMETHOD(TC_dsadb) \
  ((TC_dsadb)->mmtx ? tcdsadbunlockmethod(TC_dsadb) : true)

#define DSADBLOCKCACHE(TC_dsadb) \
  ((TC_dsadb)->mmtx ? tcdsadblockcache(TC_dsadb) : true)
#define DSADBUNLOCKCACHE(TC_dsadb) \
  ((TC_dsadb)->mmtx ? tcdsadbunlockcache(TC_dsadb) : true)

static DSADBDIST dist(uint8_t dimensions, DSADBCORD *point1, DSADBCORD *point2);
void tcdsadbsetdbgfd(TCDSADB *dsadb, int fd);
int tcdsadbdbgfd(TCDSADB *dsadb);
void tcdsadbsetecode(TCDSADB *dsadb, int ecode, const char *filename, int line, const char *func);
static bool tcdsadblockmethod(TCDSADB *dsadb, bool wr);
static bool tcdsadbunlockmethod(TCDSADB *dsadb);
static bool tcdsadblockcache(TCDSADB *dsadb);
static bool tcdsadbunlockcache(TCDSADB *dsadb);
bool tcdsadbsetmutex(TCDSADB *dsadb);
bool tcdsadbsetxmsiz(TCDSADB *dsadb, int64_t xmsiz);
bool tcdsadbsetdfunit(TCDSADB *dsadb, int32_t dfunit);
bool tcdsadbmemsync(TCDSADB *dsadb, bool phys);
bool tcdsadbsync(TCDSADB *dsadb);
static void tcdsadbdumpmeta(TCDSADB *dsadb);
bool tcdsadbtune(TCDSADB *dsadb, int32_t dimnum, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts);
bool tcdsadbsetcache(TCDSADB *dsadb, int32_t pcnum, int32_t ncnum);
static bool tcdsadbpagecacheout(TCDSADB *dsadb, DSADBPAGE *page);
static bool tcdsadbcacheadjust(TCDSADB *dsadb);
static DSADBNODE *tcdsadbnodeload(DSADBPAGE *page, int index);
static DSADBNODE *tcdsadbnodenew(TCDSADB *dsadb,DSADBCORD* point);
static uint32_t tcdsadbnodesize(TCDSADB *dsadb,DSADBNODE *node, uint32_t offset);
static DSADBPAGE *tcdsadbpagenew(TCDSADB *dsadb);
static bool tcdsadbpageremove(TCDSADB *dsadb, DSADBPAGE *page);
static bool tcdsadbpagesave(TCDSADB *dsadb, DSADBPAGE *page);
static DSADBPAGE *tcdsadbpageload(TCDSADB *dsadb, uint64_t id);
void *tcdsadbget(TCDSADB *dsadb, const DSADBCORD *kbuf, uint64_t ksiz, int *sp);
static const DSADBNODE *tcdsadbrangesearch(TCDSADB *dsadb, DSADBNODE *elem,
        const void *kbuf, int64_t ksiz, int64_t r, time_t t);
static const DSADBNODE *tcdsadbsearchimpl(TCDSADB *dsadb, const DSADBCORD *kbuf,
        int64_t ksiz, int64_t r, int *sp);
static int tcdsadbinsertnode(DSADBPAGE *page,DSADBNODE *node);
static bool tcdsadbputimpl(TCDSADB *dsadb, const void *kbuf, int ksiz,
        const void *vbuf, int vsiz, int dmode);
static bool tcdsadbopenimpl(TCDSADB *dsadb, const char *path, int omode);
static void tcdsadbclear(TCDSADB *dsadb);
static bool tcdsadbcloseimpl(TCDSADB *dsadb);
void tcdsadbdel(TCDSADB *dsadb);

/*************************************************************************************************
 * UTIL FUNTIONS
 ************************************************************************************************/

static DSADBDIST dist(uint8_t dimensions, DSADBCORD *point1, DSADBCORD *point2) {
    DSADBDIST tot = 0;
    int i = 0;

    for (; i < dimensions; ++i) {
        if (point1[i] > point2[i])
            tot += point1[i] - point2[i];
        else
            tot += point2[i] - point1[i];
    }

    return tot;
}

/* Set the file descriptor for debugging output. */
void tcdsadbsetdbgfd(TCDSADB *dsadb, int fd){
  assert(dsadb && fd >= 0);
  tchdbsetdbgfd(dsadb->hdb, fd);
}


/* Get the file descriptor for debugging output. */
int tcdsadbdbgfd(TCDSADB *dsadb){
  assert(dsadb);
  return tchdbdbgfd(dsadb->hdb);
}

/* Set the error code of a DSA tree database object. */
void tcdsadbsetecode(TCDSADB *dsadb, int ecode, const char *filename, int line,
        const char *func) {
    assert(dsadb && filename && line >= 1 && func);
    tchdbsetecode(dsadb->hdb, ecode, filename, line, func);
}

/* Lock a method of the DSA tree database object.
 `dsadb' specifies the DSA tree database object.
 `wr' specifies whether the lock is writer or not.
 If successful, the return value is true, else, it is false. */
static bool tcdsadblockmethod(TCDSADB *dsadb, bool wr) {
    assert(dsadb);
    if (wr ? pthread_rwlock_wrlock(dsadb->mmtx) != 0 : pthread_rwlock_rdlock(
            dsadb->mmtx) != 0) {
        tcdsadbsetecode(dsadb, TCETHREAD, __FILE__, __LINE__, __func__);
        return false;
    }

    TCTESTYIELD();
    return true;
}

/* Unlock a method of the DSA tree database object.
 `dsadb' specifies the DSA tree database object.
 If successful, the return value is true, else, it is false. */
static bool tcdsadbunlockmethod(TCDSADB *dsadb) {
    assert(dsadb);
    if (pthread_rwlock_unlock(dsadb->mmtx) != 0) {
        tcdsadbsetecode(dsadb, TCETHREAD, __FILE__, __LINE__, __func__);
        return false;
    }
    TCTESTYIELD();
    return true;
}

/* Lock the cache of the DSA tree database object.
 `dsadb' specifies the DSA tree database object.
 If successful, the return value is true, else, it is false. */
static bool tcdsadblockcache(TCDSADB *dsadb) {
    assert(dsadb);
    if (pthread_mutex_lock(dsadb->cmtx) != 0) {
        tcdsadbsetecode(dsadb, TCETHREAD, __FILE__, __LINE__, __func__);
        return false;
    }
    TCTESTYIELD();
    return true;
}

/* Unlock the cache of the DSA tree database object.
 `dsadb' specifies the DSA tree database object.
 If successful, the return value is true, else, it is false. */
static bool tcdsadbunlockcache(TCDSADB *dsadb) {
    assert(dsadb);
    if (pthread_mutex_unlock(dsadb->cmtx) != 0) {
        tcdsadbsetecode(dsadb, TCETHREAD, __FILE__, __LINE__, __func__);
        return false;
    }
    TCTESTYIELD();
    return true;
}

/* Set mutual exclusion control of a DSA tree database object for threading. */
bool tcdsadbsetmutex(TCDSADB *dsadb){
  assert(dsadb);
  if(!TCUSEPTHREAD) return true;

  if(dsadb->mmtx || dsadb->open){
    tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  TCMALLOC(dsadb->mmtx, sizeof(pthread_rwlock_t));
  TCMALLOC(dsadb->cmtx, sizeof(pthread_mutex_t));
  bool err = false;
  if(pthread_rwlock_init(dsadb->mmtx, NULL) != 0) err = true;
  if(pthread_mutex_init(dsadb->cmtx, NULL) != 0) err = true;
  if(err) {
    TCFREE(dsadb->cmtx);
    TCFREE(dsadb->mmtx);
    dsadb->cmtx = NULL;
    dsadb->mmtx = NULL;
    return false;
  }

  return tchdbsetmutex(dsadb->hdb);
}


/* Set the size of the extra mapped memory of a DSA tree database object. */
bool tcdsadbsetxmsiz(TCDSADB *dsadb, int64_t xmsiz){
  assert(dsadb);
  if(dsadb->open){
    tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  return tchdbsetxmsiz(dsadb->hdb, xmsiz);
}

/* Set the unit step number of auto defragmentation of a DSA tree database object. */
bool tcdsadbsetdfunit(TCDSADB *dsadb, int32_t dfunit){
  assert(dsadb);
  if(dsadb->open){
    tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  return tchdbsetdfunit(dsadb->hdb, dfunit);
}

/* Synchronize updated contents of a DSA tree database object with the file and the device. */
bool tcdsadbsync(TCDSADB *dsadb)
{
  assert(dsadb);
  if(!DSADBLOCKMETHOD(dsadb, true)) return false;
  if(!dsadb->open || !dsadb->wmode || dsadb->tran){
    tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
    DSADBUNLOCKMETHOD(dsadb);
    return false;
  }
  bool rv = tcdsadbmemsync(dsadb, true);
  DSADBUNLOCKMETHOD(dsadb);
  return rv;
}


/* Synchronize updating contents on memory of a B+ tree database object. */
bool tcdsadbmemsync(TCDSADB *dsadb, bool phys){
  assert(dsadb);
  if(!dsadb->open || !dsadb->wmode){
    tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  bool err = false;
  tcdsadbdumpmeta(dsadb);
  if(!tchdbmemsync(dsadb->hdb, phys)) err = true;
  return !err;
}

/* Serialize meta data into the opaque field.
   `dsadb' specifies the DSA tree database object. */
static void tcdsadbdumpmeta(TCDSADB *dsadb){
  assert(dsadb);
  memset(dsadb->opaque, 0, 64);
  char *wp = dsadb->opaque;

  uint32_t lnum;
  uint64_t llnum;

  llnum = dsadb->nnode;
  llnum = TCHTOILL(llnum);
  memcpy(wp, &llnum, sizeof(llnum));
  wp += sizeof(llnum);

  llnum = dsadb->npage;
  llnum = TCHTOILL(llnum);
  memcpy(wp, &llnum, sizeof(llnum));
  wp += sizeof(llnum);

  lnum = dsadb->arity;
  lnum = TCHTOILL(lnum);
  memcpy(wp, &lnum, sizeof(lnum));
  wp += sizeof(lnum);

  lnum = dsadb->dimensions;
  lnum = TCHTOILL(lnum);
  memcpy(wp, &lnum, sizeof(lnum));
  wp += sizeof(lnum);

  lnum = dsadb->ncnum;
  lnum = TCHTOILL(lnum);
  memcpy(wp, &lnum, sizeof(lnum));
  wp += sizeof(lnum);

  lnum = dsadb->pcnum;
  lnum = TCHTOILL(lnum);
  memcpy(wp, &lnum, sizeof(lnum));
  wp += sizeof(lnum);

  llnum = dsadb->root_offset;
  llnum = TCHTOILL(llnum);
  memcpy(wp, &llnum, sizeof(llnum));
  wp += sizeof(llnum);

  llnum = dsadb->root_pid;
  llnum = TCHTOILL(llnum);
  memcpy(wp, &llnum, sizeof(llnum));
  wp += sizeof(llnum);
}

/* Deserialize meta data from the opaque field.
   `dsadb' specifies the DSA tree database object. */
static void tcdsadbloadmeta(TCDSADB *dsadb){
  const char *rp = dsadb->opaque;

  uint32_t lnum;
  uint64_t llnum;

  memcpy(&llnum, rp, sizeof(llnum));
  dsadb->nnode = TCITOHLL(llnum);
  rp += sizeof(llnum);

  memcpy(&llnum, rp, sizeof(llnum));
  dsadb->npage = TCITOHLL(llnum);
  rp += sizeof(llnum);


  memcpy(&lnum, rp, sizeof(lnum));
  dsadb->arity = TCITOHL(lnum);
  rp += sizeof(lnum);

  memcpy(&lnum, rp, sizeof(lnum));
  dsadb->dimensions = TCITOHL(lnum);
  rp += sizeof(lnum);

  memcpy(&lnum, rp, sizeof(lnum));
  dsadb->ncnum = TCITOHL(lnum);
  rp += sizeof(lnum);

  memcpy(&lnum, rp, sizeof(lnum));
  dsadb->pcnum = TCITOHL(lnum);
  rp += sizeof(lnum);

  memcpy(&llnum, rp, sizeof(llnum));
  dsadb->root_offset = TCITOHLL(llnum);
  rp += sizeof(llnum);

  memcpy(&llnum, rp, sizeof(llnum));
  dsadb->root_pid = TCITOHLL(llnum);
  rp += sizeof(llnum);

}

/* Set the tuning parameters of a DSA tree database object. */
bool tcdsadbtune(TCDSADB *dsadb, int32_t dimnum, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts){
  assert(dsadb);
  if(dsadb->open){
    tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  dsadb->dimensions = (dimnum > 0) ? tclmin(dimnum, DSADBMAXDIMENSION) : DSADBMAXDIMENSION;
  dsadb->opts = opts;
  uint8_t hopts = 0;
  if(opts & DSADBTDEFLATE) hopts |= HDBTDEFLATE;
  if(opts & DSADBTBZIP) hopts |= HDBTBZIP;
  if(opts & DSADBTTCBS) hopts |= HDBTTCBS;
  if(opts & DSADBTEXCODEC) hopts |= HDBTEXCODEC;
  bnum = (bnum > 0) ? bnum : DSADBDEFBNUM;
  apow = (apow >= 0) ? apow : DSADBDEFAPOW;
  fpow = (fpow >= 0) ? fpow : DSADBDEFFPOW;
  return tchdbtune(dsadb->hdb, bnum, apow, fpow, hopts);
}

/* Set the caching parameters of a DSA tree database object. */
bool tcdsadbsetcache(TCDSADB *dsadb, int32_t pcnum, int32_t ncnum){
  assert(dsadb);
  if(dsadb->open){
    tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  if(pcnum > 0) dsadb->pcnum = tclmax(pcnum, DSADBMAXPAGECACHE);
  if(ncnum > 0) dsadb->ncnum = tclmax(ncnum, DSADBMAXNODECACHE);
  return true;
}

static bool tcdsadbpagecacheout(TCDSADB *dsadb, DSADBPAGE *page)
{
  assert(dsadb && page);
  bool err = false;
  if(!tcdsadbpagesave(dsadb,page)) err = true;

  TCPTRLIST *nodes = page->nodes;
  int ln = TCPTRLISTNUM(nodes);
  for(int i = 0; i < ln; i++){
    DSADBNODE *node = TCPTRLISTVAL(nodes, i);
    if (node)
    {
        if (node->point) TCFREE(node->point);
        TCFREE(node);
    }
  }

  tcptrlistdel(nodes);

  tcmapout(dsadb->pagec, &(page->id), sizeof(page->id));
  return !err;
}

static bool tcdsadbnodecacheout(TCDSADB *dsadb,const void *kbuf, int ksiz)
{
  assert(dsadb && kbuf && ksiz > 0);
  bool err = false;
  tcmapout(dsadb->nodec, kbuf, ksiz);
  return !err;
}

static bool tcdsadbcacheadjust(TCDSADB *dsadb){
  bool err = false;

  if(TCMAPRNUM(dsadb->pagec) > dsadb->pcnum)
  {
    int ecode = tchdbecode(dsadb->hdb);
    bool clk = DSADBLOCKCACHE(dsadb);
    TCMAP *pagec= dsadb->pagec;
    tcmapiterinit(pagec);
    int dnum = tclmin(tclmax(TCMAPRNUM(pagec) - dsadb->pcnum, DSADBCACHEOUT),dsadb->pcnum);


    for(int i = 0; i < dnum; i++){
      int rsiz;
      void* x = (void*) tcmapiternext(pagec, &rsiz);
      DSADBPAGE* temp = (DSADBPAGE *)tcmapiterval(x, &rsiz);

      if(!tcdsadbpagecacheout(dsadb,temp))
        err = true;
    }

    if(clk) DSADBUNLOCKCACHE(dsadb);
    if(!err && tchdbecode(dsadb->hdb) != ecode)
      tcdsadbsetecode(dsadb, ecode, __FILE__, __LINE__, __func__);
  }

  if(TCMAPRNUM(dsadb->nodec) > dsadb->ncnum){
    int ecode = tchdbecode(dsadb->hdb);
    bool clk = DSADBLOCKCACHE(dsadb);
    TCMAP *nodec = dsadb->nodec;
    tcmapiterinit(nodec);

    int dnum = tclmin(tclmax(TCMAPRNUM(dsadb->nodec) - dsadb->ncnum, DSADBCACHEOUT),dsadb->ncnum);

    for(int i = 0; i < dnum; i++){
      int rsiz;
      void *kbuf = (void*) tcmapiternext(nodec, &rsiz);
      if(!tcdsadbnodecacheout(dsadb, kbuf, rsiz ))
        err = true;
    }

    if(clk) DSADBUNLOCKCACHE(dsadb);

    if(!err && tchdbecode(dsadb->hdb) != ecode)
      tcdsadbsetecode(dsadb, ecode, __FILE__, __LINE__, __func__);
  }
  return !err;
}

/* Load a node from page
 `page' specifies the page contains nodes.
 `index' specifies the index of node in list
 The return value is the node object or `NULL' on failure.
 */
static DSADBNODE *tcdsadbnodeload(DSADBPAGE *page, int index) {
    assert(page);
    if (index < 0) {
        return NULL;
    }
    return TCPTRLISTVAL(page->nodes, index);
}

/* Create a new node.
 `dsadb' specifies the DSA tree database object.
 The return value is the new node object. */

static DSADBNODE *tcdsadbnodenew(TCDSADB *dsadb,DSADBCORD* point) {
    assert(dsadb);
    DSADBNODE *node;
    TCMALLOC(node,sizeof(DSADBNODE));
    node->radius = 0;
    node->child.offset = DSADBINVOFFSETID;
    node->child.pid = DSADBINVPAGEID;
    node->sibling.offset = DSADBINVOFFSETID;
    node->depth = 0;
    TCMALLOC(node->point,dsadb->dimensions*sizeof(DSADBCORD));

    for (int i=0;i<dsadb->dimensions;i++)
    {
        node->point[i] = point[i];
    }

    node->time = time(NULL);

    return node;
}

static uint32_t tcdsadbnodesize(TCDSADB *dsadb,DSADBNODE *node, uint32_t offset)
{
    uint32_t size = 0;

    int64_t llnum;
    uint64_t ullnum;
    uint32_t ulnum;

    if (node == NULL) return 0;

    /* offset of node */
    size += TCCALCVNUMSIZE(offset);

    ullnum = node->time;
    size += TCCALCVNUMSIZE(ullnum);

    ulnum = node->radius + 1;
    size += TCCALCVNUMSIZE(ulnum);

    llnum = node->sibling.offset + 1;
    size += TCCALCVNUMSIZE(llnum);

    llnum = node->child.pid + 1;
    size += TCCALCVNUMSIZE(llnum);

    llnum = node->child.offset + 1;
    size += TCCALCVNUMSIZE(llnum);

    for (int j = 0; j < dsadb->dimensions; j++) {
        ulnum = node->point[j] + 1;
        size += TCCALCVNUMSIZE(ulnum);
    }

    return size;
}

/* Create a new page.
 `dsadb' specifies the DSA tree database object.
 The return value is the new page object. */

static DSADBPAGE *tcdsadbpagenew(TCDSADB *dsadb) {
    assert(dsadb);
    DSADBPAGE page;
    page.id = ++dsadb->npage + DSADBPAGEIDBASE;
    page.nodes = tcptrlistnew();
    page.size = 0;
    page.subtree_with_diff_parent_count = 1;
    page.dirty = true;

    tcmapputkeep(dsadb->pagec, &(page.id), sizeof(page.id), &page, sizeof(page));
    int rsiz;
//    printf("PAGE IN NEW %llu\n",page.id);
    return (DSADBPAGE *)tcmapget(dsadb->pagec, &(page.id), sizeof(page.id), &rsiz);
}

/* Remove a page
 `dsadb' specifies the DSA tree database object.
 `page' specifies the page object.
 If successful, the return value is true, else, it is false. */

static bool tcdsadbpageremove(TCDSADB *dsadb, DSADBPAGE *page)
{
    assert(dsadb && page);

    char static_buf[(sizeof(uint64_t) + 1) * 3];

    bool err = false;
    int step = sprintf(static_buf, "%llx", (unsigned long long) page->id);
    if (!tchdbout(dsadb->hdb, static_buf, step) && tchdbecode(
            dsadb->hdb) != TCENOREC)
        err = true;

    bool clk = DSADBLOCKCACHE(dsadb);
    tcmapout(dsadb->pagec,&page->id, sizeof(page->id));
    if (clk)
        DSADBUNLOCKCACHE(dsadb);

    return err;
}

/* Save a page into the internal database.
 `dsadb' specifies the DSA tree database object.
 `page' specifies the page object.
 If successful, the return value is true, else, it is false. */
static bool tcdsadbpagesave(TCDSADB *dsadb, DSADBPAGE *page) {
    assert(dsadb && page);
    TCXSTR *rbuf = tcxstrnew3(DSADBPAGEBUFSIZ);
    char static_buf[(sizeof(uint64_t) + 1) * 3];
    char *dynamic_buf;
    char *wp = static_buf;

    int step;

    uint64_t ullnum;
    uint32_t ulnum;

    int64_t size = tcptrlistnum(page->nodes);

    /* Number of nodes in this page */
    ulnum = size;
    TCSETVNUMBUF(step, wp, ulnum);
    TCXSTRCAT(rbuf, static_buf, step);
    /* number of subtree with different parents */
    ulnum = page->subtree_with_diff_parent_count;

    TCSETVNUMBUF(step, wp, ulnum);

    TCXSTRCAT(rbuf, static_buf, step);

    TCMALLOC(dynamic_buf,1 + size * ( 5 + dsadb->dimensions )* sizeof(uint64_t));
    DSADBNODE *node;
    for (int i = 0; i < size; i++)
    {
        node = tcdsadbnodeload(page,i);

        if (node == NULL) continue;

        wp = dynamic_buf;

        /* offset of node */
        ulnum = i;
        TCSETVNUMBUF(step, wp, ulnum);
        wp += step;

        ullnum = node->time;
        TCSETVNUMBUF64(step, wp, ullnum);
        wp += step;

        ulnum = node->radius + 1;
        TCSETVNUMBUF(step, wp, ulnum);
        wp += step;

        ullnum = node->sibling.offset + 1;
        TCSETVNUMBUF64(step, wp, ullnum);
        wp += step;

        ullnum = node->child.pid + 1;
        TCSETVNUMBUF64(step, wp, ullnum);
        wp += step;

        ullnum = node->child.offset + 1;
        TCSETVNUMBUF64(step, wp, ullnum);
        wp += step;

        for (int j = 0; j < dsadb->dimensions; j++) {
            ulnum = node->point[j] + 1;
            TCSETVNUMBUF(step, wp, ulnum);
            wp += step;
        }

        TCXSTRCAT(rbuf, dynamic_buf, wp - dynamic_buf);
    }
    TCFREE(dynamic_buf);
    bool err = false;
    step = sprintf(static_buf, "%llx", (unsigned long long) page->id);
    if (size < 1 && !tchdbout(dsadb->hdb, static_buf, step) && tchdbecode(
            dsadb->hdb) != TCENOREC)
        err = true;

    page->size = TCXSTRSIZE(rbuf);

    if (!tchdbput(dsadb->hdb, static_buf, step, TCXSTRPTR(rbuf), TCXSTRSIZE(rbuf)))
        err = true;
    tcxstrdel(rbuf);

    return !err;
}

/* Load a page from the internal database.
 `dsadb' specifies the DSA tree database object.
 `id' specifies the ID number of the page.
 The return value is the node object or `NULL' on failure.
 TODO: refactor this function, it's similar with tcdsadbget   */
static DSADBPAGE *tcdsadbpageload(TCDSADB *dsadb, uint64_t id) {

    assert(dsadb && id > DSADBPAGEIDBASE);
    bool clk = DSADBLOCKCACHE(dsadb);
    int rsiz;

    // Get the page from cache if exists :
    DSADBPAGE *p = (DSADBPAGE *) tcmapget3(dsadb->pagec, &id, sizeof(id), &rsiz);
    if (p)
    {
        if (clk)
            DSADBUNLOCKCACHE(dsadb);
        return p;
    }

    if (clk)
        DSADBUNLOCKCACHE(dsadb);
    //TCDODEBUG(dsadb->cnt_loadnode++);

    // Not available in cache :
    char hbuf[(sizeof(uint64_t) + 1) * 2];
    int step;

    // Get the order of this node in list
    step = sprintf(hbuf, "%llx", (unsigned long long) (id));
    char *rbuf = NULL;
    char wbuf[DSADBPAGEBUFSIZ];
    const char *rp = NULL;
    rsiz = tchdbget3(dsadb->hdb, hbuf, step, wbuf, DSADBPAGEBUFSIZ);

    if (rsiz < 1)
    { // If getting failed
        tcdsadbsetecode(dsadb, TCEMISC, __FILE__, __LINE__, __func__);
        return NULL;
    } else if (rsiz < DSADBPAGEBUFSIZ)
    { // Buffer size is big enough for the record
        rp = wbuf;
    } else { // The actual record size is larger than buffer size
        if (!(rbuf = tchdbget(dsadb->hdb, hbuf, step, &rsiz))) {
            tcdsadbsetecode(dsadb, TCEMISC, __FILE__, __LINE__, __func__);
            return NULL;
        }
        rp = rbuf;
    }

    DSADBPAGE page;

    page.nodes = tcptrlistnew();
    page.size = rsiz;

    int64_t llnum;
    uint64_t ullnum;
    uint32_t ulnum;
    int cur_index;
    int64_t size;

    page.id = id;

    /* get number of nodes */
    TCREADVNUMBUF(rp, llnum, step);
    size = llnum;
    rp += step;
    rsiz -= step;

    /* number of subtrees with different parents */
    TCREADVNUMBUF(rp, ulnum, step);
    page.subtree_with_diff_parent_count = ulnum;
    rp += step;
    rsiz -= step;
    bool err = false;

    while (rsiz > 0)
    {
        TCREADVNUMBUF(rp, ulnum, step);
        cur_index = ulnum;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF64(rp, ullnum, step);
        uint64_t time = ullnum - 1;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF(rp, ulnum, step);
        DSADBDIST radius = ulnum - 1;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF64(rp, ullnum, step);
        int64_t sibling_offset = ullnum - 1;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF64(rp, ullnum, step);
        int64_t child_pid = ullnum - 1;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF64(rp, ullnum, step);
        int64_t child_offset = ullnum - 1;
        rp += step;
        rsiz -= step;

        DSADBCORD* point;
        TCMALLOC(point,dsadb->dimensions*sizeof(DSADBCORD));

        for (int j = 0; j < dsadb->dimensions; j++) {
            TCREADVNUMBUF(rp, ulnum, step);
            point[j] = ulnum - 1;
            rp += step;
            rsiz -= step;
        }

        DSADBNODE *node = tcdsadbnodenew(dsadb,point);
        node->time = time;
        node->radius = radius;
        node->sibling.offset = sibling_offset;
        node->child.offset = child_offset;
        node->child.pid = child_pid;

        while (tcptrlistnum(page.nodes) < cur_index)
        {
            tcptrlistpush(page.nodes, NULL);
        }
        tcptrlistpush(page.nodes, node);
        TCFREE(point);
    }

    while (tcptrlistnum(page.nodes) < page.size)
    {
        tcptrlistpush(page.nodes, NULL);
    }

    if (err || rsiz != 0) {
        tcdsadbsetecode(dsadb, TCEMISC, __FILE__, __LINE__, __func__);
        return NULL;
    }

//    printf("PAGE IN %llu\n",id);
    clk = DSADBLOCKCACHE(dsadb);
    tcmapput(dsadb->pagec, &id, sizeof(id), &page, sizeof(page));
    p = (DSADBPAGE *) tcmapget(dsadb->pagec, &id, sizeof(id), &rsiz);
    if (clk)
        DSADBUNLOCKCACHE(dsadb);
    return p;
}

/* Load a node from the internal database.
 `dsadb' specifies the DSA tree database object.
 `id' specifies the ID number of the node.
 The return value is the node object or `NULL' on failure.
 */
void *tcdsadbget(TCDSADB *dsadb, const DSADBCORD *kbuf, uint64_t ksiz, int *sp) {
    assert(dsadb && id > DSADBNODEIDBASE);

    if(!dsadb->open){
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        DSADBUNLOCKMETHOD(dsadb);
        return NULL;
    }

    bool clk = DSADBLOCKCACHE(dsadb);

    int rsiz;
    // Get the node from cache if exists :

    const void *value = tcmapget3(dsadb->nodec, kbuf, ksiz, &rsiz);

    if (!value)
    {
        if (clk)
            DSADBUNLOCKCACHE(dsadb);
        //TCDODEBUG(dsadb->cnt_loadnode++);

        // Not available in cache :

        char *rbuf = NULL;
        char wbuf[DSADBPAGEBUFSIZ];
        const char *rp = NULL;

        // Get record and write to buffer ( wbuf )
        rsiz = tchdbget3(dsadb->hdb, kbuf, ksiz, wbuf, DSADBPAGEBUFSIZ);

        if (rsiz < 1) { // If getting failed
            tcdsadbsetecode(dsadb, TCEMISC, __FILE__, __LINE__, __func__);
            return NULL;
        } else if (rsiz < DSADBPAGEBUFSIZ) { // Buffer size is big enough for the record
            rp = wbuf;
        } else { // The actual record size is larger than buffer size
            if (!(rbuf = tchdbget(dsadb->hdb, kbuf, ksiz, &rsiz))) {
                tcdsadbsetecode(dsadb, TCEMISC, __FILE__, __LINE__, __func__);
                return NULL;
            }
            rp = rbuf;
        }

        clk = DSADBLOCKCACHE(dsadb);

        tcmapputkeep(dsadb->nodec, kbuf, ksiz, rp, rsiz);
        value = (void*) tcmapget(dsadb->nodec, kbuf, ksiz, &rsiz);
    }

    memcpy(sp,&rsiz,sizeof(rsiz));

    char *rv;
    if(value){
        TCMEMDUP(rv, value, rsiz);
    } else {
        rv = NULL;
    }

    if (clk)
        DSADBUNLOCKCACHE(dsadb);

    bool adj = TCMAPRNUM(dsadb->pagec) > dsadb->pcnum || TCMAPRNUM(dsadb->nodec) > dsadb->ncnum;

    if(adj && DSADBLOCKMETHOD(dsadb, true)){
      tcdsadbcacheadjust(dsadb);
      DSADBUNLOCKMETHOD(dsadb);
    }

    return rv;
}

static const DSADBNODE *tcdsadbrangesearch(TCDSADB *dsadb, DSADBNODE *elem,
        const void *kbuf, int64_t ksiz, int64_t r, time_t t) {
    DSADBDIST dp, min_dist;
    DSADBNODE *sibling;
    int32_t child_offset, sibling_offset;
    time_t t1;

    dp = dist(dsadb->dimensions, (DSADBCORD*) kbuf, elem->point);

    if ((elem->time <= t) && (dp <= elem->radius + r))
    {
        if (dp <= r) {
           /*  return tcdsadbget(dsadb, elem->point, ksiz, &sp); */
           return elem;
        }

        min_dist = DSADBMAXDIST;
        DSADBPAGE *page = tcdsadbpageload(dsadb, elem->child.pid);
        child_offset = elem->child.offset;

        while (child_offset != DSADBINVOFFSETID)
        {
            DSADBNODE *node = tcdsadbnodeload(page, child_offset);
//            printf("NODE %d %d\n",node->point[0],node->point[1]);
            /* TODO: Avoid calling dist function by using dist array */
            dp = dist(dsadb->dimensions, (DSADBCORD*) kbuf, node->point);

            if (dp <= min_dist + 2* r ) {
                /* BEGIN Get smallest t from its next siblings */
                t1 = t;
                sibling_offset = node->sibling.offset;
                while (sibling_offset != DSADBINVOFFSETID) {
                    sibling = tcdsadbnodeload(page, sibling_offset);
                    if ((sibling->time <= t1) && (dp > dist(dsadb->dimensions,
                            (DSADBCORD*) kbuf, sibling->point) + 2* r )) {
                        t1 = sibling->time;
                    }
                    sibling_offset = sibling->sibling.offset;
                }
                /* END */

                const DSADBNODE *result = tcdsadbrangesearch(dsadb, node, kbuf, ksiz, r, t1);
                if (result != NULL) {
                    return result;
                }

                min_dist = MIN(min_dist,dp);
            }

            child_offset = node->sibling.offset;
        }
    }

    return NULL;
}

/* Retrieve a record in a DSA tree database object.
 `dsadb' specifies the DSA tree database object.
 `kbuf' specifies the pointer to the region of the key.
 `ksiz' specifies the size of the region of the key.
 `sp' specifies the pointer to the variable into which the size of the region of the return
 value is assigned.
 If successful, the return value is the pointer to the region of the value of the corresponding
 record. */
static const DSADBNODE *tcdsadbsearchimpl(TCDSADB *dsadb, const DSADBCORD *kbuf,
        int64_t ksiz, int64_t r, int *sp) {
    assert(dsadb && kbuf && ksiz >= 0 && sp);

    time_t t = time(NULL);
    DSADBPAGE *page = tcdsadbpageload(dsadb, dsadb->root_pid);
    DSADBNODE *elem = tcdsadbnodeload(page, dsadb->root_offset);

    return tcdsadbrangesearch(dsadb, elem, kbuf, ksiz, r, t);

}

static int tcdsadbinsertnode(DSADBPAGE *page,DSADBNODE *node)
{
    int idx = 0;
    while (idx < tcptrlistnum(page->nodes))
    {
        if (tcdsadbnodeload(page,idx) == NULL)
        {
            break;
        }
        idx++;
    }
    if (idx == tcptrlistnum(page->nodes))
    {
        tcptrlistpush(page->nodes,node);
    }
    else
    {
        tcptrlistover(page->nodes,idx,node);
    }

    return idx;
}

/* Store a record into a DSA tree database object.
 `dsadb' specifies the DSA tree database object.
 `kbuf' specifies the pointer to the region of the key.
 `ksiz' specifies the size of the region of the key.
 `vbuf' specifies the pointer to the region of the value.
 `vsiz' specifies the size of the region of the value.
 `dmode' specifies behavior when the key overlaps.
 If successful, the return value is true, else, it is false. */
static bool tcdsadbputimpl(TCDSADB *dsadb, const void *kbuf, int ksiz,
        const void *vbuf, int vsiz, int dmode) {
    assert(dsadb && kbuf && ksiz >= 0);
    int64_t pid = dsadb->root_pid;
    int64_t root_offset = dsadb->root_offset;

    /* Store the record to cache and hash db first */
    tchdbput(dsadb->hdb, kbuf, ksiz, vbuf, vsiz);
    tcmapputkeep(dsadb->nodec, kbuf, dsadb->dimensions*sizeof(DSADBCORD), vbuf, vsiz);

    /* Initialize the node */
    DSADBNODE *node = tcdsadbnodenew(dsadb,(DSADBCORD*) kbuf);

//    printf("*** Insert %d %d\n",node->point[0],node->point[1]);

    /* If the tree is empty */

    if (dsadb->root_pid == DSADBINVPAGEID) {
        DSADBPAGE *page = tcdsadbpagenew(dsadb);
        tcdsadbinsertnode(page,node);
        tcdsadbpagesave(dsadb, page);

        /* This node is the root */
        dsadb->root_pid = page->id;
        dsadb->root_offset = tcptrlistnum(page->nodes) - 1;
    }
    else
    {
        /* Get the root node */

        DSADBPAGE *page = tcdsadbpageload(dsadb, pid);
        DSADBPAGE *parent_page = NULL;
        DSADBNODE *elem = tcdsadbnodeload(page, root_offset);
        DSADBNODE *child;
        DSADBNODE *candidate;
        DSADBDIST min_dist, child_dist;
        uint16_t nchild;
        uint32_t n_size;

        int64_t first_node_offset = DSADBINVOFFSETID;
        DSADBNODE *first_node_parent = NULL;
        /* Traverse through its neighbors  */
        DSADBDIST dp = dist(dsadb->dimensions, elem->point, node->point);

        while (1)
        {
            min_dist = DSADBMAXDIST;
            candidate = NULL;
            child = elem;
            nchild = 0;
            if (elem == NULL) printf("NULL\n");
            elem->radius = MAX(elem->radius,dp);
            n_size = 0;

            parent_page = page;

            if (elem->child.pid != DSADBINVPAGEID)
            {
                /* this trick reduces number of times to load page by checking if the page is loaded or not */
                if (!page || (pid != elem->child.pid))
                {
                    page = tcdsadbpageload(dsadb, elem->child.pid);
                    first_node_offset = DSADBINVOFFSETID;
                }
                int64_t child_offset = elem->child.offset;

                if (first_node_offset == DSADBINVOFFSETID)
                {
                    first_node_offset = child_offset;
                    first_node_parent = elem;
                }

                /* traverse all the child node */
                while (child_offset != DSADBINVOFFSETID)
                {
                    nchild++;

                    child = tcdsadbnodeload(page, child_offset);

                    child_dist = dist(dsadb->dimensions, child->point,node->point);

                    if (child_dist < min_dist) {
                        min_dist = child_dist;
                        candidate = child;
                    }

                    n_size += tcdsadbnodesize(dsadb,child,child_offset);

                    child_offset = child->sibling.offset;
                }
            }
            if ((dp < min_dist) && (nchild < dsadb->arity))
            {
                // insert node to page
                uint64_t idx = tcdsadbinsertnode(page,node);
                /* Insert as a new child */

                if (child == elem)
                {
                    elem->child.pid = page->id;
                    elem->child.offset = idx;
//                    printf("Insert as a child of %d, at pid %lld offset %lld\n",elem->point[0],page->id,idx);
                }
                /* Insert as a new sibling */
                else
                {
                    child->sibling.offset = idx;
//                    printf("Insert as a sibling of %d, at pid %lld offset %lld\n",child->point[0],page->id,idx);
                }

                tcdsadbpagesave(dsadb, page);

                n_size += tcdsadbnodesize(dsadb,node,idx);

                if (page->size >= DSADBMAXDISKPAGESIZE)
                {
                    /* move to parent */
                    int node_has_child = 0;
                    if ( (parent_page != NULL) && (parent_page->id != page->id) && (parent_page->size + n_size < DSADBMAXDISKPAGESIZE))
                    {
//                        printf("############ MOVE TO PARENT:  page : %lld  -> parent : %lld\n",page->id,parent_page->id);

                        int64_t child_offset = elem->child.offset;

                        // get from current page
                        child = tcdsadbnodeload(page, child_offset);
                        if (child->child.pid == page->id)
                        {
                            node_has_child++;
                        }
                        // insert to parent page
                        int64_t idx = tcdsadbinsertnode(parent_page,child);

                        // remove from current page
                        tcptrlistover(page->nodes,child_offset,NULL);

                        // re-set far pointer
                        elem->child.pid = parent_page->id;
                        elem->child.offset = idx;

                        child_offset = child->sibling.offset;

                        // copy from page to parent_page
                        while (child_offset != DSADBINVOFFSETID)
                        {
                            // get from current page
                            DSADBNODE *temp = tcdsadbnodeload(page, child_offset);
                            // insert to parent page
                            int64_t idx = tcdsadbinsertnode(parent_page,temp);

                            // remove from current page
                            tcptrlistover(page->nodes,child_offset,NULL);

                            // update previous sibling
                            child->sibling.offset = idx;

                            if (temp->child.pid == page->id)
                            {
                                node_has_child++;
                            }

                            child = temp;
                            child_offset = child->sibling.offset;
                        }
                        tcdsadbpagesave(dsadb,page);
                        tcdsadbpagesave(dsadb,parent_page);
                        page->subtree_with_diff_parent_count = page->subtree_with_diff_parent_count - 1 + node_has_child;
                    }
                    /* vertical split */
                    else if (page->subtree_with_diff_parent_count > 1)
                    {
//                        printf("############ VERTICAL page : %lld\n",page->id);
                        int64_t added_queue[DSADBMAXNODECOUNT];
                        int first = 0;
                        int last = 0;

                        DSADBPAGE *new_page = tcdsadbpagenew(dsadb);

                        node = tcdsadbnodeload(page, first_node_offset);
                        int64_t new_idx = tcdsadbinsertnode(new_page,node);

                        first_node_parent->child.pid = new_page->id;
                        first_node_parent->child.offset = new_idx;
                        added_queue[last++] = new_idx;

                        tcptrlistover(page->nodes,first_node_offset,NULL);

                        while (last > first)
                        {
                            int idx = added_queue[first++];

                            // get node from new page
                            node = tcdsadbnodeload(new_page, idx);

                            // check next sibling
                            if ( (node->sibling.offset != DSADBINVOFFSETID) )
                            {
                                DSADBNODE *temp = tcdsadbnodeload(page, node->sibling.offset);

                                // insert to parent page
                                int new_idx = tcdsadbinsertnode(new_page,temp);

                                // add to queue
                                added_queue[last++] = new_idx;
                                tcptrlistover(page->nodes,node->sibling.offset,NULL);

                                // update
                                node->sibling.offset = new_idx;
                            }

                            if ( (node->child.pid == page->id))
                            {
                                DSADBNODE *temp = tcdsadbnodeload(page, node->child.offset);

                                // insert to parent page
                                int new_idx = tcdsadbinsertnode(new_page,temp);

                               // add to queue
                                added_queue[last++] = new_idx;
                                tcptrlistover(page->nodes,node->child.offset,NULL);

                                node->child.pid = new_page->id;
                                node->child.offset = new_idx;
                            }
                        }
                        page->subtree_with_diff_parent_count--;
                        new_page->subtree_with_diff_parent_count = 1;
                        tcdsadbpagesave(dsadb,page);
                        tcdsadbpagesave(dsadb,new_page);
                    }
                    else  /* Horizontal split */
                    {
//                        printf("############ HORIZONTAL page : %lld\n",page->id);
                        int i;

                        bool is_parent[DSADBMAXNODECOUNT];
                        memset(is_parent,1,DSADBMAXNODECOUNT*sizeof(bool));

                        /* Traverse all the nodes in page to
                            detect the parents of subtrees in this page
                            */
                        for (i = 0; i < tcptrlistnum(page->nodes); i++)
                        {
                            DSADBNODE *node = tcdsadbnodeload(page,i);

                            if (node == NULL) continue;

                            if ( (node->child.pid == page->id) && (node->child.offset != DSADBINVOFFSETID) )
                            {
                                is_parent[node->child.offset] = false;
                            }

                            if (node->sibling.offset != DSADBINVOFFSETID)
                            {
                                is_parent[node->sibling.offset] = false;
                            }
                        }

                        int64_t queue[DSADBMAXNODECOUNT];
                        int first = 0;
                        int last = 0;

                        int depth[DSADBMAXNODECOUNT];
                        int size[DSADBMAXNODECOUNT];

                        memset(depth,0,DSADBMAXNODECOUNT*sizeof(int));
                        memset(size,0,DSADBMAXNODECOUNT*sizeof(int));

                        for (i = 0; i < tcptrlistnum(page->nodes); i++)
                        {
                            DSADBNODE *node = tcdsadbnodeload(page,i);
                            if (node == NULL) continue;
                            if (is_parent[i])
                            {
                                queue[last++] = i;
                                depth[i] = 0;
                            }
                        }

                        while (last > first)
                        {
                            int64_t idx = queue[first++];
                            DSADBNODE *node = tcdsadbnodeload(page,idx);
                            size[depth[idx]] += tcdsadbnodesize(dsadb,node,idx);

//                              printf("-- %lld : depth = %d\n",idx,depth[idx]);
                            if ( (node->child.pid == page->id) && (node->child.offset != DSADBINVOFFSETID) )
                            {
                                queue[last++] = node->child.offset;
                                depth[node->child.offset] = depth[idx] + 1;
//                                printf("++ child %lld\n",node->child.offset);
                            }

                            if (node->sibling.offset != DSADBINVOFFSETID)
                            {
                                queue[last++] = node->sibling.offset;
                                depth[node->sibling.offset] = depth[idx];
//                                printf("++ dib %lld\n",node->sibling.offset);
                            }
                        }

                        /* Find the smallest d */
                        int d = 0;
                        uint64_t total_size = 0;
                        while (true)
                        {
                            total_size += size[d++];
                            if (total_size >= DSADBMAXDISKPAGESIZE / 2)
                            {
                                break;
                            }
                        }

                        if (size[d] == 0)
                        {
                            d--;
                        }

                        int64_t added_queue[DSADBMAXNODECOUNT];

                        int qsize = last;

                        first = 0;
                        last = 0;
                        DSADBPAGE *new_page = tcdsadbpagenew(dsadb);

                        int parent_node_count = 0;

                        /* We traverse all the nodes of depth d-1 to add initial nodes*/
                        for (i = 0; i < qsize; i++)
                        {
                            int64_t idx = queue[i];
                            if (depth[idx] == d - 1)
                            {
                                // get current node
                                node = tcdsadbnodeload(page, idx);
                                if ( (node->child.pid == page->id))
                                {
                                     parent_node_count++;

                                     DSADBNODE *temp = tcdsadbnodeload(page, node->child.offset);

                                     // insert to parent page
                                     int new_idx = tcdsadbinsertnode(new_page,temp);

                                     // add to queue, the index is the offset of new page
                                     added_queue[last++] = new_idx;
                                     tcptrlistover(page->nodes,node->child.offset,NULL);

                                     node->child.pid = new_page->id;
                                     node->child.offset = new_idx;
                                }
                            }
                        }

                        while (last > first)
                        {
                            int64_t idx = added_queue[first++];

                            // get node from new page
                            node = tcdsadbnodeload(new_page, idx);

                            // check next sibling
                            if ( (node->sibling.offset != DSADBINVOFFSETID) )
                            {
                                DSADBNODE *temp = tcdsadbnodeload(page, node->sibling.offset);
                                // insert to parent page
                                int new_idx = tcdsadbinsertnode(new_page,temp);

                                // add to queue
                                added_queue[last++] = new_idx;

                                tcptrlistover(page->nodes,node->sibling.offset,NULL);
                                node->sibling.offset = new_idx;
                            }

                            if ( (node->child.pid == page->id))
                            {
                                DSADBNODE *temp = tcdsadbnodeload(page, node->child.offset);
                                // insert to parent page
                                int new_idx = tcdsadbinsertnode(new_page,temp);

                                // add to queue
                                added_queue[last++] = new_idx;

                                tcptrlistover(page->nodes,node->child.offset,NULL);
                                node->child.pid = new_page->id;
                                node->child.offset = new_idx;
                            }
                        }

                        new_page->subtree_with_diff_parent_count = parent_node_count;

                        tcdsadbpagesave(dsadb,page);
                        tcdsadbpagesave(dsadb,new_page);
                    }

                }

/*                printf("page size = %d\n",page->size); */
                break;
            }
            else
            {
                elem = candidate;
                dp = min_dist;
            }
        }
    }
    return true;
}

/* Open a database file and connect a DSA tree database object.
 `dsadb' specifies the DSA tree database object.
 `path' specifies the path of the internal database file.
 `omode' specifies the connection mode.
 If successful, the return value is true, else, it is false.

 TODO: Need more comments
 */

static bool tcdsadbopenimpl(TCDSADB *dsadb, const char *path, int omode) {
    assert(dsadb && path);
    int homode = HDBOREADER;
    if (omode & DSADBOWRITER) {
        homode = HDBOWRITER;
        if (omode & DSADBOCREAT)
            homode |= HDBOCREAT;
        if (omode & DSADBOTRUNC)
            homode |= HDBOTRUNC;
        dsadb->wmode = true;
    } else {
        dsadb->wmode = false;
    }
    if (omode & DSADBONOLCK)
        homode |= HDBONOLCK;
    if (omode & DSADBOLCKNB)
        homode |= HDBOLCKNB;
    if (omode & DSADBOTSYNC)
        homode |= HDBOTSYNC;
    tchdbsettype(dsadb->hdb, TCDBTBTREE);

    if (!tchdbopen(dsadb->hdb, path, homode))
        return false;

    dsadb->nnode = 0;
    dsadb->npage = 0;
    dsadb->nodec = tcmapnew2(dsadb->ncnum * 2 + 1);
    dsadb->pagec = tcmapnew2(dsadb->pcnum * 2 + 1);
    dsadb->open = true;

    uint8_t hopts = tchdbopts(dsadb->hdb);
    uint8_t opts = 0;
/*    if(hopts & HDBTLARGE) opts |= DSADBTLARGE; */
    if(hopts & HDBTDEFLATE) opts |= DSADBTDEFLATE;
    if(hopts & HDBTBZIP) opts |= DSADBTBZIP;
    if(hopts & HDBTTCBS) opts |= DSADBTTCBS;
    if(hopts & HDBTEXCODEC) opts |= DSADBTEXCODEC;
    dsadb->opts = opts;

    dsadb->opaque = tchdbopaque(dsadb->hdb);

    if (dsadb->wmode && tchdbrnum(dsadb->hdb) < 1)
    {
        tcdsadbdumpmeta(dsadb);
    }

    tcdsadbloadmeta(dsadb);

    return true;
}

/* Clear all members.
 `dsadb' specifies the DSA tree database object. */
static void tcdsadbclear(TCDSADB *dsadb) {
    assert(dsadb);
    dsadb->hdb = NULL;
    dsadb->mmtx = NULL;
    dsadb->cmtx = NULL;
    dsadb->open = false;
    dsadb->wmode = false;
    dsadb->root_pid = DSADBINVPAGEID;
    dsadb->root_offset = DSADBINVOFFSETID;
    dsadb->nnode = 0;
    dsadb->ncnum = DSADBDEFNCNUM;
    dsadb->pcnum = DSADBDEFPCNUM;
    dsadb->arity = DSDDBDEFARITY;
}

/* Close a DSA tree database object.
 `dsadb' specifies the B+ tree database object.
 If successful, the return value is true, else, it is false. */
static bool tcdsadbcloseimpl(TCDSADB *dsadb) {
    assert(dsadb);
    bool err = false;
    dsadb->open = false;

    const char *vbuf;
    int vsiz;

    TCMAP *pagec = dsadb->pagec;
    tcmapiterinit(pagec);

    while((vbuf = tcmapiternext(pagec, &vsiz)) != NULL){
        if(!tcdsadbpagecacheout(dsadb,(DSADBPAGE* )tcmapiterval(vbuf, &vsiz) ))
          err = true;
    }

    TCMAP *nodec = dsadb->nodec;
    tcmapiterinit(nodec);

    while((vbuf = tcmapiternext(nodec, &vsiz)) != NULL){
        if(!tcdsadbnodecacheout(dsadb,vbuf,vsiz ))
          err = true;
    }

    if(dsadb->wmode) tcdsadbdumpmeta(dsadb);
    tcmapdel(dsadb->pagec);
    tcmapdel(dsadb->nodec);
    if(!tchdbclose(dsadb->hdb)) err = true;
    return !err;
}

/* Delete a DSA tree database object. */
void tcdsadbdel(TCDSADB *dsadb){
  assert(dsadb);
  if(dsadb->open) tcdsadbclose(dsadb);
  tchdbdel(dsadb->hdb);
  if(dsadb->mmtx){
    pthread_mutex_destroy(dsadb->cmtx);
    pthread_rwlock_destroy(dsadb->mmtx);
    TCFREE(dsadb->cmtx);
    TCFREE(dsadb->mmtx);
  }
  TCFREE(dsadb);
}

/*************************************************************************************************
 * API
 *************************************************************************************************/

/* Get the last happened error code of a B+ tree database object. */
int tcdsadbecode(TCDSADB *dsadb) {
    assert(dsadb);
    return tchdbecode(dsadb->hdb);
}

/* Get the message string corresponding to an error code. */
const char *tcdsadberrmsg(int ecode) {
    return tcerrmsg(ecode);
}

TCDSADB *tcdsadbnew(void) {
    TCDSADB *dsadb;
    TCMALLOC(dsadb, sizeof(*dsadb));
    tcdsadbclear(dsadb);
    dsadb->hdb = tchdbnew();
    dsadb->nnode = 0;
    dsadb->npage = 0;
    tchdbsetxmsiz(dsadb->hdb, 0);
    return dsadb;
}

/* Open a database file and connect a DSA tree database object. */
bool tcdsadbopen(TCDSADB *dsadb, const char *path, int omode) {
    assert(dsadb && path);
    if (!DSADBLOCKMETHOD(dsadb, true))
        return false;
    if (dsadb->open) {
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        DSADBUNLOCKMETHOD(dsadb);
        return false;
    }
    bool rv = tcdsadbopenimpl(dsadb, path, omode);
    DSADBUNLOCKMETHOD(dsadb);
    return rv;
}

/* Store a record into a DSA tree database object. */
bool tcdsadbput(TCDSADB *dsadb, const void *kbuf, int ksiz, const void *vbuf,
        int vsiz) {

    assert(dsadb && kbuf && ksiz >= 0 && vbuf && vsiz >= 0);
    int sp;

    if (ksiz < dsadb->dimensions * sizeof(DSADBCORD))
    {
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        return false;
    }

    /* Check if this key is exist or not */
    if (tcdsadbget(dsadb,kbuf,ksiz,&sp))
    {
        return true;
    }

    if (!DSADBLOCKMETHOD(dsadb, true))
        return false;

    if(!dsadb->open || !dsadb->wmode)
    {
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        DSADBUNLOCKMETHOD(dsadb);
        return false;
    }

    bool rv = tcdsadbputimpl(dsadb, kbuf, ksiz, vbuf, vsiz, DSADBPDOVER);

    DSADBUNLOCKMETHOD(dsadb);
    bool adj = TCMAPRNUM(dsadb->pagec) > dsadb->pcnum || TCMAPRNUM(dsadb->nodec) > dsadb->ncnum;
    if(adj && DSADBLOCKMETHOD(dsadb, true)){
      tcdsadbcacheadjust(dsadb);
      DSADBUNLOCKMETHOD(dsadb);
    }
    return rv;
}

/* Store a string record into a DSA tree database object. */
bool tcdsadbput2(TCDSADB *dsadb, const char *str, const char *vstr) {
    assert(dsadb && kstr && vstr);
    DSADBCORD *kstr;
    TCMALLOC(kstr, strlen(str)*sizeof(DSADBCORD));
    for (int i = 0; i < strlen(str); i++) {
        kstr[i] = str[i];
    }
    return tcdsadbput(dsadb, kstr, strlen(str) * sizeof(DSADBCORD), vstr,
            strlen(vstr));
}

/* Search for a record in a DSAT tree database object. */
void *tcdsadbsearch(TCDSADB *dsadb, const void *kbuf, int ksiz, int64_t r, int *sp) {
    assert(dsadb && kbuf && ksiz >= 0 && sp);

    if (ksiz < dsadb->dimensions * sizeof(DSADBCORD))
    {
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        return NULL;
    }

    /* Try to get directly from hash database */
    const char *vbuf = tcdsadbget(dsadb, kbuf, ksiz, sp);

    if (!DSADBLOCKMETHOD(dsadb, false))
        return NULL;

    if(!dsadb->open){
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        DSADBUNLOCKMETHOD(dsadb);
        return NULL;
    }

    if (vbuf != NULL)
    {
        DSADBUNLOCKMETHOD(dsadb);
    }
    else // (vbuf == NULL)
    {
        DSADBNODE *node = (DSADBNODE*) tcdsadbsearchimpl(dsadb, kbuf, ksiz, r, sp);
        DSADBUNLOCKMETHOD(dsadb);
        if (node != NULL)
        {
            vbuf = tcdsadbget(dsadb, node->point, dsadb->dimensions*sizeof(DSADBCORD), sp);
        }
    }

    char *rv;

    if (vbuf) {
        TCMEMDUP(rv, vbuf, *sp);
    } else {
        rv = NULL;
    }

    bool adj = TCMAPRNUM(dsadb->pagec) > dsadb->pcnum || TCMAPRNUM(dsadb->nodec) > dsadb->ncnum;

    if(adj && DSADBLOCKMETHOD(dsadb, true)){
      tcdsadbcacheadjust(dsadb);
      DSADBUNLOCKMETHOD(dsadb);
    }
    return rv;
}

/* Search for a record with key as string in a DSAT tree database object. */
void *tcdsadbsearch2(TCDSADB *dsadb, const char *kbuf, int64_t r) {
    assert(dsadb && kbuf && ksiz >= 0);

    DSADBCORD *kstr;
    TCMALLOC(kstr, strlen(kbuf)*sizeof(DSADBCORD));
    for (int i = 0; i < strlen(kbuf); i++) {
        kstr[i] = kbuf[i];
    }
    int sp;

    return tcdsadbsearch(dsadb, kstr, strlen(kbuf) * sizeof(DSADBCORD), r, &sp);
}

/* Close a DSA tree database object. */
bool tcdsadbclose(TCDSADB *dsadb) {
    assert(dsadb);
    if (!DSADBLOCKMETHOD(dsadb, true))
        return false;

    if(!dsadb->open){
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        DSADBUNLOCKMETHOD(dsadb);
        return false;
    }

    bool rv = tcdsadbcloseimpl(dsadb);
    DSADBUNLOCKMETHOD(dsadb);
    return rv;
}


/*************************************************************************************************
 * debugging functions
 *************************************************************************************************/


/* Print meta data of the header into the debugging output.
   `dsadb' specifies the DSA tree database object. */
/*
void tcdsadbprintmeta(TCDSADB *dsadb){
  assert(dsadb);
  int dbgfd = tchdbdbgfd(dsadb->hdb);
  if(dbgfd < 0) return;
  if(dbgfd == UINT16_MAX) dbgfd = 1;
  char buf[BDBPAGEBUFSIZ];
  char *wp = buf;
  wp += sprintf(wp, "META:");
  wp += sprintf(wp, " mmtx=%p", (void *)bdbdsadb->mmtx);
  wp += sprintf(wp, " cmtx=%p", (void *)dsadb->cmtx);
  wp += sprintf(wp, " hdb=%p", (void *)dsadb->hdb);
  wp += sprintf(wp, " opaque=%p", (void *)dsadb->opaque);
  wp += sprintf(wp, " open=%d", dsadb->open);
  wp += sprintf(wp, " wmode=%d", dsadb->wmode);

  wp += sprintf(wp, " lmemb=%u", dsadb->lmemb);
  wp += sprintf(wp, " nmemb=%u", bdb->nmemb);
  wp += sprintf(wp, " opts=%u", bdb->opts);
  wp += sprintf(wp, " root=%llx", (unsigned long long)bdb->root);
  wp += sprintf(wp, " first=%llx", (unsigned long long)bdb->first);
  wp += sprintf(wp, " last=%llx", (unsigned long long)bdb->last);
  wp += sprintf(wp, " lnum=%llu", (unsigned long long)bdb->lnum);
  wp += sprintf(wp, " nnum=%llu", (unsigned long long)bdb->nnum);
  wp += sprintf(wp, " rnum=%llu", (unsigned long long)bdb->rnum);
  wp += sprintf(wp, " leafc=%p", (void *)bdb->leafc);
  wp += sprintf(wp, " nodec=%p", (void *)bdb->nodec);
  wp += sprintf(wp, " cmp=%p", (void *)(intptr_t)bdb->cmp);
  wp += sprintf(wp, " cmpop=%p", (void *)bdb->cmpop);
  wp += sprintf(wp, " lcnum=%u", bdb->lcnum);
  wp += sprintf(wp, " ncnum=%u", bdb->ncnum);
  wp += sprintf(wp, " lsmax=%u", bdb->lsmax);
  wp += sprintf(wp, " lschk=%u", bdb->lschk);
  wp += sprintf(wp, " capnum=%llu", (unsigned long long)bdb->capnum);
  wp += sprintf(wp, " hist=%p", (void *)bdb->hist);
  wp += sprintf(wp, " hnum=%d", bdb->hnum);
  wp += sprintf(wp, " hleaf=%llu", (unsigned long long)bdb->hleaf);
  wp += sprintf(wp, " lleaf=%llu", (unsigned long long)bdb->lleaf);
  wp += sprintf(wp, " tran=%d", bdb->tran);
  wp += sprintf(wp, " rbopaque=%p", (void *)bdb->rbopaque);
  wp += sprintf(wp, " clock=%llu", (unsigned long long)bdb->clock);
  wp += sprintf(wp, " cnt_saveleaf=%lld", (long long)bdb->cnt_saveleaf);
  wp += sprintf(wp, " cnt_loadleaf=%lld", (long long)bdb->cnt_loadleaf);
  wp += sprintf(wp, " cnt_killleaf=%lld", (long long)bdb->cnt_killleaf);
  wp += sprintf(wp, " cnt_adjleafc=%lld", (long long)bdb->cnt_adjleafc);
  wp += sprintf(wp, " cnt_savenode=%lld", (long long)bdb->cnt_savenode);
  wp += sprintf(wp, " cnt_loadnode=%lld", (long long)bdb->cnt_loadnode);
  wp += sprintf(wp, " cnt_adjnodec=%lld", (long long)bdb->cnt_adjnodec);
  *(wp++) = '\n';
  tcwrite(dbgfd, buf, wp - buf);
}
*/

/* END OF FILE */
