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
#define DSADBPAGESIZE         (1LL<<14)         /* maximum size of disk page (1LL<<12) */
#define DSADBMAXNODECOUNT     (1LL<<10)         /* maximum size of disk page */
#define DSADBMAXNODECACHE     2048              /* maximum number or node to be cached */
#define DSADBMAXPAGECACHE     2048              /* maximum number or page to be cached */
#define DSADBINVPAGEID       -1                 /* invalid page id */
#define DSADBINVOFFSETID     -1                 /* invalid offset id */
#define DSADBMAXDIST          (300LL*81)        /* specific to Image Mark case */
#define DSADBDEFDIMENSION     81                /* max dimenstion of one node */

#define DSADBCACHEOUT         64                /* number of pages in a process of cacheout  */

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
    DSADBFPTR child;
    DSADBLPTR sibling;
    DSADBDIST radius;
    DSADBCORD point[DSADBDEFDIMENSION];
} DSADBNODE; /* DSAT node */

typedef struct {
    uint64_t id;
    bool dirty;
    uint32_t subtree_with_diff_parent_count;
    uint64_t node_count;
    uint32_t depth;
    DSADBNODE nodes[];
} DSADBPAGE; /* Page structure */

enum { /* enumeration for duplication behavior */
    DSADBPDOVER,     /* overwrite an existing value */
    DSADBPDKEEP,     /* keep the existing value */
    DSADBPDCAT,      /* concatenate values */
    DSADBPDDUP,      /* allow duplication of keys */
    DSADBPDDUPB,     /* allow backward duplication */
    DSADBPDADDINT,   /* add an integer */
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
static bool tcdsadbpagesave(TCDSADB *dsadb, DSADBPAGE *page);
static DSADBPAGE *tcdsadbpageload(TCDSADB *dsadb, uint64_t id);
void *tcdsadbget(TCDSADB *dsadb, const DSADBCORD *kbuf, uint64_t ksiz, int *sp);
static bool tcdsadbnodecheck(TCDSADB *dsadb, const DSADBCORD *kbuf, uint64_t ksiz);
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
void tcdsadbprintmeta(TCDSADB *dsadb);
/*************************************************************************************************
 * UTIL FUNTIONS
 ************************************************************************************************/

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
  if(pcnum > 0) dsadb->pcnum = tclmin(pcnum, DSADBMAXPAGECACHE);
  if(ncnum > 0) dsadb->ncnum = tclmin(ncnum, DSADBMAXNODECACHE);
  return true;
}

static bool tcdsadbpagecacheout(TCDSADB *dsadb, DSADBPAGE *page)
{
  assert(dsadb && page);

  if (!page->dirty) return true;

  bool err = false;
  if(!tcdsadbpagesave(dsadb,page)) err = true;
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
    TCDODEBUG(dsadb->cnt_cnt_adjpagec++);

    int ecode = tchdbecode(dsadb->hdb);
    bool clk = DSADBLOCKCACHE(dsadb);
    TCMAP *pagec= dsadb->pagec;
    tcmapiterinit(pagec);
    int dnum = tclmin(tclmax(TCMAPRNUM(pagec) - dsadb->pcnum, DSADBCACHEOUT),dsadb->pcnum);

    for(int i = 0; i < dnum; i++)
    {
        int rsiz;
        void* x = (void*) tcmapiternext(pagec, &rsiz);
        DSADBPAGE* temp = (DSADBPAGE *)tcmapiterval(x, &rsiz);
        if (temp->id == dsadb->root_pid) continue;
        if(!tcdsadbpagecacheout(dsadb,temp))
            err = true;
    }

    if(clk) DSADBUNLOCKCACHE(dsadb);
    if(!err && tchdbecode(dsadb->hdb) != ecode)
      tcdsadbsetecode(dsadb, ecode, __FILE__, __LINE__, __func__);
  }

  if(TCMAPRNUM(dsadb->nodec) > dsadb->ncnum)
  {
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
    return (DSADBNODE *) &page->nodes[index];
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

    for (int i=0;i<DSADBDEFDIMENSION;i++)
    {
        node->point[i] = point[i];
    }

    node->time = time(NULL);

    return node;
}

static uint32_t tcdsadbnodesize(TCDSADB *dsadb,DSADBNODE *node, uint32_t offset)
{
    return sizeof(DSADBNODE);
}

/* Create a new page.
 `dsadb' specifies the DSA tree database object.
 The return value is the new page object. */

static DSADBPAGE *tcdsadbpagenew(TCDSADB *dsadb) {
    assert(dsadb);
    DSADBPAGE *page;
    uint64_t id = ++dsadb->npage + DSADBPAGEIDBASE;
    TCMALLOC(page,DSADBPAGESIZE);
    memset(page,0,DSADBPAGESIZE);
    page->id = id;
    page->subtree_with_diff_parent_count = 1;
    page->dirty = true;
    page->node_count = 0;
    tcmapputkeep(dsadb->pagec, &id, sizeof(id), page, DSADBPAGESIZE);
    int rsiz;

//    printf("PAGE IN NEW %llu\n",page.id);

    TCFREE(page);
    return (DSADBPAGE *)tcmapget(dsadb->pagec, &id, sizeof(id), &rsiz);
}

/* Save a page into the internal database.
 `dsadb' specifies the DSA tree database object.
 `page' specifies the page object.
 If successful, the return value is true, else, it is false. */
static bool tcdsadbpagesave(TCDSADB *dsadb, DSADBPAGE *page) {

    assert(dsadb && page);
    char hbuf[(sizeof(uint64_t) + 1) * 2];
    int step = sprintf(hbuf, "%llx", (unsigned long long) page->id);
    if (!tchdbout(dsadb->hdb, hbuf, step) && tchdbecode(dsadb->hdb) != TCENOREC)
            return false;
    return tchdbput(dsadb->hdb, hbuf, step, page, DSADBPAGESIZE);
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
        TCDODEBUG(dsadb->cnt_cachehit++);
        if (clk)
            DSADBUNLOCKCACHE(dsadb);
        return p;
    }

    TCDODEBUG(dsadb->cnt_cachemiss++);
    if (clk)
        DSADBUNLOCKCACHE(dsadb);
    //TCDODEBUG(dsadb->cnt_loadnode++);

    // Not available in cache :
    char hbuf[(sizeof(uint64_t) + 1) * 2];
    int step;

    // Get the order of this node in list
    step = sprintf(hbuf, "%llx", (unsigned long long) (id));

    DSADBPAGE *page;
    TCMALLOC(page,DSADBPAGESIZE);

    rsiz = tchdbget3(dsadb->hdb, hbuf, step, page, DSADBPAGESIZE);

    clk = DSADBLOCKCACHE(dsadb);
    tcmapput(dsadb->pagec, &id, sizeof(id), page, DSADBPAGESIZE);
    TCFREE(page);

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

/* Check existence of a node in the internal database.
   `dsadb' specifies the DSA tree database object.
   `id' specifies the ID number of the leaf.
   The return value is true if the leaf exists, else, it is false. */
static bool tcdsadbnodecheck(TCDSADB *dsadb, const DSADBCORD *kbuf, uint64_t ksiz) {
  assert(dsadb && id > 0);
  bool clk = DSADBLOCKCACHE(dsadb);
  int rsiz;
  void *value = (void*) tcmapget3(dsadb->nodec, kbuf, ksiz, &rsiz);
  if(clk) DSADBUNLOCKCACHE(dsadb);
  if(value) return true;

  return tchdbvsiz(dsadb->hdb, kbuf, ksiz) > 0;
}

static const DSADBNODE *tcdsadbrangesearch(TCDSADB *dsadb, DSADBNODE *elem,
        const void *kbuf, int64_t ksiz, int64_t r, time_t t) {
    DSADBDIST dp, dp1,min_dist;
    DSADBNODE *sibling;
    int32_t child_offset, sibling_offset;
    time_t t1;

    DSADBCORD* ktemp = (DSADBCORD*) kbuf;
    TCDSADBDIST(DSADBDEFDIMENSION, ktemp, elem->point, dp);

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
            DSADBCORD* ktemp = (DSADBCORD*) kbuf;
            TCDSADBDIST(DSADBDEFDIMENSION, ktemp, node->point, dp);

            if (dp <= min_dist + 2* r ) {

                /* BEGIN Get smallest t from its next siblings */
                t1 = t;
                sibling_offset = node->sibling.offset;
                while (sibling_offset != DSADBINVOFFSETID) {
                    sibling = tcdsadbnodeload(page, sibling_offset);

                    TCDSADBDIST(DSADBDEFDIMENSION, ktemp, sibling->point, dp1);

                    if ((sibling->time <= t1) && (dp > dp1 + 2* r )) {
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
    while (page->nodes[idx].time != 0)
    {
        idx++;
    }

    memcpy(&(page->nodes[idx]),node,sizeof(DSADBNODE));

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

    /* Check if this key is exist or not */
    if (tcdsadbnodecheck(dsadb,kbuf,ksiz))
    {
        tchdbput(dsadb->hdb, kbuf, ksiz, vbuf, vsiz);
        tcmapputkeep(dsadb->nodec, kbuf, DSADBDEFDIMENSION*sizeof(DSADBCORD), vbuf, vsiz);
        return true;
    }

    dsadb->nnode++;
    /* Store the record to cache and hash db first */
    tchdbput(dsadb->hdb, kbuf, ksiz, vbuf, vsiz);
    tcmapputkeep(dsadb->nodec, kbuf, DSADBDEFDIMENSION*sizeof(DSADBCORD), vbuf, vsiz);

    /* Initialize the node */
    DSADBNODE *node = tcdsadbnodenew(dsadb,(DSADBCORD*) kbuf);

    printf("*** Insert %d %d\n",node->point[0],node->point[1]);

    /* If the tree is empty */
    if (dsadb->root_pid == DSADBINVPAGEID) {
        DSADBPAGE *page = tcdsadbpagenew(dsadb);
        int idx = tcdsadbinsertnode(page,node);
        TCFREE(node);
        tcdsadbpagesave(dsadb, page);

        /* This node is the root */
        dsadb->root_pid = page->id;
		page->depth = 1;
        dsadb->root_offset = idx;
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

        int64_t first_node_offset = DSADBINVOFFSETID;
        DSADBNODE *first_node_parent = NULL;

        /* Traverse through its neighbors  */
        DSADBDIST dp;
        TCDSADBDIST(DSADBDEFDIMENSION, elem->point, node->point, dp);

        while (1)
        {
            min_dist = DSADBMAXDIST;
            candidate = NULL;
            child = elem;
            nchild = 0;
            elem->radius = MAX(elem->radius,dp);
            int node_count = 0;

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

                    TCDSADBDIST(DSADBDEFDIMENSION, child->point,node->point,child_dist);

                    if (child_dist < min_dist) {
                        min_dist = child_dist;
                        candidate = child;
                    }

                    node_count++;

                    child_offset = child->sibling.offset;
                }
            }

            if ((dp < min_dist) && (nchild < dsadb->arity))
            {
                // insert node to page
                uint64_t idx = tcdsadbinsertnode(page,node);
                TCFREE(node);
                node = tcdsadbnodeload(page,idx);

                /* Insert as a new child */
                if (child == elem)
                {
                    elem->child.pid = page->id;
                    elem->child.offset = idx;
//                    printf("Insert as child of %d %d, pid=%lld idx = %lld \n",child->point[0],child->point[1],page->id,idx);
                }
                /* Insert as a new sibling */
                else
                {
                    child->sibling.offset = idx;
//                    printf("Insert as sibling of %d %d, pid=%lld idx = %lld \n",child->point[0],child->point[1],page->id,idx);
                }
                printf("INSERTED\n");
                page->dirty = true;

                node_count++;
                page->node_count++;

                if (page->node_count < dsadb->maxnodeperpage)
                {
                    if (page->id == dsadb->root_pid)
                    {
                        tcdsadbpagesave(dsadb,page);
                    }
                }
                else // (page->size  >= DSADBPAGESIZE)
                {
                    /* move to parent */
                    int node_has_child = 0;
                    if ( (parent_page != NULL) && (parent_page->id != page->id) && (parent_page->node_count + node_count < dsadb->maxnodeperpage))
                    {
//                        printf("############ MOVE TO PARENT:  page : %lld  -> parent : %lld\n",page->id,parent_page->id);

                        uint32_t removed_node_count = 0;
                        uint32_t added_node_count = 0;

                        int64_t child_offset = elem->child.offset;

                        // get from current page
                        child = tcdsadbnodeload(page, child_offset);
                        if (child->child.pid == page->id)
                        {
                            node_has_child++;
                        }

                        // insert to parent page
                        int64_t idx = tcdsadbinsertnode(parent_page,child);

                        // reload from new location
                        child = tcdsadbnodeload(parent_page, idx);

                        // remove from current page
                        page->nodes[child_offset].time = 0;

                        removed_node_count ++;
                        added_node_count++;

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
                            temp->time = 0;

                            // reload from new location
                            temp = tcdsadbnodeload(parent_page, idx);

                            removed_node_count ++;
                            added_node_count++;

                            // update previous sibling
                            child->sibling.offset = idx;

                            if (temp->child.pid == page->id)
                            {
                                node_has_child++;
                            }

                            child = temp;
                            child_offset = child->sibling.offset;
                        }

                        page->node_count -= removed_node_count;
                        parent_page->node_count += added_node_count;

                        page->dirty = true;
                        parent_page->dirty = true;

                        if (parent_page->id == dsadb->root_pid)
                        {
                            tcdsadbpagesave(dsadb,parent_page);
                        }

                        page->subtree_with_diff_parent_count = page->subtree_with_diff_parent_count - 1 + node_has_child;
                    }
                    /* vertical split */
                    else if (page->subtree_with_diff_parent_count > 1)
                    {
//                        printf("############ VERTICAL page : %lld\n",page->id);
                        int64_t added_queue[DSADBMAXNODECOUNT];
                        int first = 0;
                        int last = 0;
                        uint32_t removed_node_count = 0;
                        uint32_t added_node_count = 0;

                        DSADBPAGE *new_page = tcdsadbpagenew(dsadb);

                        // get from current page
                        node = tcdsadbnodeload(page, first_node_offset);

                        // insert to new page
                        int64_t new_idx = tcdsadbinsertnode(new_page,node);

                        node->time = 0;

                        // reload from new page
                        node = tcdsadbnodeload(new_page, new_idx);

                        first_node_parent->child.pid = new_page->id;
                        first_node_parent->child.offset = new_idx;
                        added_queue[last++] = new_idx;

                        removed_node_count ++;
                        added_node_count ++;

                        while (last > first)
                        {
                            int idx = added_queue[first++];

                            // get node from new page
                            node = tcdsadbnodeload(new_page, idx);

                            // check next sibling
                            if ( (node->sibling.offset != DSADBINVOFFSETID) )
                            {
                                DSADBNODE *temp = tcdsadbnodeload(page, node->sibling.offset);

                                // insert to new page
                                int new_idx = tcdsadbinsertnode(new_page,temp);

                                // add to queue
                                added_queue[last++] = new_idx;

                                temp->time = 0;

                                removed_node_count ++;
                                added_node_count ++;

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

                                temp->time = 0;

                                removed_node_count ++;
                                added_node_count ++;

                                node->child.pid = new_page->id;
                                node->child.offset = new_idx;
                            }
                        }

                        page->node_count -= removed_node_count;
                        new_page->node_count += added_node_count;

                        page->subtree_with_diff_parent_count--;
                        new_page->subtree_with_diff_parent_count = 1;

                        page->dirty = true;
						new_page->depth = page->depth;

                        if (page->id == dsadb->root_pid)
                        {
                            tcdsadbpagesave(dsadb,page);
                        }
                    }
                    else  /* Horizontal split */
                    {
//                        printf("############ HORIZONTAL page : %lld\n",page->id);
                        int i;
                        uint32_t removed_node_count = 0;
                        uint32_t added_node_count = 0;

                        bool is_parent[DSADBMAXNODECOUNT];
                        memset(is_parent,1,DSADBMAXNODECOUNT*sizeof(bool));

                        /* Traverse all the nodes in page to
                            detect the parents of subtrees in this page
                        */

                        for (i = 0; i < dsadb->maxnodeperpage; i++)
                        {
                            DSADBNODE *node = tcdsadbnodeload(page,i);

                            if (node->time == 0) continue;

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

                        for (i = 0; i < dsadb->maxnodeperpage; i++)
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
//                            printf("NODE INDEX %lld data : %d %d \n",idx,node->point[0],node->point[1]);
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
//                                printf("++ sib %lld\n",node->sibling.offset);
                            }
                        }
                        /* Find the smallest d */
                        int d = 0;
                        uint64_t total_size = 0;
                        while (true)
                        {
                            total_size += size[d++];
                            if (total_size >= DSADBPAGESIZE / 2)
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

                        /* We traverse all the nodes of depth d-1 to add initial nodes */
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

                                     temp->time = 0;

                                     // add to queue, the index is the offset of new page
                                     added_queue[last++] = new_idx;

                                     removed_node_count ++;
                                     added_node_count ++;

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

                                temp->time = 0;

                                // add to queue
                                added_queue[last++] = new_idx;

                                removed_node_count ++;
                                added_node_count ++;

                                node->sibling.offset = new_idx;
                            }

                            if ( (node->child.pid == page->id))
                            {
                                DSADBNODE *temp = tcdsadbnodeload(page, node->child.offset);
                                // insert to parent page
                                int new_idx = tcdsadbinsertnode(new_page,temp);

                                temp->time = 0;

                                // add to queue
                                added_queue[last++] = new_idx;

                                removed_node_count ++;
                                added_node_count ++;

                                node->child.pid = new_page->id;
                                node->child.offset = new_idx;
                            }
                        }

                        page->node_count -= removed_node_count;
                        new_page->node_count += added_node_count;

                        new_page->subtree_with_diff_parent_count = parent_node_count;

                        page->dirty = true;
						new_page->depth = page->depth + 1;
                        if (page->id == dsadb->root_pid)
                        {
                            tcdsadbpagesave(dsadb,page);
                        }
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

    TCDODEBUG(dsadb->cnt_cachehit=0);
    TCDODEBUG(dsadb->cnt_cachemiss=0);
    TCDODEBUG(dsadb->cnt_adjpagec=0);
    TCDODEBUG(dsadb->cnt_savepage=0);
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
    dsadb->depth = 0;
    dsadb->maxnodeperpage = (DSADBPAGESIZE - sizeof(DSADBPAGE))/sizeof(DSADBNODE) - 1;
    printf("max node per page : %lld\n",dsadb->maxnodeperpage);
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
    tcdsadbprintmeta(dsadb);
    DSADBUNLOCKMETHOD(dsadb);
    return rv;
}

/* Store a record into a DSA tree database object. */
bool tcdsadbput(TCDSADB *dsadb, const void *kbuf, int ksiz, const void *vbuf,
        int vsiz) {

    assert(dsadb && kbuf && ksiz >= 0 && vbuf && vsiz >= 0);

    if (ksiz < DSADBDEFDIMENSION * sizeof(DSADBCORD))
    {
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        return false;
    }

    ksiz = MIN(ksiz,DSADBDEFDIMENSION * sizeof(DSADBCORD));

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

    if (ksiz < DSADBDEFDIMENSION * sizeof(DSADBCORD))
    {
        tcdsadbsetecode(dsadb, TCEINVALID, __FILE__, __LINE__, __func__);
        return NULL;
    }

    ksiz = MIN(ksiz,DSADBDEFDIMENSION * sizeof(DSADBCORD));

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
            vbuf = tcdsadbget(dsadb, node->point, DSADBDEFDIMENSION*sizeof(DSADBCORD), sp);
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
    tcdsadbprintmeta(dsadb);
    DSADBUNLOCKMETHOD(dsadb);
    return rv;
}


/*************************************************************************************************
 * debugging functions
 *************************************************************************************************/


/* Print meta data of the header into the debugging output.
   `dsadb' specifies the DSA tree database object. */

void tcdsadbprintmeta(TCDSADB *dsadb){
  assert(dsadb);

  char buf[DSADBPAGEBUFSIZ];
  char *wp = buf;
  wp += sprintf(wp, "META:");
  wp += sprintf(wp, " nodecount=%lld", dsadb->nnode) ;
  wp += sprintf(wp, " pagecount=%lld", dsadb->npage) ;
  wp += sprintf(wp, " page cache count=%d", dsadb->pcnum) ;
  wp += sprintf(wp, " cnt_cachehit=%lld", (long long) dsadb->cnt_cachehit) ;
  wp += sprintf(wp, " cnt_cachemiss=%lld",(long long) dsadb->cnt_cachemiss) ;
  wp += sprintf(wp, " cnt_adjpagec=%lld", (long long) dsadb->cnt_adjpagec) ;
  wp += sprintf(wp, " cnt_savepage=%lld", (long long) dsadb->cnt_savepage) ;
  *(wp++) = '\n';
  *(wp++) = '\0';
  printf("%s",buf);
}


/* END OF FILE */
