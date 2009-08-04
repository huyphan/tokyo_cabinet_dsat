#include "tcutil.h"
#include "tchdb.h"
#include "tcddb.h"
#include "myconf.h"
#include "time.h"

#define DDBPAGEBUFSIZ   ((1LL<<15)+1)     /* size of a buffer to read each page */
#define DDBNODEIDBASE   ((1LL<<63)+1)
#define DDBPAGEIDBASE   1
#define DDBDEFVCNUM     512               /* default number of node cache */
#define DDBDEFPCNUM     512               /* default number of page cache */
#define DDBMAXDISKPAGESIZE  124           /* maximum size of disk page (1LL<<12) */
#define DDBMAXNODECOUNT     (1LL<<10)     /* maximum size of disk page */
#define DDBINVPAGEID   -1                 /* invalid page id */
#define DDBINVOFFSETID -1                 /* invalid offset id */
#define DDBMAXDIST      (250LL*81)        /* specific to Image Mark case */

typedef unsigned short DDBDIST;
typedef unsigned int DDBCORD;

typedef struct {
    int64_t pid;
    int64_t offset;
} DDBFPTR; /* Far pointer */

typedef struct {
    int64_t offset;
} DDBLPTR; /* Local page pointer */

typedef struct {
    uint64_t time;
    /*  uint64_t id; */
    DDBFPTR child;
    DDBLPTR sibling;
    DDBDIST radius;
    DDBCORD *point;
    uint32_t depth;
} DDBNODE; /* DSAT node */

typedef struct {
    uint64_t id;
    bool dirty;
    uint32_t subtree_with_diff_parent_count;
    uint64_t size;
    TCPTRLIST *nodes;
} DDBPAGE; /* Page structure */

enum { /* enumeration for duplication behavior */
    DDBPDOVER, /* overwrite an existing value */
    DDBPDKEEP, /* keep the existing value */
    DDBPDCAT, /* concatenate values */
    DDBPDDUP, /* allow duplication of keys */
    DDBPDDUPB, /* allow backward duplication */
    DDBPDADDINT, /* add an integer */
    DDBPDADDDBL, /* add a real number */
    DDBPDPROC
/* process by a callback function */
};

#define DDBLOCKMETHOD(TC_ddb, TC_wr) \
  ((TC_ddb)->mmtx ? tcddblockmethod((TC_ddb), (TC_wr)) : true)
#define DDBUNLOCKMETHOD(TC_ddb) \
  ((TC_ddb)->mmtx ? tcddbunlockmethod(TC_ddb) : true)

#define DDBLOCKCACHE(TC_ddb) \
  ((TC_ddb)->mmtx ? tcddblockcache(TC_ddb) : true)
#define DDBUNLOCKCACHE(TC_ddb) \
  ((TC_ddb)->mmtx ? tcddbunlockcache(TC_ddb) : true)

/*************************************************************************************************
 * UTIL FUNTIONS
 ************************************************************************************************/

static DDBDIST dist(uint8_t dimensions, DDBCORD *point1, DDBCORD *point2) {
    DDBDIST tot = 0;
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
void tcddbsetdbgfd(TCDDB *ddb, int fd){
  assert(ddb && fd >= 0);
  tchdbsetdbgfd(ddb->hdb, fd);
}


/* Get the file descriptor for debugging output. */
int tcddbdbgfd(TCDDB *ddb){
  assert(ddb);
  return tchdbdbgfd(ddb->hdb);
}

/* Set the error code of a DSA tree database object. */
void tcddbsetecode(TCDDB *ddb, int ecode, const char *filename, int line,
        const char *func) {
    assert(ddb && filename && line >= 1 && func);
    tchdbsetecode(ddb->hdb, ecode, filename, line, func);
}

/* Lock a method of the DSA tree database object.
 `ddb' specifies the DSA tree database object.
 `wr' specifies whether the lock is writer or not.
 If successful, the return value is true, else, it is false. */
static bool tcddblockmethod(TCDDB *ddb, bool wr) {
    assert(ddb);
    if (wr ? pthread_rwlock_wrlock(ddb->mmtx) != 0 : pthread_rwlock_rdlock(
            ddb->mmtx) != 0) {
        tcddbsetecode(ddb, TCETHREAD, __FILE__, __LINE__, __func__);
        return false;
    }
    TCTESTYIELD();
    return true;
}

/* Unlock a method of the DSA tree database object.
 `ddb' specifies the DSA tree database object.
 If successful, the return value is true, else, it is false. */
static bool tcddbunlockmethod(TCDDB *ddb) {
    assert(ddb);
    if (pthread_rwlock_unlock(ddb->mmtx) != 0) {
        tcddbsetecode(ddb, TCETHREAD, __FILE__, __LINE__, __func__);
        return false;
    }
    TCTESTYIELD();
    return true;
}

/* Lock the cache of the DSA tree database object.
 `ddb' specifies the DSA tree database object.
 If successful, the return value is true, else, it is false. */
static bool tcddblockcache(TCDDB *ddb) {
    assert(ddb);
    if (pthread_mutex_lock(ddb->cmtx) != 0) {
        tcddbsetecode(ddb, TCETHREAD, __FILE__, __LINE__, __func__);
        return false;
    }
    TCTESTYIELD();
    return true;
}

/* Unlock the cache of the DSA tree database object.
 `ddb' specifies the DSA tree database object.
 If successful, the return value is true, else, it is false. */
static bool tcddbunlockcache(TCDDB *ddb) {
    assert(ddb);
    if (pthread_mutex_unlock(ddb->cmtx) != 0) {
        tcddbsetecode(ddb, TCETHREAD, __FILE__, __LINE__, __func__);
        return false;
    }
    TCTESTYIELD();
    return true;
}

/* Set mutual exclusion control of a DSA tree database object for threading. */
bool tcddbsetmutex(TCDDB *ddb){
  assert(ddb);
  if(!TCUSEPTHREAD) return true;
  if(ddb->mmtx || ddb->open){
    tcddbsetecode(ddb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  TCMALLOC(ddb->mmtx, sizeof(pthread_rwlock_t));
  TCMALLOC(ddb->cmtx, sizeof(pthread_mutex_t));
  bool err = false;
  if(pthread_rwlock_init(ddb->mmtx, NULL) != 0) err = true;
  if(pthread_mutex_init(ddb->cmtx, NULL) != 0) err = true;
  if(err) {
    TCFREE(ddb->cmtx);
    TCFREE(ddb->mmtx);
    ddb->cmtx = NULL;
    ddb->mmtx = NULL;
    return false;
  }
  return tchdbsetmutex(ddb->hdb);
}



/* Set the size of the extra mapped memory of a DSA tree database object. */
bool tcddbsetxmsiz(TCDDB *ddb, int64_t xmsiz){
  assert(ddb);
  if(ddb->open){
    tcddbsetecode(ddb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  return tchdbsetxmsiz(ddb->hdb, xmsiz);
}


/* Set the unit step number of auto defragmentation of a DSA tree database object. */
bool tcddbsetdfunit(TCDDB *ddb, int32_t dfunit){
  assert(ddb);
  if(ddb->open){
    tcddbsetecode(bdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  return tchdbsetdfunit(ddb->hdb, dfunit);
}

/* Load a node from page
 `page' specifies the page contains nodes.
 `index' specifies the index of node in list
 The return value is the node object or `NULL' on failure.
 */
static DDBNODE *tcddbnodeload(DDBPAGE *page, int64_t index) {
    assert(page);

    if (index < 0) {
        return NULL;
    }

    return TCPTRLISTVAL(page->nodes, index);
}

/* Create a new node.
 `ddb' specifies the DSA tree database object.
 The return value is the new node object. */

static DDBNODE *tcddbnodenew(TCDDB *ddb) {
    assert(bdb);
    DDBNODE *node;
    TCMALLOC(node,sizeof(DDBNODE));
    /*    node.id = ++ddb->nnum + DDBNODEIDBASE; */
    node->radius = 0;
    node->child.offset = DDBINVOFFSETID;
    node->child.pid = DDBINVPAGEID;
    node->sibling.offset = DDBINVOFFSETID;
    node->depth = 0;
    TCMALLOC(node->point,ddb->dimensions*sizeof(DDBCORD));
    node->time = time(NULL);
    return node;
}

static uint32_t tcddbnodesize(TCDDB *ddb,DDBNODE *node, uint32_t offset)
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

    for (int j = 0; j < ddb->dimensions; j++) {
        ulnum = node->point[j] + 1;
        size += TCCALCVNUMSIZE(ulnum);
    }

    return size;
}

/* Create a new page.
 `ddb' specifies the DSA tree database object.
 The return value is the new page object. */

static DDBPAGE *tcddbpagenew(TCDDB *ddb) {
    assert(bdb);
    DDBPAGE *page;
    TCMALLOC(page,sizeof(DDBPAGE));

    page->id = ++ddb->npage + DDBPAGEIDBASE;
    page->nodes = tcptrlistnew();
    page->size = 0;
    page->subtree_with_diff_parent_count = 0;

    /*    tcmapputkeep(ddb->pagec, &(page.id), sizeof(page.id), &page, sizeof(page));
     int rsiz;
     return (DDBPAGE *)tcmapget(ddb->pagec, &(page.id), sizeof(page.id), &rsiz);
     */
    return page;
}

/* Remove a page
 `ddb' specifies the DSA tree database object.
 `page' specifies the page object.
 If successful, the return value is true, else, it is false. */

static bool tcddbpageremove(TCDDB *ddb, DDBPAGE *page)
{
    assert(ddb && page);

    char static_buf[(sizeof(uint64_t) + 1) * 3];

    bool err = false;
    int step = sprintf(static_buf, "%llx", (unsigned long long) page->id);
    if (!tchdbout(ddb->hdb, static_buf, step) && tchdbecode(
            ddb->hdb) != TCENOREC)
        err = true;

    bool clk = DDBLOCKCACHE(ddb);
    tcmapout(ddb->pagec,&page->id, sizeof(page->id));
    if (clk)
        DDBUNLOCKCACHE(ddb);

    return err;
}

/* Save a page into the internal database.
 `ddb' specifies the DSA tree database object.
 `page' specifies the page object.
 If successful, the return value is true, else, it is false. */
static bool tcddbpagesave(TCDDB *ddb, DDBPAGE *page) {
    assert(ddb && page);
    TCXSTR *rbuf = tcxstrnew3(DDBPAGEBUFSIZ);

    char static_buf[(sizeof(uint64_t) + 1) * 3];
    char *dynamic_buf;
    char *wp = static_buf;

    int step;

    int64_t llnum;
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

    TCMALLOC(dynamic_buf,1 + size * ( 5 + ddb->dimensions )* sizeof(uint64_t));

    for (int i = 0; i < size; i++)
    {
        DDBNODE *node = tcddbnodeload(page,i);

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

        llnum = node->sibling.offset + 1;
        TCSETVNUMBUF64(step, wp, llnum);
        wp += step;

        llnum = node->child.pid + 1;
        TCSETVNUMBUF64(step, wp, llnum);
        wp += step;

        llnum = node->child.offset + 1;
        TCSETVNUMBUF64(step, wp, llnum);
        wp += step;

        for (int j = 0; j < ddb->dimensions; j++) {
            ulnum = node->point[j] + 1;
            TCSETVNUMBUF(step, wp, ulnum);
            wp += step;
        }

        TCXSTRCAT(rbuf, dynamic_buf, wp - dynamic_buf);
    }

    bool err = false;
    step = sprintf(static_buf, "%llx", (unsigned long long) page->id);
    if (size < 1 && !tchdbout(ddb->hdb, static_buf, step) && tchdbecode(
            ddb->hdb) != TCENOREC)
        err = true;

    page->size = TCXSTRSIZE(rbuf);

    if (!tchdbput(ddb->hdb, static_buf, step, TCXSTRPTR(rbuf), TCXSTRSIZE(rbuf)))
        err = true;
    tcxstrdel(rbuf);

    bool clk = DDBLOCKCACHE(ddb);
    tcmapputkeep(ddb->pagec, &page->id, sizeof(page->id), page, sizeof(*page));
    if (clk)
        DDBUNLOCKCACHE(ddb);

    return !err;
}

/* Load a page from the internal database.
 `ddb' specifies the DSA tree database object.
 `id' specifies the ID number of the page.
 The return value is the node object or `NULL' on failure.
 TODO: refactor this function, it's similar with tcddbget   */
static DDBPAGE *tcddbpageload(TCDDB *ddb, uint64_t id) {

    assert(ddb && id > DDBPAGEIDBASE);
    bool clk = DDBLOCKCACHE(ddb);
    int rsiz;
    // Get the page from cache if exists :
    DDBPAGE *page = (DDBPAGE *) tcmapget3(ddb->pagec, &id, sizeof(id), &rsiz);
    if (page)
    {
        if (clk)
            DDBUNLOCKCACHE(ddb);
        return page;
    }

    if (clk)
        DDBUNLOCKCACHE(ddb);
    //TCDODEBUG(ddb->cnt_loadnode++);

    // Not available in cache :
    char hbuf[(sizeof(uint64_t) + 1) * 2];
    int step;

    // Get the order of this node in list
    step = sprintf(hbuf, "%llx", (unsigned long long) (id));
    char *rbuf = NULL;
    char wbuf[DDBPAGEBUFSIZ];
    const char *rp = NULL;
    rsiz = tchdbget3(ddb->hdb, hbuf, step, wbuf, DDBPAGEBUFSIZ);

    if (rsiz < 1)
    { // If getting failed
        tcddbsetecode(ddb, TCEMISC, __FILE__, __LINE__, __func__);
        return NULL;
    } else if (rsiz < DDBPAGEBUFSIZ)
    { // Buffer size is big enough for the record
        rp = wbuf;
    } else { // The actual record size is larger than buffer size
        if (!(rbuf = tchdbget(ddb->hdb, hbuf, step, &rsiz))) {
            tcddbsetecode(ddb, TCEMISC, __FILE__, __LINE__, __func__);
            return NULL;
        }
        rp = rbuf;
    }

    page = tcddbpagenew(ddb);
    page->size = rsiz;

    int64_t llnum;
    uint64_t ullnum;
    uint32_t ulnum;
    int cur_index;
    int64_t size;

    page->id = id;

    /* get number of nodes */
    TCREADVNUMBUF(rp, llnum, step);
    size = llnum;
    rp += step;
    rsiz -= step;

    /* number of subtrees with different parents */
    TCREADVNUMBUF(rp, ulnum, step);
    page->subtree_with_diff_parent_count = ulnum;
    rp += step;
    rsiz -= step;
    bool err = false;

    for (int i = 0; i < size; i++)
    {
        DDBNODE *node = tcddbnodenew(ddb);

        TCREADVNUMBUF(rp, ulnum, step);
        cur_index = ulnum;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF64(rp, ullnum, step);
        node->time = ullnum - 1;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF(rp, ulnum, step);
        node->radius = ulnum - 1;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF64(rp, llnum, step);
        node->sibling.offset = llnum - 1;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF64(rp, llnum, step);
        node->child.pid = llnum - 1;
        rp += step;
        rsiz -= step;

        TCREADVNUMBUF64(rp, llnum, step);
        node->child.offset = llnum - 1;
        rp += step;
        rsiz -= step;

        for (int j = 0; j < ddb->dimensions; j++) {
            TCREADVNUMBUF(rp, ulnum, step);
            node->point[j] = ulnum - 1;
            rp += step;
            rsiz -= step;
        }

        while (tcptrlistnum(page->nodes) < cur_index)
        {
            tcptrlistpush(page->nodes, NULL);
        }
        tcptrlistpush(page->nodes, node);
    }

    if (err || rsiz != 0) {
        tcddbsetecode(ddb, TCEMISC, __FILE__, __LINE__, __func__);
        return NULL;
    }

    clk = DDBLOCKCACHE(ddb);
    tcmapputkeep(ddb->pagec, &id, sizeof(id), page, sizeof(*page));
    page = (DDBPAGE *) tcmapget(ddb->pagec, &id, sizeof(id), &rsiz);
    if (clk)
        DDBUNLOCKCACHE(ddb);
    return page;
}

/* Load a node from the internal database.
 `ddb' specifies the DSA tree database object.
 `id' specifies the ID number of the node.
 The return value is the node object or `NULL' on failure.
 */
static const void *tcddbget(TCDDB *ddb, const DDBCORD *kbuf, uint64_t ksiz) {
    assert(ddb && id > DDBNODEIDBASE);
    bool clk = DDBLOCKCACHE(ddb);
    int rsiz;

    // Get the node from cache if exists :

    const void *node = tcmapget3(ddb->valuec, kbuf, ksiz, &rsiz);
    if (node) {
        if (clk)
            DDBUNLOCKCACHE(ddb);
        return node;
    }

    if (clk)
        DDBUNLOCKCACHE(ddb);
    //TCDODEBUG(ddb->cnt_loadnode++);

    // Not available in cache :

    char *rbuf = NULL;
    char wbuf[DDBPAGEBUFSIZ];
    const char *rp = NULL;

    // Get record and write to buffer ( wbuf )
    rsiz = tchdbget3(ddb->hdb, kbuf, ksiz, wbuf, DDBPAGEBUFSIZ);
    if (rsiz < 1) { // If getting failed
        tcddbsetecode(ddb, TCEMISC, __FILE__, __LINE__, __func__);
        return NULL;
    } else if (rsiz < DDBPAGEBUFSIZ) { // Buffer size is big enough for the record
        rp = wbuf;
    } else { // The actual record size is larger than buffer size
        if (!(rbuf = tchdbget(ddb->hdb, kbuf, ksiz, &rsiz))) {
            tcddbsetecode(ddb, TCEMISC, __FILE__, __LINE__, __func__);
            return NULL;
        }
        rp = rbuf;
    }

    clk = DDBLOCKCACHE(ddb);
    tcmapputkeep(ddb->valuec, kbuf, ksiz, rp, rsiz);
    node = tcmapget(ddb->valuec, kbuf, ksiz, &rsiz);
    if (clk)
        DDBUNLOCKCACHE(ddb);
    return node;
}

static const char *tcddbrangesearch(TCDDB *ddb, DDBNODE *elem,
        const void *kbuf, int64_t ksiz, int64_t r, time_t t) {
    DDBDIST dp, min_dist;
    DDBNODE *sibling;
    int32_t child_offset, sibling_offset;
    time_t t1;

    dp = dist(ddb->dimensions, (DDBCORD*) kbuf, elem->point);

    /*    printf("parent node %d\n",dp);
     printf("radius %d\n",r);
     */
    if ((elem->time <= t) && (dp <= elem->radius + r))
    {
        if (dp <= r) {
            return tcddbget(ddb, elem->point, ksiz);
        }

        min_dist = DDBMAXDIST;
        DDBPAGE *page = tcddbpageload(ddb, elem->child.pid);
        child_offset = elem->child.offset;

        while (child_offset != DDBINVOFFSETID)
        {
            DDBNODE *node = tcddbnodeload(page, child_offset);

            /* TODO: Avoid calling dist function by using dist array */
            dp = dist(ddb->dimensions, (DDBCORD*) kbuf, node->point);

            if (dp <= min_dist + 2* r ) {
                /* BEGIN Get smallest t from its next siblings */
                t1 = t;
                sibling_offset = node->sibling.offset;
                while (sibling_offset != DDBINVOFFSETID) {
                    sibling = tcddbnodeload(page, sibling_offset);
                    if ((sibling->time <= t1) && (dp > dist(ddb->dimensions,
                            (DDBCORD*) kbuf, sibling->point) + 2* r )) {
                        t1 = sibling->time;
                    }
                    sibling_offset = sibling->sibling.offset;
                }
                /* END */

                const char *vbuf = tcddbrangesearch(ddb, node, kbuf, ksiz, r,
                        t1);
                if (vbuf != NULL) {
                    return vbuf;
                }

                min_dist = MIN(min_dist,dp);
            }

            child_offset = node->sibling.offset;
        }
    }

    return NULL;
}

/* Retrieve a record in a DSA tree database object.
 `ddb' specifies the DSA tree database object.
 `kbuf' specifies the pointer to the region of the key.
 `ksiz' specifies the size of the region of the key.
 `sp' specifies the pointer to the variable into which the size of the region of the return
 value is assigned.
 If successful, the return value is the pointer to the region of the value of the corresponding
 record. */
static const char *tcddbsearchimpl(TCDDB *ddb, const DDBCORD *kbuf,
        int64_t ksiz, int64_t r) {
    assert(ddb && kbuf && ksiz >= 0 && sp);

    /* Try to get directly from hash database */
    const char *vbuf = tcddbget(ddb, kbuf, ksiz);

    if (vbuf == NULL)
    {
        time_t t = time(NULL);
        DDBPAGE *page = tcddbpageload(ddb, ddb->root_pid);
        DDBNODE *elem = tcddbnodeload(page, ddb->root_offset);
        vbuf = tcddbrangesearch(ddb, elem, kbuf, ksiz, r, t);
    }

    return vbuf;
}

static int tcddbinsertnode(DDBPAGE *page,DDBNODE *node)
{
    int idx = 0;
    while (idx < tcptrlistnum(page->nodes))
    {
        if (tcddbnodeload(page,idx) == NULL)
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
        tcptrlistinsert(page->nodes,idx,node);
    }

    return idx;
}

/* Store a record into a DSA tree database object.
 `ddb' specifies the DSA tree database object.
 `kbuf' specifies the pointer to the region of the key.
 `ksiz' specifies the size of the region of the key.
 `vbuf' specifies the pointer to the region of the value.
 `vsiz' specifies the size of the region of the value.
 `dmode' specifies behavior when the key overlaps.
 If successful, the return value is true, else, it is false. */
static bool tcddbputimpl(TCDDB *ddb, const void *kbuf, int ksiz,
        const void *vbuf, int vsiz, int dmode) {
    assert(ddb && kbuf && ksiz >= 0);

    int64_t pid = ddb->root_pid;
    int64_t root_offset = ddb->root_offset;

    /* Initialize the node */
    DDBNODE *node = tcddbnodenew(ddb);

    int i = 0;
    DDBCORD *ktemp = (DDBCORD*) kbuf;
    for (; i < ddb->dimensions; i++) {
        node->point[i] = ktemp[i];
    }

    tchdbput(ddb->hdb, kbuf, ksiz, vbuf, vsiz);

    /* If the tree is empty */
    if (ddb->root_pid == DDBINVPAGEID) {
        DDBPAGE *page = tcddbpagenew(ddb);
        tcddbinsertnode(page,node);
        tcddbpagesave(ddb, page);
        /* This node is the root */
        ddb->root_pid = page->id;
        ddb->root_offset = tcptrlistnum(page->nodes) - 1;
    }
    else
    {
        /* Get the root node */
        DDBPAGE *page = tcddbpageload(ddb, pid);
        DDBPAGE *parent_page = NULL;
        DDBNODE *elem = tcddbnodeload(page, root_offset);
        DDBNODE *child;
        DDBNODE *candidate;
        DDBDIST min_dist, child_dist;
        uint16_t nchild;
        uint32_t n_size;

        int64_t first_node_offset = DDBINVOFFSETID;
        DDBNODE *first_node_parent = NULL;

        /* Traverse through its neighbors  */
        DDBDIST dp = dist(ddb->dimensions, elem->point, node->point);
        while (1)
        {
            min_dist = DDBMAXDIST;
            candidate = NULL;
            child = elem;
            nchild = 0;

            elem->radius = MAX(elem->radius,dp);

            n_size = 0;
            if (elem->child.pid != DDBINVPAGEID)
            {
                /* this trick reduces number of times to load page by checking if the page is loaded or not */
                parent_page = page;
                if (!page || (pid != elem->child.pid))
                {
                    page = tcddbpageload(ddb, elem->child.pid);
                    first_node_offset = DDBINVOFFSETID;
                }

                int32_t child_offset = elem->child.offset;
                if (first_node_offset == DDBINVOFFSETID)
                {
                    first_node_offset = child_offset;
                    first_node_parent = elem;
                }

                // traverse all the child node
                while (child_offset != DDBINVOFFSETID)
                {
                    nchild++;
                    child = tcddbnodeload(page, child_offset);
                    child_dist = dist(ddb->dimensions, child->point,
                            node->point);
                    if (child_dist < min_dist) {
                        min_dist = child_dist;
                        candidate = child;
                    }

                    n_size += tcddbnodesize(ddb,child,child_offset);

                    child_offset = child->sibling.offset;
                }
            }

            if ((dp < min_dist) && (nchild < ddb->arity))
            {
                // insert node to page

                int idx = tcddbinsertnode(page,node);

                /* Insert as a new child */
                if (child == elem)
                {
                    elem->child.pid = page->id;
                    elem->child.offset = idx;
                    printf("Insert as a child of %c \n",child->point[0]);
                }
                /* Insert as a new sibling */
                else
                {
                    child->sibling.offset = idx;
                    printf("Insert as a sibling of %c \n",child->point[0]);
                }

                tcddbpagesave(ddb, page);

                n_size += tcddbnodesize(ddb,node,idx);

                if (page->size >= DDBMAXDISKPAGESIZE)
                {
                    /* move to parent */
                    if ( (parent_page->id != page->id) && (parent_page->size + n_size < DDBMAXDISKPAGESIZE))
                    {
                        int64_t child_offset = elem->child.offset;

                        // get from current page
                        child = tcddbnodeload(page, child_offset);

                        // insert to parent page
                        int64_t idx = tcddbinsertnode(parent_page,child);

                        // remove from current page
                        tcptrlistover(page->nodes,child_offset,NULL);

                        // re-set far pointer
                        elem->child.pid = parent_page->id;
                        elem->child.offset = idx;

                        child_offset = child->sibling.offset;

                        // copy from page to parent_page
                        while (child_offset != DDBINVOFFSETID)
                        {
                            // get from current page
                            DDBNODE *temp = tcddbnodeload(page, child_offset);

                            // insert to parent page
                            int64_t idx = tcddbinsertnode(parent_page,temp);

                            // remove from current page
                            tcptrlistover(page->nodes,child_offset,NULL);

                            // update previous sibling
                            child->sibling.offset = idx;

                            child = temp;
                            child_offset = child->sibling.offset;
                        }


                        tcddbpagesave(ddb, parent_page);
                    }
                    /* vertical split */
                    else if (page->subtree_with_diff_parent_count > 1)
                    {
                        int64_t added_queue[DDBMAXNODECOUNT];
                        int first = 0;
                        int last = 0;

                        DDBPAGE *new_page = tcddbpagenew(ddb);

                        node = tcddbnodeload(page, first_node_offset);
                        int64_t new_idx = tcddbinsertnode(new_page,node);

                        first_node_parent->child.pid = new_page->id;
                        first_node_parent->child.offset = new_idx;
                        added_queue[last++] = new_idx;

                        tcptrlistover(page->nodes,first_node_offset,NULL);

                        while (last > first)
                        {
                            int idx = added_queue[first++];

                            // get node from new page
                            node = tcddbnodeload(new_page, idx);

                            // check next sibling
                            if ( (node->sibling.offset != DDBINVOFFSETID) )
                            {
                                DDBNODE *temp = tcddbnodeload(page, node->sibling.offset);

                                // insert to parent page
                                int new_idx = tcddbinsertnode(new_page,temp);

                                // update
                                node->sibling.offset = new_idx;

                                // add to queue
                                added_queue[last++] = new_idx;
                                tcptrlistover(page->nodes,node->sibling.offset,NULL);
                            }

                            if ( (node->child.pid == page->id))
                            {
                                DDBNODE *temp = tcddbnodeload(page, node->child.offset);

                                // insert to parent page
                                int new_idx = tcddbinsertnode(new_page,temp);

                                node->child.pid = new_page->id;
                                node->child.offset = new_idx;

                                // add to queue
                                added_queue[last++] = new_idx;
                                tcptrlistover(page->nodes,node->child.offset,NULL);
                            }
                        }

                        page->subtree_with_diff_parent_count--;
                        new_page->subtree_with_diff_parent_count = 1;

                        tcddbpageremove(ddb,page);
                        tcddbpagesave(ddb,new_page);
                    }
                    else  /* Horizontal split */
                    {
                        int i;

                        bool is_parent[DDBMAXNODECOUNT];
                        memset(is_parent,1,DDBMAXNODECOUNT*sizeof(bool));

                        /* Traverse all the nodes in page to
                            detect the parents of subtrees in this page
                            */
                        for (i = 0; i < tcptrlistnum(page->nodes); i++)
                        {
                            DDBNODE *node = tcddbnodeload(page,i);

                            if (node == NULL) continue;

                            if ( (node->child.pid == page->id) && (node->child.offset != DDBINVOFFSETID) )
                            {
                                is_parent[node->child.offset] = false;
                            }

                            if (node->sibling.offset != DDBINVOFFSETID)
                            {
                                is_parent[node->sibling.offset] = false;
                            }
                        }

                        int64_t queue[DDBMAXNODECOUNT];
                        int first = 0;
                        int last = 0;

                        int depth[DDBMAXNODECOUNT];
                        int size[DDBMAXNODECOUNT];

                        memset(depth,0,DDBMAXNODECOUNT*sizeof(int));
                        memset(size,0,DDBMAXNODECOUNT*sizeof(int));

                        for (i = 0; i < tcptrlistnum(page->nodes); i++)
                        {
                            DDBNODE *node = tcddbnodeload(page,i);
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
                            DDBNODE *node = tcddbnodeload(page,idx);
                            size[depth[idx]] += tcddbnodesize(ddb,node,idx);

//                              printf("-- %lld : depth = %d\n",idx,depth[idx]);
                            if ( (node->child.pid == page->id) && (node->child.offset != DDBINVOFFSETID) )
                            {
                                queue[last++] = node->child.offset;
                                depth[node->child.offset] = depth[idx] + 1;
//                                printf("++ child %lld\n",node->child.offset);
                            }

                            if (node->sibling.offset != DDBINVOFFSETID)
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
                            if (total_size >= DDBMAXDISKPAGESIZE / 2)
                            {
                                break;
                            }
                        }

                        if (size[d] == 0)
                        {
                            d--;
                        }

                        int64_t added_queue[DDBMAXNODECOUNT];

                        int qsize = last;

                        first = 0;
                        last = 0;
                        DDBPAGE *new_page = tcddbpagenew(ddb);

                        int parent_node_count = 0;

                        /* We traverse all the nodes of depth d-1 to add initial nodes*/
                        for (i = 0; i < qsize; i++)
                        {
                            int64_t idx = queue[i];
                            if (depth[idx] == d - 1)
                            {
                                // get current node
                                node = tcddbnodeload(page, idx);
                                if ( (node->child.pid == page->id))
                                {
                                     parent_node_count++;

                                     DDBNODE *temp = tcddbnodeload(page, node->child.offset);

                                     // insert to parent page
                                     int new_idx = tcddbinsertnode(new_page,temp);

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
                            node = tcddbnodeload(new_page, idx);

                            // check next sibling
                            if ( (node->sibling.offset != DDBINVOFFSETID) )
                            {
                                DDBNODE *temp = tcddbnodeload(page, node->sibling.offset);

                                // insert to parent page
                                int new_idx = tcddbinsertnode(new_page,temp);

                                // add to queue
                                added_queue[last++] = new_idx;

                                tcptrlistover(page->nodes,node->sibling.offset,NULL);
                                node->sibling.offset = new_idx;
                            }

                            if ( (node->child.pid == page->id))
                            {
                                DDBNODE *temp = tcddbnodeload(page, node->child.offset);

                                // insert to parent page
                                int new_idx = tcddbinsertnode(new_page,temp);

                                // add to queue
                                added_queue[last++] = new_idx;

                                tcptrlistover(page->nodes,node->child.offset,NULL);
                                node->child.pid = new_page->id;
                                node->child.offset = new_idx;
                            }
                        }

                        new_page->subtree_with_diff_parent_count = parent_node_count;

                        tcddbpagesave(ddb,page);
                        tcddbpagesave(ddb,new_page);
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
 `ddb' specifies the DSA tree database object.
 `path' specifies the path of the internal database file.
 `omode' specifies the connection mode.
 If successful, the return value is true, else, it is false.

 TODO: Need more comments
 */

static bool tcddbopenimpl(TCDDB *ddb, const char *path, int omode) {
    assert(ddb && path);
    int homode = HDBOREADER;
    if (omode & DDBOWRITER) {
        homode = HDBOWRITER;
        if (omode & DDBOCREAT)
            homode |= HDBOCREAT;
        if (omode & DDBOTRUNC)
            homode |= HDBOTRUNC;
        ddb->wmode = true;
    } else {
        ddb->wmode = false;
    }
    if (omode & DDBONOLCK)
        homode |= HDBONOLCK;
    if (omode & DDBOLCKNB)
        homode |= HDBOLCKNB;
    if (omode & DDBOTSYNC)
        homode |= HDBOTSYNC;
    tchdbsettype(ddb->hdb, TCDBTBTREE);
    if (!tchdbopen(ddb->hdb, path, homode))
        return false;
    ddb->nnum = 0;
    ddb->npage = 0;
    ddb->valuec = tcmapnew2(ddb->vcnum * 2 + 1);
    ddb->pagec = tcmapnew2(ddb->pcnum * 2 + 1);
    ddb->open = true;
    // TODO : getmetadata of tree
    return true;
}

/* Clear all members.
 `ddb' specifies the DSA tree database object. */
static void tcddbclear(TCDDB *ddb) {
    assert(ddb);
    ddb->hdb = NULL;
    ddb->open = false;
    ddb->wmode = false;
    ddb->root_pid = DDBINVPAGEID;
    ddb->root_offset = DDBINVOFFSETID;
    ddb->nnum = 0;
    ddb->vcnum = DDBDEFVCNUM;
    ddb->pcnum = DDBDEFPCNUM;
}

/* Close a DSA tree database object.
 `ddb' specifies the B+ tree database object.
 If successful, the return value is true, else, it is false. */
static bool tcddbcloseimpl(TCDDB *ddb) {
    assert(ddb);
    bool err = false;
    ddb->open = false;
    if (!tchdbclose(ddb->hdb))
        err = true;
    return !err;
}

/* Delete a DSA tree database object. */
void tcddbdel(TCDDB *ddb){
  assert(ddb);
  if(ddb->open) tcddbclose(ddb);
  tchdbdel(ddb->hdb);
  if(ddb->mmtx){
    pthread_mutex_destroy(ddb->cmtx);
    pthread_rwlock_destroy(ddb->mmtx);
    TCFREE(ddb->cmtx);
    TCFREE(ddb->mmtx);
  }
  TCFREE(ddb);
}

/*************************************************************************************************
 * API
 *************************************************************************************************/

/* Get the last happened error code of a B+ tree database object. */
int tcddbecode(TCDDB *ddb) {
    assert(ddb);
    return tchdbecode(ddb->hdb);
}

/* Get the message string corresponding to an error code. */
const char *tcddberrmsg(int ecode) {
    return tcerrmsg(ecode);
}

TCDDB *tcddbnew(void) {
    TCDDB *ddb;
    TCMALLOC(ddb, sizeof(*ddb));
    tcddbclear(ddb);
    ddb->hdb = tchdbnew();
    ddb->nnum = 0;
    ddb->npage = 0;
    return ddb;
}

/* Open a database file and connect a DSA tree database object. */
bool tcddbopen(TCDDB *ddb, const char *path, int omode) {
    assert(ddb && path);
    if (!DDBLOCKMETHOD(ddb, true))
        return false;
    if (ddb->open) {
        tcddbsetecode(ddb, TCEINVALID, __FILE__, __LINE__, __func__);
        DDBUNLOCKMETHOD(ddb);
        return false;
    }
    bool rv = tcddbopenimpl(ddb, path, omode);
    DDBUNLOCKMETHOD(ddb);
    return rv;
}

/* Store a record into a DSA tree database object. */
bool tcddbput(TCDDB *ddb, const void *kbuf, int ksiz, const void *vbuf,
        int vsiz) {
    assert(ddb && kbuf && ksiz >= 0 && vbuf && vsiz >= 0);
    if (!DDBLOCKMETHOD(ddb, true))
        return false;
    /*  if(!ddb->open || !ddb->wmode){
     tcddbsetecode(ddb, TCEINVALID, __FILE__, __LINE__, __func__);
     DDBUNLOCKMETHOD(ddb);
     return false;
     }
     */
    bool rv = tcddbputimpl(ddb, kbuf, ksiz, vbuf, vsiz, DDBPDOVER);
    DDBUNLOCKMETHOD(ddb);
    return rv;
}

/* Store a string record into a DSA tree database object. */
bool tcddbput2(TCDDB *ddb, const char *str, const char *vstr) {
    assert(ddb && kstr && vstr);
    DDBCORD *kstr;
    TCMALLOC(kstr, strlen(str)*sizeof(DDBCORD));
    for (int i = 0; i < strlen(str); i++) {
        kstr[i] = str[i];
    }
    return tcddbput(ddb, kstr, strlen(str) * sizeof(DDBCORD), vstr,
            strlen(vstr));
}

/* Search for a record in a DSAT tree database object. */
void *tcddbsearch(TCDDB *ddb, const void *kbuf, int ksiz, int64_t r) {
    assert(ddb && kbuf && ksiz >= 0);
    if (!DDBLOCKMETHOD(ddb, false))
        return NULL;
    /*  if(!ddb->open){
     tcddbsetecode(ddb, TCEINVALID, __FILE__, __LINE__, __func__);
     DDBUNLOCKMETHOD(ddb);
     return NULL;
     }
     */
    const char *vbuf = tcddbsearchimpl(ddb, kbuf, ksiz, r);
    char *rv;

    if (vbuf) {
        TCMEMDUP(rv, vbuf, strlen(vbuf));
    } else {
        rv = NULL;
    }

    DDBUNLOCKMETHOD(ddb);
    return rv;
}

/* Search for a record with key as string in a DSAT tree database object. */
void *tcddbsearch2(TCDDB *ddb, const char *kbuf, int64_t r) {
    assert(ddb && kbuf && ksiz >= 0);
    if (!DDBLOCKMETHOD(ddb, false))
        return NULL;

    DDBCORD *kstr;
    TCMALLOC(kstr, strlen(kbuf)*sizeof(DDBCORD));
    for (int i = 0; i < strlen(kbuf); i++) {
        kstr[i] = kbuf[i];
    }

    return tcddbsearch(ddb, kstr, strlen(kbuf) * sizeof(DDBCORD), r);
}

/* Close a DSA tree database object. */
bool tcddbclose(TCDDB *ddb) {
    assert(ddb);
    if (!DDBLOCKMETHOD(ddb, true))
        return false;
    /*  if(!ddb->open){
     tcbdbsetecode(bdb, TCEINVALID, __FILE__, __LINE__, __func__);
     BDBUNLOCKMETHOD(bdb);
     return false;
     }
     */
    bool rv = tcddbcloseimpl(ddb);
    DDBUNLOCKMETHOD(ddb);
    return rv;
}

/* END OF FILE */
