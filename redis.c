/**
 * REDIS
 */

#include "config.h"

#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <sys/errno.h>
#include <fcntl.h>
#include <signal.h>
#include <execinfo.h>
#include <assert.h>

#include "zmalloc.h"
#include "dict.h"
#include "adlist.h"
#include "anet.h"
#include "ae.h"
#include "sds.h"
#include "zipmap.h"

#define REDIS_VERSION "1.3.6"

/* Error codes */
#define REDIS_OK                0
#define REDIS_ERR               -1


/* 静态服务器配置 */
/* Static server configuration */
#define REDIS_SERVERPORT        6379    /* TCP port */
#define REDIS_MAXIDLETIME       (60*5)  /* 客户端默认超时时间 */
#define REDIS_IOBUF_LEN         1024
#define REDIS_LOADBUF_LEN       1024
#define REDIS_STATIC_ARGS       4
#define REDIS_DEFAULT_DBNUM     16          /* redis数据库默认数量 */
#define REDIS_CONFIGLINE_MAX    1024        /* 配置文件最大行数 */
#define REDIS_OBJFREELIST_MAX   1000000 /* Max number of objects to cache */
#define REDIS_MAX_SYNC_TIME     60      /* Slave can't take more to sync */
#define REDIS_EXPIRELOOKUPS_PER_CRON    100 /* try to expire 100 keys/second */
#define REDIS_MAX_WRITE_PER_EVENT (1024*64)
#define REDIS_REQUEST_MAX_SIZE (1024*1024*256) /* max bytes in inline command */

/* Hash table parameters */
#define REDIS_HT_MINFILL        10      /* Minimal hash table fill 10% */


/* 对象类型 */
#define REDIS_STRING 0
#define REDIS_LIST 1
#define REDIS_SET 2
#define REDIS_ZSET 3
#define REDIS_HASH 4


/* 对象编码
 * 某些类型的对象(如字符串和散列值)可以在内部以多种方式表示。
 * 对象的` encoding `字段被设置为该对象的其中一个字段。
 */
#define REDIS_ENCODING_RAW 0    /* Raw representation */
#define REDIS_ENCODING_INT 1    /* Encoded as integer */
#define REDIS_ENCODING_ZIPMAP 2 /* Encoded as zipmap */
#define REDIS_ENCODING_HT 3     /* Encoded as an hash table */


/* 对象存储位置标识 */
#define REDIS_VM_MEMORY 0       /* The object is on memory */
#define REDIS_VM_SWAPPED 1      /* The object is on disk */
#define REDIS_VM_SWAPPING 2     /* Redis is swapping this object on disk */
#define REDIS_VM_LOADING 3      /* Redis is loading this object from disk */


/* Client flags */
#define REDIS_SLAVE 1       /* This client is a slave server */
#define REDIS_MASTER 2      /* This client is a master server */
#define REDIS_MONITOR 4     /* This client is a slave monitor, see MONITOR */
#define REDIS_MULTI 8       /* This client is in a MULTI context */
#define REDIS_BLOCKED 16    /* The client is waiting in a blocking operation */
#define REDIS_IO_WAIT 32    /* The client is waiting for Virtual Memory I/O */

/* 从节点复制状态标识 */
#define REDIS_REPL_NONE 0   /* No active replication */
#define REDIS_REPL_CONNECT 1    /* Must connect to master */
#define REDIS_REPL_CONNECTED 2  /* Connected to master */



/* 日志级别 */
#define REDIS_DEBUG 0
#define REDIS_VERBOSE 1
#define REDIS_NOTICE 2
#define REDIS_WARNING 3

/* Anti-warning macro... */
#define REDIS_NOTUSED(V) ((void) V)

/* AOF默认级别 */
#define APPENDFSYNC_NO 0
#define APPENDFSYNC_ALWAYS 1
#define APPENDFSYNC_EVERYSEC 2

/* Hashes 相关默认值 */
#define REDIS_HASH_MAX_ZIPMAP_ENTRIES 64
#define REDIS_HASH_MAX_ZIPMAP_VALUE 512

/* We can print the stacktrace, so our assert is defined this way: */
#define redisAssert(_e) ((_e)?(void)0 : (_redisAssert(#_e,__FILE__,__LINE__),_exit(1)))
static void _redisAssert(char *estr, char *file, int line);


/*================================= 数据类型 ============================== */

/* A redis object, that is a type able to hold a string / list / set */

/* The VM object structure */
struct redisObjectVM {
    off_t page;         /* the page at witch the object is stored on disk */
    off_t usedpages;    /* number of pages used on disk */
    time_t atime;       /* Last access time */
} vm;

/* The actual Redis Object */
typedef struct redisObject {
    void *ptr;
    unsigned char type;
    unsigned char encoding;
    unsigned char storage;  /* If this object is a key, where is the value?
                             * REDIS_VM_MEMORY, REDIS_VM_SWAPPED, ... */
    unsigned char vtype; /* If this object is a key, and value is swapped out,
                          * this is the type of the swapped out object. */
    int refcount;
    /* VM fields, this are only allocated if VM is active, otherwise the
     * object allocation function will just allocate
     * sizeof(redisObjct) minus sizeof(redisObjectVM), so using
     * Redis without VM active will not have any overhead. */
    struct redisObjectVM vm;
} robj;

/**
 * redisDb用于标识一个数据库
 */
typedef struct redisDb {
    dict *dict;                 /* 数据库的键空间 */
    dict *expires;              /* 设置了超时时间的KEY及其超时时间 */
    dict *blockingkeys;         /* 客户端等待操作的数据 */
    dict *io_keys;              /* 客户端等待系统IO的数据*/
    int id;                     /* 数据库的ID */
} redisDb;

/* Client MULTI/EXEC state */
typedef struct multiCmd {
    robj **argv;
    int argc;
    struct redisCommand *cmd;
} multiCmd;

typedef struct multiState {
    multiCmd *commands;     /* Array of MULTI commands */
    int count;              /* Total number of MULTI commands */
} multiState;

/**
 * 客户端全局状态结构体
 */
typedef struct redisClient {
    int fd;
    redisDb *db;
    int dictid;
    sds querybuf;
    robj **argv, **mbargv;
    int argc, mbargc;
    int bulklen;            /* bulk read len. -1 if not in bulk read mode */
    int multibulk;          /* multi bulk command format active */
    list *reply;
    int sentlen;
    time_t lastinteraction; /* time of the last interaction, used for timeout */
    int flags;              /* REDIS_SLAVE | REDIS_MONITOR | REDIS_MULTI ... */
    int slaveseldb;         /* slave selected db, if this client is a slave */
    int authenticated;      /* when requirepass is non-NULL */
    int replstate;          /* replication state if this is a slave */
    int repldbfd;           /* replication DB file descriptor */
    long repldboff;         /* replication DB file offset */
    off_t repldbsize;       /* replication DB file size */
    multiState mstate;      /* MULTI/EXEC state */
    robj **blockingkeys;    /* The key we are waiting to terminate a blocking
                             * operation such as BLPOP. Otherwise NULL. */
    int blockingkeysnum;    /* Number of blocking keys */
    time_t blockingto;      /* Blocking operation timeout. If UNIX current time
                             * is >= blockingto then the operation timed out. */
    list *io_keys;          /* Keys this client is waiting to be loaded from the
                             * swap file in order to continue. */
} redisClient;

struct saveparam {
    time_t seconds;
    int changes;
};

/**
 * 服务器全局全局状态结构体
 *
 * 用于表示整个Redis服务器的结构体，它包含了各种属性和信息，用于描述服务器的状态、配置和运行时数据。
 */
struct redisServer {
    int port;
    int fd;
    redisDb *db;
    dict *sharingpool;          /* Poll used for object sharing - 弹出对象共享池 */
    unsigned int sharingpoolsize;
    long long dirty;            /* changes to DB from the last save - 最后一修改的数据库 */
    list *clients;
    list *slaves, *monitors;
    char neterr[ANET_ERR_LEN];
    aeEventLoop *el;            /* 事件处理器 */
    int cronloops;              /* number of times the cron function run */
    list *objfreelist;          /* A list of freed objects to avoid malloc() */
    time_t lastsave;            /* Unix time of last save succeeede */
    /* Fields used only for stats */
    time_t stat_starttime;         /* server start time */
    long long stat_numcommands;    /* number of processed commands */
    long long stat_numconnections; /* number of connections received */
    /* Configuration */
    int verbosity;
    int glueoutputbuf;
    int maxidletime;
    int dbnum;
    int daemonize;
    int appendonly;
    int appendfsync;
    time_t lastfsync;
    int appendfd;
    int appendseldb;
    char *pidfile;
    pid_t bgsavechildpid;
    pid_t bgrewritechildpid;
    sds bgrewritebuf; /* buffer taken by parent during oppend only rewrite */
    struct saveparam *saveparams;
    int saveparamslen;
    char *logfile;
    char *bindaddr;
    char *dbfilename;
    char *appendfilename;
    char *requirepass;
    int shareobjects;
    int rdbcompression;
    /* Replication related */
    int isslave;
    char *masterauth;
    char *masterhost;
    int masterport;
    redisClient *master;    /* client that is master for this slave */
    int replstate;
    unsigned int maxclients;
    unsigned long long maxmemory;
    unsigned int blpop_blocked_clients;
    unsigned int vm_blocked_clients;
    /* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    /* Virtual memory configuration */
    int vm_enabled;
    char *vm_swap_file;
    off_t vm_page_size;
    off_t vm_pages;
    unsigned long long vm_max_memory;
    /* Hashes config */
    size_t hash_max_zipmap_entries;
    size_t hash_max_zipmap_value;
    /* Virtual memory state */
    FILE *vm_fp;
    int vm_fd;
    off_t vm_next_page; /* Next probably empty page */
    off_t vm_near_pages; /* Number of pages allocated sequentially */
    unsigned char *vm_bitmap; /* Bitmap of free/used pages */
    time_t unixtime;    /* Unix time sampled every second. */
    /* Virtual memory I/O threads stuff */
    /* An I/O thread process an element taken from the io_jobs queue and
     * put the result of the operation in the io_done list. While the
     * job is being processed, it's put on io_processing queue. */
    list *io_newjobs; /* List of VM I/O jobs yet to be processed */
    list *io_processing; /* List of VM I/O jobs being processed */
    list *io_processed; /* List of VM I/O jobs already processed */
    list *io_ready_clients; /* Clients ready to be unblocked. All keys loaded */
    pthread_mutex_t io_mutex; /* lock to access io_jobs/io_done/io_thread_job */
    pthread_mutex_t obj_freelist_mutex; /* safe redis objects creation/free */
    pthread_mutex_t io_swapfile_mutex; /* So we can lseek + write */
    pthread_attr_t io_threads_attr; /* attributes for threads creation */
    int io_active_threads; /* Number of running I/O threads */
    int vm_max_threads; /* Max number of I/O threads running at the same time */
    /* Our main thread is blocked on the event loop, locking for sockets ready
     * to be read or written, so when a threaded I/O operation is ready to be
     * processed by the main thread, the I/O thread will use a unix pipe to
     * awake the main thread. The followings are the two pipe FDs. */
    int io_ready_pipe_read;
    int io_ready_pipe_write;
    /* Virtual memory stats */
    unsigned long long vm_stats_used_pages;
    unsigned long long vm_stats_swapped_objects;
    unsigned long long vm_stats_swapouts;
    unsigned long long vm_stats_swapins;
    FILE *devnull;
};

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    dict *dict;
    zskiplist *zsl;
} zset;

/* Our shared "common" objects */

struct sharedObjectsStruct {
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *pong, *space,
            *colon, *nullbulk, *nullmultibulk, *queued,
            *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
            *outofrangeerr, *plus,
            *select0, *select1, *select2, *select3, *select4,
            *select5, *select6, *select7, *select8, *select9;
} shared;

/* 实际用作常量的全局变量。下面的double值用于磁盘上的双序列化，并在运行时初始化，以避免奇怪的编译器优化。 */

static double R_Zero, R_PosInf, R_NegInf, R_Nan;

/* VM threaded I/O request message */
#define REDIS_IOJOB_LOAD 0          /* Load from disk to memory */
#define REDIS_IOJOB_PREPARE_SWAP 1  /* Compute needed pages */
#define REDIS_IOJOB_DO_SWAP 2       /* Swap from memory to disk */
typedef struct iojob {
    int type;   /* Request type, REDIS_IOJOB_* */
    redisDb *db;/* Redis database */
    robj *key;  /* This I/O request is about swapping this key */
    robj *val;  /* the value to swap for REDIS_IOREQ_*_SWAP, otherwise this
                 * field is populated by the I/O thread for REDIS_IOREQ_LOAD. */
    off_t page; /* Swap page where to read/write the object */
    off_t pages; /* Swap pages needed to safe object. PREPARE_SWAP return val */
    int canceled; /* True if this command was canceled by blocking side of VM */
    pthread_t thread; /* ID of the thread processing this entry */
} iojob;

/*================================ Prototypes =============================== */

static void freeStringObject(robj *o);
static void freeListObject(robj *o);
static void freeSetObject(robj *o);
static void decrRefCount(void *o);
static robj *createObject(int type, void *ptr);
static void freeClient(redisClient *c);
static int rdbLoad(char *filename);
static void addReply(redisClient *c, robj *obj);
static void addReplySds(redisClient *c, sds s);
static void incrRefCount(robj *o);
static int rdbSaveBackground(char *filename);
static robj *createStringObject(char *ptr, size_t len);
static robj *dupStringObject(robj *o);
static void replicationFeedSlaves(list *slaves, struct redisCommand *cmd, int dictid, robj **argv, int argc);
static void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc);
static int syncWithMaster(void);
static robj *tryObjectSharing(robj *o);
static int tryObjectEncoding(robj *o);
static robj *getDecodedObject(robj *o);
static int removeExpire(redisDb *db, robj *key);
static int expireIfNeeded(redisDb *db, robj *key);
static int deleteIfVolatile(redisDb *db, robj *key);
static int deleteIfSwapped(redisDb *db, robj *key);
static int deleteKey(redisDb *db, robj *key);
static time_t getExpire(redisDb *db, robj *key);
static int setExpire(redisDb *db, robj *key, time_t when);
static void updateSlavesWaitingBgsave(int bgsaveerr);
static void freeMemoryIfNeeded(void);
static int processCommand(redisClient *c);
static void setupSigSegvAction(void);
static void rdbRemoveTempFile(pid_t childpid);
static void aofRemoveTempFile(pid_t childpid);
static size_t stringObjectLen(robj *o);
static void processInputBuffer(redisClient *c);
static zskiplist *zslCreate(void);
static void zslFree(zskiplist *zsl);
static void zslInsert(zskiplist *zsl, double score, robj *obj);
static void sendReplyToClientWritev(aeEventLoop *el, int fd, void *privdata, int mask);
static void initClientMultiState(redisClient *c);
static void freeClientMultiState(redisClient *c);
static void queueMultiCommand(redisClient *c, struct redisCommand *cmd);
static void unblockClientWaitingData(redisClient *c);
static int handleClientsWaitingListPush(redisClient *c, robj *key, robj *ele);
static void vmInit(void);
static void vmMarkPagesFree(off_t page, off_t count);
static robj *vmLoadObject(robj *key);
static robj *vmPreviewObject(robj *key);
static int vmSwapOneObjectBlocking(void);
static int vmSwapOneObjectThreaded(void);
static int vmCanSwapOut(void);
static int tryFreeOneObjectFromFreelist(void);
static void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void vmThreadedIOCompletedJob(aeEventLoop *el, int fd, void *privdata, int mask);
static void vmCancelThreadedIOJob(robj *o);
static void lockThreadedIO(void);
static void unlockThreadedIO(void);
static int vmSwapObjectThreaded(robj *key, robj *val, redisDb *db);
static void freeIOJob(iojob *j);
static void queueIOJob(iojob *j);
static int vmWriteObjectOnSwap(robj *o, off_t page);
static robj *vmReadObjectFromSwap(off_t page, int type);
static void waitEmptyIOJobsQueue(void);
static void vmReopenSwapFile(void);
static int vmFreePage(off_t page);
static void zunionInterBlockClientOnSwappedKeys(redisClient *c);
static int blockClientOnSwappedKeys(struct redisCommand *cmd, redisClient *c);
static int dontWaitForSwappedKey(redisClient *c, robj *key);
static void handleClientsBlockedOnSwappedKey(redisDb *db, robj *key);
static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
static struct redisCommand *lookupCommand(char *name);
static void call(redisClient *c, struct redisCommand *cmd);
static void resetClient(redisClient *c);
static void convertToRealHash(robj *o);

static void authCommand(redisClient *c);
static void pingCommand(redisClient *c);
static void echoCommand(redisClient *c);
static void setCommand(redisClient *c);
static void setnxCommand(redisClient *c);
static void getCommand(redisClient *c);
static void delCommand(redisClient *c);
static void existsCommand(redisClient *c);
static void incrCommand(redisClient *c);
static void decrCommand(redisClient *c);
static void incrbyCommand(redisClient *c);
static void decrbyCommand(redisClient *c);
static void selectCommand(redisClient *c);
static void randomkeyCommand(redisClient *c);
static void keysCommand(redisClient *c);
static void dbsizeCommand(redisClient *c);
static void lastsaveCommand(redisClient *c);
static void saveCommand(redisClient *c);
static void bgsaveCommand(redisClient *c);
static void bgrewriteaofCommand(redisClient *c);
static void shutdownCommand(redisClient *c);
static void moveCommand(redisClient *c);
static void renameCommand(redisClient *c);
static void renamenxCommand(redisClient *c);
static void lpushCommand(redisClient *c);
static void rpushCommand(redisClient *c);
static void lpopCommand(redisClient *c);
static void rpopCommand(redisClient *c);
static void llenCommand(redisClient *c);
static void lindexCommand(redisClient *c);
static void lrangeCommand(redisClient *c);
static void ltrimCommand(redisClient *c);
static void typeCommand(redisClient *c);
static void lsetCommand(redisClient *c);
static void saddCommand(redisClient *c);
static void sremCommand(redisClient *c);
static void smoveCommand(redisClient *c);
static void sismemberCommand(redisClient *c);
static void scardCommand(redisClient *c);
static void spopCommand(redisClient *c);
static void srandmemberCommand(redisClient *c);
static void sinterCommand(redisClient *c);
static void sinterstoreCommand(redisClient *c);
static void sunionCommand(redisClient *c);
static void sunionstoreCommand(redisClient *c);
static void sdiffCommand(redisClient *c);
static void sdiffstoreCommand(redisClient *c);
static void syncCommand(redisClient *c);
static void flushdbCommand(redisClient *c);
static void flushallCommand(redisClient *c);
static void sortCommand(redisClient *c);
static void lremCommand(redisClient *c);
static void rpoplpushcommand(redisClient *c);
static void infoCommand(redisClient *c);
static void mgetCommand(redisClient *c);
static void monitorCommand(redisClient *c);
static void expireCommand(redisClient *c);
static void expireatCommand(redisClient *c);
static void getsetCommand(redisClient *c);
static void ttlCommand(redisClient *c);
static void slaveofCommand(redisClient *c);
static void debugCommand(redisClient *c);
static void msetCommand(redisClient *c);
static void msetnxCommand(redisClient *c);
static void zaddCommand(redisClient *c);
static void zincrbyCommand(redisClient *c);
static void zrangeCommand(redisClient *c);
static void zrangebyscoreCommand(redisClient *c);
static void zcountCommand(redisClient *c);
static void zrevrangeCommand(redisClient *c);
static void zcardCommand(redisClient *c);
static void zremCommand(redisClient *c);
static void zscoreCommand(redisClient *c);
static void zremrangebyscoreCommand(redisClient *c);
static void multiCommand(redisClient *c);
static void execCommand(redisClient *c);
static void discardCommand(redisClient *c);
static void blpopCommand(redisClient *c);
static void brpopCommand(redisClient *c);
static void appendCommand(redisClient *c);
static void substrCommand(redisClient *c);
static void zrankCommand(redisClient *c);
static void zrevrankCommand(redisClient *c);
static void hsetCommand(redisClient *c);
static void hgetCommand(redisClient *c);
static void hdelCommand(redisClient *c);
static void hlenCommand(redisClient *c);
static void zremrangebyrankCommand(redisClient *c);
static void zunionCommand(redisClient *c);
static void zinterCommand(redisClient *c);
static void hkeysCommand(redisClient *c);
static void hvalsCommand(redisClient *c);
static void hgetallCommand(redisClient *c);
static void hexistsCommand(redisClient *c);


static void createSharedObjects(void);

/*================================= Globals ================================= */

/* 服务器全局状态 */
/* Global vars */
static struct redisServer server;





/*====================== Hash table type implementation  ==================== */

/* 这是一种使用SDS作为键，redis对象作为值的散列表类型
 *
 * redis对象可以保存 SDS strings, lists, sets
 */

static void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}


static void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

static int sdsDictKeyCompare(void *privdata, const void *key1,
                             const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static void dictRedisObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */
    decrRefCount(val);
}

static int dictObjKeyCompare(void *privdata, const void *key1,
                             const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return sdsDictKeyCompare(privdata,o1->ptr,o2->ptr);
}

static unsigned int dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

static int dictEncObjKeyCompare(void *privdata, const void *key1,
                                const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == REDIS_ENCODING_INT &&
        o2->encoding == REDIS_ENCODING_INT &&
        o1->ptr == o2->ptr) return 1;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = sdsDictKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

static unsigned int dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (o->encoding == REDIS_ENCODING_RAW) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == REDIS_ENCODING_INT) {
            char buf[32];
            int len;

            len = snprintf(buf,32,"%ld",(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Sets type and expires */
static dictType setDictType = {
        dictEncObjHash,            /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictEncObjKeyCompare,      /* key compare */
        dictRedisObjectDestructor, /* key destructor */
        NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
static dictType zsetDictType = {
        dictEncObjHash,            /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictEncObjKeyCompare,      /* key compare */
        dictRedisObjectDestructor, /* key destructor */
        dictVanillaFree            /* val destructor of malloc(sizeof(double)) */
};

/* Db->dict */
static dictType dbDictType = {
        dictObjHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictObjKeyCompare,          /* key compare */
        dictRedisObjectDestructor,  /* key destructor */
        dictRedisObjectDestructor   /* val destructor */
};

/* Db->expires */
static dictType keyptrDictType = {
        dictObjHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictObjKeyCompare,         /* key compare */
        dictRedisObjectDestructor, /* key destructor */
        NULL                       /* val destructor */
};

/* Hash type hash table (note that small hashes are represented with zimpaps) */
static dictType hashDictType = {
        dictEncObjHash,             /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictEncObjKeyCompare,       /* key compare */
        dictRedisObjectDestructor,  /* key destructor */
        dictRedisObjectDestructor   /* val destructor */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
static dictType keylistDictType = {
        dictObjHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictObjKeyCompare,          /* key compare */
        dictRedisObjectDestructor,  /* key destructor */
        dictListDestructor          /* val destructor */
};

/*============================ Utility functions ============================ */


/**
 * 打印日志方法
 *
 * @param level
 * @param fmt
 * @param ...
 */
static void redisLog(int level, const char *fmt, ...) {
    va_list ap;
    FILE *fp;

    fp = (server.logfile == NULL) ? stdout : fopen(server.logfile,"a");
    if (!fp) return;

    va_start(ap, fmt);
    if (level >= server.verbosity) {
        char *c = ".-*#";
        char buf[64];
        time_t now;

        now = time(NULL);
        strftime(buf,64,"%d %b %H:%M:%S",localtime(&now));
        fprintf(fp,"[%d] %s %c ",(int)getpid(),buf,c[level]);
        vfprintf(fp, fmt, ap);
        fprintf(fp,"\n");
        fflush(fp);
    }
    va_end(ap);

    if (server.logfile) fclose(fp);
}


/* ========================= Random utility functions ======================= */

/* Redis generally does not try to recover from out of memory conditions
 * when allocating objects or strings, it is not clear if it will be possible
 * to report this condition to the client since the networking layer itself
 * is based on heap allocation for send buffers, so we simply abort.
 * At least the code will be simpler to read... */
static void oom(const char *msg) {
    redisLog(REDIS_WARNING, "%s: Out of memory\n",msg);
    sleep(1);
    abort();
}

/* ====================== Redis server networking stuff ===================== */

static void closeTimedoutClients(void) {
    redisClient *c;
    listNode *ln;
    time_t now = time(NULL);
    listIter li;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);
        if (server.maxidletime &&
            !(c->flags & REDIS_SLAVE) &&    /* no timeout for slaves */
            !(c->flags & REDIS_MASTER) &&   /* no timeout for masters */
            (now - c->lastinteraction > server.maxidletime))
        {
            redisLog(REDIS_VERBOSE,"Closing idle client");
            freeClient(c);
        } else if (c->flags & REDIS_BLOCKED) {
            if (c->blockingto != 0 && c->blockingto < now) {
                addReply(c,shared.nullmultibulk);
                unblockClientWaitingData(c);
            }
        }
    }
}

static int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < REDIS_HT_MINFILL));
}

/* If the percentage of used slots in the HT reaches REDIS_HT_MINFILL
 * we resize the hash table to save memory */
static void tryResizeHashTables(void) {
    int j;

    for (j = 0; j < server.dbnum; j++) {
        if (htNeedsResize(server.db[j].dict)) {
            redisLog(REDIS_VERBOSE,"The hash table %d is too sparse, resize it...",j);
            dictResize(server.db[j].dict);
            redisLog(REDIS_VERBOSE,"Hash table %d resized.",j);
        }
        if (htNeedsResize(server.db[j].expires))
            dictResize(server.db[j].expires);
    }
}

/* A background saving child (BGSAVE) terminated its work. Handle this. */
void backgroundSaveDoneHandler(int statloc) {
    int exitcode = WEXITSTATUS(statloc);
    int bysignal = WIFSIGNALED(statloc);

    if (!bysignal && exitcode == 0) {
        redisLog(REDIS_NOTICE,
                 "Background saving terminated with success");
        server.dirty = 0;
        server.lastsave = time(NULL);
    } else if (!bysignal && exitcode != 0) {
        redisLog(REDIS_WARNING, "Background saving error");
    } else {
        redisLog(REDIS_WARNING,
                 "Background saving terminated by signal");
        rdbRemoveTempFile(server.bgsavechildpid);
    }
    server.bgsavechildpid = -1;
    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    updateSlavesWaitingBgsave(exitcode == 0 ? REDIS_OK : REDIS_ERR);
}

/* A background append only file rewriting (BGREWRITEAOF) terminated its work.
 * Handle this. */
void backgroundRewriteDoneHandler(int statloc) {
    int exitcode = WEXITSTATUS(statloc);
    int bysignal = WIFSIGNALED(statloc);

    if (!bysignal && exitcode == 0) {
        int fd;
        char tmpfile[256];

        redisLog(REDIS_NOTICE,
                 "Background append only file rewriting terminated with success");
        /* Now it's time to flush the differences accumulated by the parent */
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) server.bgrewritechildpid);
        fd = open(tmpfile,O_WRONLY|O_APPEND);
        if (fd == -1) {
            redisLog(REDIS_WARNING, "Not able to open the temp append only file produced by the child: %s", strerror(errno));
            goto cleanup;
        }
        /* Flush our data... */
        if (write(fd,server.bgrewritebuf,sdslen(server.bgrewritebuf)) !=
            (signed) sdslen(server.bgrewritebuf)) {
            redisLog(REDIS_WARNING, "Error or short write trying to flush the parent diff of the append log file in the child temp file: %s", strerror(errno));
            close(fd);
            goto cleanup;
        }
        redisLog(REDIS_NOTICE,"Parent diff flushed into the new append log file with success (%lu bytes)",sdslen(server.bgrewritebuf));
        /* Now our work is to rename the temp file into the stable file. And
         * switch the file descriptor used by the server for append only. */
        if (rename(tmpfile,server.appendfilename) == -1) {
            redisLog(REDIS_WARNING,"Can't rename the temp append only file into the stable one: %s", strerror(errno));
            close(fd);
            goto cleanup;
        }
        /* Mission completed... almost */
        redisLog(REDIS_NOTICE,"Append only file successfully rewritten.");
        if (server.appendfd != -1) {
            /* If append only is actually enabled... */
            close(server.appendfd);
            server.appendfd = fd;
            fsync(fd);
            server.appendseldb = -1; /* Make sure it will issue SELECT */
            redisLog(REDIS_NOTICE,"The new append only file was selected for future appends.");
        } else {
            /* If append only is disabled we just generate a dump in this
             * format. Why not? */
            close(fd);
        }
    } else if (!bysignal && exitcode != 0) {
        redisLog(REDIS_WARNING, "Background append only file rewriting error");
    } else {
        redisLog(REDIS_WARNING,
                 "Background append only file rewriting terminated by signal");
    }
    cleanup:
    sdsfree(server.bgrewritebuf);
    server.bgrewritebuf = sdsempty();
    aofRemoveTempFile(server.bgrewritechildpid);
    server.bgrewritechildpid = -1;
}


static int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int j, loops = server.cronloops++;
    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);

    /* We take a cached value of the unix time in the global state because
     * with virtual memory and aging there is to store the current time
     * in objects at every object access, and accuracy is not needed.
     * To access a global var is faster than calling time(NULL) */
    server.unixtime = time(NULL);

    /* Show some info about non-empty databases */
    for (j = 0; j < server.dbnum; j++) {
        long long size, used, vkeys;

        size = dictSlots(server.db[j].dict);
        used = dictSize(server.db[j].dict);
        vkeys = dictSize(server.db[j].expires);
        if (!(loops % 5) && (used || vkeys)) {
            redisLog(REDIS_VERBOSE,"DB %d: %lld keys (%lld volatile) in %lld slots HT.",j,used,vkeys,size);
            /* dictPrintStats(server.dict); */
        }
    }

    /* We don't want to resize the hash tables while a bacground saving
     * is in progress: the saving child is created using fork() that is
     * implemented with a copy-on-write semantic in most modern systems, so
     * if we resize the HT while there is the saving child at work actually
     * a lot of memory movements in the parent will cause a lot of pages
     * copied. */
    if (server.bgsavechildpid == -1) tryResizeHashTables();

    /* Show information about connected clients */
    if (!(loops % 5)) {
        redisLog(REDIS_VERBOSE,"%d clients connected (%d slaves), %zu bytes in use, %d shared objects",
                 listLength(server.clients)-listLength(server.slaves),
                 listLength(server.slaves),
                 zmalloc_used_memory(),
                 dictSize(server.sharingpool));
    }

    /* Close connections of timedout clients */
    if ((server.maxidletime && !(loops % 10)) || server.blpop_blocked_clients)
        closeTimedoutClients();

    /* Check if a background saving or AOF rewrite in progress terminated */
    if (server.bgsavechildpid != -1 || server.bgrewritechildpid != -1) {
        int statloc;
        pid_t pid;

        if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
            if (pid == server.bgsavechildpid) {
                backgroundSaveDoneHandler(statloc);
            } else {
                backgroundRewriteDoneHandler(statloc);
            }
        }
    } else {
        /* If there is not a background saving in progress check if
         * we have to save now */
        time_t now = time(NULL);
        for (j = 0; j < server.saveparamslen; j++) {
            struct saveparam *sp = server.saveparams+j;

            if (server.dirty >= sp->changes &&
                now-server.lastsave > sp->seconds) {
                redisLog(REDIS_NOTICE,"%d changes in %d seconds. Saving...",
                         sp->changes, sp->seconds);
                rdbSaveBackground(server.dbfilename);
                break;
            }
        }
    }

    /* Try to expire a few timed out keys. The algorithm used is adaptive and
     * will use few CPU cycles if there are few expiring keys, otherwise
     * it will get more aggressive to avoid that too much memory is used by
     * keys that can be removed from the keyspace. */
    for (j = 0; j < server.dbnum; j++) {
        int expired;
        redisDb *db = server.db+j;

        /* Continue to expire if at the end of the cycle more than 25%
         * of the keys were expired. */
        do {
            long num = dictSize(db->expires);
            time_t now = time(NULL);

            expired = 0;
            if (num > REDIS_EXPIRELOOKUPS_PER_CRON)
                num = REDIS_EXPIRELOOKUPS_PER_CRON;
            while (num--) {
                dictEntry *de;
                time_t t;

                if ((de = dictGetRandomKey(db->expires)) == NULL) break;
                t = (time_t) dictGetEntryVal(de);
                if (now > t) {
                    deleteKey(db,dictGetEntryKey(de));
                    expired++;
                }
            }
        } while (expired > REDIS_EXPIRELOOKUPS_PER_CRON/4);
    }

    /* Swap a few keys on disk if we are over the memory limit and VM
     * is enbled. Try to free objects from the free list first. */
    if (vmCanSwapOut()) {
        while (server.vm_enabled && zmalloc_used_memory() >
                                    server.vm_max_memory)
        {
            int retval;

            if (tryFreeOneObjectFromFreelist() == REDIS_OK) continue;
            retval = (server.vm_max_threads == 0) ?
                     vmSwapOneObjectBlocking() :
                     vmSwapOneObjectThreaded();
            if (retval == REDIS_ERR && (loops % 30) == 0 &&
                zmalloc_used_memory() >
                (server.vm_max_memory+server.vm_max_memory/10))
            {
                redisLog(REDIS_WARNING,"WARNING: vm-max-memory limit exceeded by more than 10%% but unable to swap more objects out!");
            }
            /* Note that when using threade I/O we free just one object,
             * because anyway when the I/O thread in charge to swap this
             * object out will finish, the handler of completed jobs
             * will try to swap more objects if we are still out of memory. */
            if (retval == REDIS_ERR || server.vm_max_threads > 0) break;
        }
    }

    /* Check if we should connect to a MASTER */
    if (server.replstate == REDIS_REPL_CONNECT) {
        redisLog(REDIS_NOTICE,"Connecting to MASTER...");
        if (syncWithMaster() == REDIS_OK) {
            redisLog(REDIS_NOTICE,"MASTER <-> SLAVE sync succeeded");
        }
    }
    return 1000;
}

static void createSharedObjects(void) {
    shared.crlf = createObject(REDIS_STRING,sdsnew("\r\n"));
    shared.ok = createObject(REDIS_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(REDIS_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(REDIS_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(REDIS_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(REDIS_STRING,sdsnew(":1\r\n"));
    shared.nullbulk = createObject(REDIS_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(REDIS_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(REDIS_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(REDIS_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(REDIS_STRING,sdsnew("+QUEUED\r\n"));
    shared.wrongtypeerr = createObject(REDIS_STRING,sdsnew(
            "-ERR Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(REDIS_STRING,sdsnew(
            "-ERR no such key\r\n"));
    shared.syntaxerr = createObject(REDIS_STRING,sdsnew(
            "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(REDIS_STRING,sdsnew(
            "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(REDIS_STRING,sdsnew(
            "-ERR index out of range\r\n"));
    shared.space = createObject(REDIS_STRING,sdsnew(" "));
    shared.colon = createObject(REDIS_STRING,sdsnew(":"));
    shared.plus = createObject(REDIS_STRING,sdsnew("+"));
    shared.select0 = createStringObject("select 0\r\n",10);
    shared.select1 = createStringObject("select 1\r\n",10);
    shared.select2 = createStringObject("select 2\r\n",10);
    shared.select3 = createStringObject("select 3\r\n",10);
    shared.select4 = createStringObject("select 4\r\n",10);
    shared.select5 = createStringObject("select 5\r\n",10);
    shared.select6 = createStringObject("select 6\r\n",10);
    shared.select7 = createStringObject("select 7\r\n",10);
    shared.select8 = createStringObject("select 8\r\n",10);
    shared.select9 = createStringObject("select 9\r\n",10);
}


/**
 * 服务端AOF参数
 * @param seconds
 * @param changes
 */
static void appendServerSaveParams(time_t seconds, int changes) {
    server.saveparams = zrealloc(server.saveparams,sizeof(struct saveparam)*(server.saveparamslen+1));
    server.saveparams[server.saveparamslen].seconds = seconds;
    server.saveparams[server.saveparamslen].changes = changes;
    server.saveparamslen++;
}

/**
 * 重置服务端持久化参数
 */
static void resetServerSaveParams() {
    zfree(server.saveparams);
    server.saveparams = NULL;
    server.saveparamslen = 0;
}

/**
 * 初始化服务端配置方法
 */
static void initServerConfig() {
    server.dbnum = REDIS_DEFAULT_DBNUM;
    server.port = REDIS_SERVERPORT;
    server.verbosity = REDIS_VERBOSE;
    server.maxidletime = REDIS_MAXIDLETIME;
    server.saveparams = NULL;
    server.logfile = NULL; /* NULL = 通过标准输出 */
    server.bindaddr = NULL;
    server.glueoutputbuf = 1;
    server.daemonize = 0;
    server.appendonly = 0;
    server.appendfsync = APPENDFSYNC_ALWAYS;
    server.lastfsync = time(NULL);
    server.appendfd = -1;
    server.appendseldb = -1; /* Make sure the first time will not match */
    server.pidfile = "/var/run/redis.pid";
    server.dbfilename = "dump.rdb";
    server.appendfilename = "appendonly.aof";
    server.requirepass = NULL;
    server.shareobjects = 0;
    server.rdbcompression = 1;
    server.sharingpoolsize = 1024;
    server.maxclients = 0;
    server.blpop_blocked_clients = 0;
    server.maxmemory = 0;
    server.vm_enabled = 0;
    server.vm_swap_file = zstrdup("/tmp/redis-%p.vm");
    server.vm_page_size = 256;          /* 256 bytes per page */
    server.vm_pages = 1024*1024*100;    /* 104 millions of pages */
    server.vm_max_memory = 1024LL*1024*1024*1; /* 1 GB of RAM */
    server.vm_max_threads = 4;
    server.vm_blocked_clients = 0;
    server.hash_max_zipmap_entries = REDIS_HASH_MAX_ZIPMAP_ENTRIES;
    server.hash_max_zipmap_value = REDIS_HASH_MAX_ZIPMAP_VALUE;

    resetServerSaveParams();

    appendServerSaveParams(60*60,1);  /* save after 1 hour and 1 change */
    appendServerSaveParams(300,100);  /* save after 5 minutes and 100 changes */
    appendServerSaveParams(60,10000); /* save after 1 minute and 10000 changes */

    /* 复制相关 */
    server.isslave = 0;
    server.masterauth = NULL;
    server.masterhost = NULL;
    server.masterport = 6379;
    server.master = NULL;
    server.replstate = REDIS_REPL_NONE;

    /* 实数常量初始化 */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;
}

/**
 * 初始化服务端
 */
static void initServer() {
    int j;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSigSegvAction();

    server.devnull = fopen("/dev/null","w");
    if (server.devnull == NULL) {
        redisLog(REDIS_WARNING, "Can't open /dev/null: %s", server.neterr);
        exit(1);
    }
    server.clients = listCreate();
    server.slaves = listCreate();
    server.monitors = listCreate();
    server.objfreelist = listCreate();
    createSharedObjects();
    server.el = aeCreateEventLoop();
    server.db = zmalloc(sizeof(redisDb)*server.dbnum);
    server.sharingpool = dictCreate(&setDictType,NULL);
    server.fd = anetTcpServer(server.neterr, server.port, server.bindaddr);
    if (server.fd == -1) {
        redisLog(REDIS_WARNING, "Opening TCP port: %s", server.neterr);
        exit(1);
    }
    for (j = 0; j < server.dbnum; j++) {
        server.db[j].dict = dictCreate(&dbDictType,NULL);
        server.db[j].expires = dictCreate(&keyptrDictType,NULL);
        server.db[j].blockingkeys = dictCreate(&keylistDictType,NULL);
        if (server.vm_enabled)
            server.db[j].io_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].id = j;
    }
    server.cronloops = 0;
    server.bgsavechildpid = -1;
    server.bgrewritechildpid = -1;
    server.bgrewritebuf = sdsempty();
    server.lastsave = time(NULL);
    server.dirty = 0;
    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_starttime = time(NULL);
    server.unixtime = time(NULL);
    aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL);
    if (aeCreateFileEvent(server.el, server.fd, AE_READABLE,
                          acceptHandler, NULL) == AE_ERR) oom("creating file event");

    if (server.appendonly) {
        server.appendfd = open(server.appendfilename,O_WRONLY|O_APPEND|O_CREAT,0644);
        if (server.appendfd == -1) {
            redisLog(REDIS_WARNING, "Can't open the append-only file: %s",
                     strerror(errno));
            exit(1);
        }
    }

    if (server.vm_enabled) vmInit();
}

/**
 * 字符串 YES 或者 NO 转换为int值
 *
 * YES        1
 * NO         0
 * 非YES和NO  -1
 *
 * @param s
 * @return
 */
static int yesnotoi(char *s) {
    if (!strcasecmp(s,"yes")) {
        return 1;
    } else if (!strcasecmp(s,"no")) {
        return 0;
    } else {
        return -1;
    }
}

/**
 * 加载配置文件
 *
 * @param filename
 */
static void loadServerConfig(char *filename) {
    FILE *fp;
    char buf[REDIS_CONFIGLINE_MAX+1];
    char *err = NULL;
    int linenum = 0;
    sds line = NULL;

    if (filename[0] == '-' && filename[1] == '\0') {
        fp = stdin;
    } else {
        if ((fp = fopen(filename,"r")) == NULL) {
            redisLog(REDIS_WARNING,"Fatal error, can't open config file");
            exit(1);
        }
    }

    while(fgets(buf,REDIS_CONFIGLINE_MAX+1,fp) != NULL) {
        sds *argv;
        int argc, j;

        linenum++;
        line = sdsnew(buf);
        line = sdstrim(line," \t\r\n");

        /* 跳过注释和空行 */
        if (line[0] == '#' || line[0] == '\0') {
            sdsfree(line);
            continue;
        }

        /* Split into arguments */
        argv = sdssplitlen(line,sdslen(line)," ",1,&argc);
        sdstolower(argv[0]);

        /* Execute config directives */
        if (!strcasecmp(argv[0],"timeout") && argc == 2) {
            server.maxidletime = atoi(argv[1]);
            if (server.maxidletime < 0) {
                err = "Invalid timeout value"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"port") && argc == 2) {
            server.port = atoi(argv[1]);
            if (server.port < 1 || server.port > 65535) {
                err = "Invalid port"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"bind") && argc == 2) {
            server.bindaddr = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"save") && argc == 3) {
            int seconds = atoi(argv[1]);
            int changes = atoi(argv[2]);
            if (seconds < 1 || changes < 0) {
                err = "Invalid save parameters"; goto loaderr;
            }
            appendServerSaveParams(seconds,changes);
        } else if (!strcasecmp(argv[0],"dir") && argc == 2) {
            if (chdir(argv[1]) == -1) {
                redisLog(REDIS_WARNING,"Can't chdir to '%s': %s",
                         argv[1], strerror(errno));
                exit(1);
            }
        } else if (!strcasecmp(argv[0],"loglevel") && argc == 2) {
            if (!strcasecmp(argv[1],"debug")) server.verbosity = REDIS_DEBUG;
            else if (!strcasecmp(argv[1],"verbose")) server.verbosity = REDIS_VERBOSE;
            else if (!strcasecmp(argv[1],"notice")) server.verbosity = REDIS_NOTICE;
            else if (!strcasecmp(argv[1],"warning")) server.verbosity = REDIS_WARNING;
            else {
                err = "Invalid log level. Must be one of debug, notice, warning";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"logfile") && argc == 2) {
            FILE *logfp;

            server.logfile = zstrdup(argv[1]);
            if (!strcasecmp(server.logfile,"stdout")) {
                zfree(server.logfile);
                server.logfile = NULL;
            }
            if (server.logfile) {
                /* Test if we are able to open the file. The server will not
                 * be able to abort just for this problem later... */
                logfp = fopen(server.logfile,"a");
                if (logfp == NULL) {
                    err = sdscatprintf(sdsempty(),
                                       "Can't open the log file: %s", strerror(errno));
                    goto loaderr;
                }
                fclose(logfp);
            }
        } else if (!strcasecmp(argv[0],"databases") && argc == 2) {
            server.dbnum = atoi(argv[1]);
            if (server.dbnum < 1) {
                err = "Invalid number of databases"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"maxclients") && argc == 2) {
            server.maxclients = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"maxmemory") && argc == 2) {
            server.maxmemory = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"slaveof") && argc == 3) {
            server.masterhost = sdsnew(argv[1]);
            server.masterport = atoi(argv[2]);
            server.replstate = REDIS_REPL_CONNECT;
        } else if (!strcasecmp(argv[0],"masterauth") && argc == 2) {
            server.masterauth = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"glueoutputbuf") && argc == 2) {
            if ((server.glueoutputbuf = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"shareobjects") && argc == 2) {
            if ((server.shareobjects = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"rdbcompression") && argc == 2) {
            if ((server.rdbcompression = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"shareobjectspoolsize") && argc == 2) {
            server.sharingpoolsize = atoi(argv[1]);
            if (server.sharingpoolsize < 1) {
                err = "invalid object sharing pool size"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"daemonize") && argc == 2) {
            if ((server.daemonize = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"appendonly") && argc == 2) {
            if ((server.appendonly = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"appendfsync") && argc == 2) {
            if (!strcasecmp(argv[1],"no")) {
                server.appendfsync = APPENDFSYNC_NO;
            } else if (!strcasecmp(argv[1],"always")) {
                server.appendfsync = APPENDFSYNC_ALWAYS;
            } else if (!strcasecmp(argv[1],"everysec")) {
                server.appendfsync = APPENDFSYNC_EVERYSEC;
            } else {
                err = "argument must be 'no', 'always' or 'everysec'";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"requirepass") && argc == 2) {
            server.requirepass = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"pidfile") && argc == 2) {
            server.pidfile = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"dbfilename") && argc == 2) {
            server.dbfilename = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"vm-enabled") && argc == 2) {
            if ((server.vm_enabled = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"vm-swap-file") && argc == 2) {
            zfree(server.vm_swap_file);
            server.vm_swap_file = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"vm-max-memory") && argc == 2) {
            server.vm_max_memory = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"vm-page-size") && argc == 2) {
            server.vm_page_size = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"vm-pages") && argc == 2) {
            server.vm_pages = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"vm-max-threads") && argc == 2) {
            server.vm_max_threads = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"hash-max-zipmap-entries") && argc == 2){
            server.hash_max_zipmap_entries = strtol(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"hash-max-zipmap-value") && argc == 2){
            server.hash_max_zipmap_value = strtol(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"vm-max-threads") && argc == 2) {
            server.vm_max_threads = strtoll(argv[1], NULL, 10);
        } else {
            err = "Bad directive or wrong number of arguments"; goto loaderr;
        }
        for (j = 0; j < argc; j++)
            sdsfree(argv[j]);
        zfree(argv);
        sdsfree(line);
    }
    if (fp != stdin) fclose(fp);
    return;

    loaderr:
    fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR ***\n");
    fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
    fprintf(stderr, ">>> '%s'\n", line);
    fprintf(stderr, "%s\n", err);
    exit(1);
}


/* ======================= Redis objects implementation ===================== */

static robj *createObject(int type, void *ptr) {
    robj *o;

    if (server.vm_enabled) pthread_mutex_lock(&server.obj_freelist_mutex);
    if (listLength(server.objfreelist)) {
        listNode *head = listFirst(server.objfreelist);
        o = listNodeValue(head);
        listDelNode(server.objfreelist,head);
        if (server.vm_enabled) pthread_mutex_unlock(&server.obj_freelist_mutex);
    } else {
        if (server.vm_enabled) {
            pthread_mutex_unlock(&server.obj_freelist_mutex);
            o = zmalloc(sizeof(*o));
        } else {
            o = zmalloc(sizeof(*o)-sizeof(struct redisObjectVM));
        }
    }
    o->type = type;
    o->encoding = REDIS_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;
    if (server.vm_enabled) {
        /* Note that this code may run in the context of an I/O thread
         * and accessing to server.unixtime in theory is an error
         * (no locks). But in practice this is safe, and even if we read
         * garbage Redis will not fail, as it's just a statistical info */
        o->vm.atime = server.unixtime;
        o->storage = REDIS_VM_MEMORY;
    }
    return o;
}

static robj *createStringObject(char *ptr, size_t len) {
    return createObject(REDIS_STRING,sdsnewlen(ptr,len));
}

static robj *dupStringObject(robj *o) {
    assert(o->encoding == REDIS_ENCODING_RAW);
    return createStringObject(o->ptr,sdslen(o->ptr));
}

static robj *createListObject(void) {
    list *l = listCreate();

    listSetFreeMethod(l,decrRefCount);
    return createObject(REDIS_LIST,l);
}

static robj *createSetObject(void) {
    dict *d = dictCreate(&setDictType,NULL);
    return createObject(REDIS_SET,d);
}

static robj *createHashObject(void) {
    /* All the Hashes start as zipmaps. Will be automatically converted
     * into hash tables if there are enough elements or big elements
     * inside. */
    unsigned char *zm = zipmapNew();
    robj *o = createObject(REDIS_HASH,zm);
    o->encoding = REDIS_ENCODING_ZIPMAP;
    return o;
}

static robj *createZsetObject(void) {
    zset *zs = zmalloc(sizeof(*zs));

    zs->dict = dictCreate(&zsetDictType,NULL);
    zs->zsl = zslCreate();
    return createObject(REDIS_ZSET,zs);
}

static void freeStringObject(robj *o) {
    if (o->encoding == REDIS_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

static void freeListObject(robj *o) {
    listRelease((list*) o->ptr);
}

static void freeSetObject(robj *o) {
    dictRelease((dict*) o->ptr);
}

static void freeZsetObject(robj *o) {
    zset *zs = o->ptr;

    dictRelease(zs->dict);
    zslFree(zs->zsl);
    zfree(zs);
}

static void freeHashObject(robj *o) {
    switch (o->encoding) {
        case REDIS_ENCODING_HT:
            dictRelease((dict*) o->ptr);
            break;
        case REDIS_ENCODING_ZIPMAP:
            zfree(o->ptr);
            break;
        default:
            redisAssert(0);
            break;
    }
}

static void incrRefCount(robj *o) {
    redisAssert(!server.vm_enabled || o->storage == REDIS_VM_MEMORY);
    o->refcount++;
}

static void decrRefCount(void *obj) {
    robj *o = obj;

    /* Object is a key of a swapped out value, or in the process of being
     * loaded. */
    if (server.vm_enabled &&
        (o->storage == REDIS_VM_SWAPPED || o->storage == REDIS_VM_LOADING))
    {
        if (o->storage == REDIS_VM_SWAPPED || o->storage == REDIS_VM_LOADING) {
            redisAssert(o->refcount == 1);
        }
        if (o->storage == REDIS_VM_LOADING) vmCancelThreadedIOJob(obj);
        redisAssert(o->type == REDIS_STRING);
        freeStringObject(o);
        vmMarkPagesFree(o->vm.page,o->vm.usedpages);
        pthread_mutex_lock(&server.obj_freelist_mutex);
        if (listLength(server.objfreelist) > REDIS_OBJFREELIST_MAX ||
            !listAddNodeHead(server.objfreelist,o))
            zfree(o);
        pthread_mutex_unlock(&server.obj_freelist_mutex);
        server.vm_stats_swapped_objects--;
        return;
    }
    /* Object is in memory, or in the process of being swapped out. */
    if (--(o->refcount) == 0) {
        if (server.vm_enabled && o->storage == REDIS_VM_SWAPPING)
            vmCancelThreadedIOJob(obj);
        switch(o->type) {
            case REDIS_STRING: freeStringObject(o); break;
            case REDIS_LIST: freeListObject(o); break;
            case REDIS_SET: freeSetObject(o); break;
            case REDIS_ZSET: freeZsetObject(o); break;
            case REDIS_HASH: freeHashObject(o); break;
            default: redisAssert(0); break;
        }
        if (server.vm_enabled) pthread_mutex_lock(&server.obj_freelist_mutex);
        if (listLength(server.objfreelist) > REDIS_OBJFREELIST_MAX ||
            !listAddNodeHead(server.objfreelist,o))
            zfree(o);
        if (server.vm_enabled) pthread_mutex_unlock(&server.obj_freelist_mutex);
    }
}

/* Compare two string objects via strcmp() or alike.
 * Note that the objects may be integer-encoded. In such a case we
 * use snprintf() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * Important note: if objects are not integer encoded, but binary-safe strings,
 * sdscmp() from sds.c will apply memcmp() so this function ca be considered
 * binary safe. */
static int compareStringObjects(robj *a, robj *b) {
    redisAssert(a->type == REDIS_STRING && b->type == REDIS_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    int bothsds = 1;

    if (a == b) return 0;
    if (a->encoding != REDIS_ENCODING_RAW) {
        snprintf(bufa,sizeof(bufa),"%ld",(long) a->ptr);
        astr = bufa;
        bothsds = 0;
    } else {
        astr = a->ptr;
    }
    if (b->encoding != REDIS_ENCODING_RAW) {
        snprintf(bufb,sizeof(bufb),"%ld",(long) b->ptr);
        bstr = bufb;
        bothsds = 0;
    } else {
        bstr = b->ptr;
    }
    return bothsds ? sdscmp(astr,bstr) : strcmp(astr,bstr);
}

/* Get a decoded version of an encoded object (returned as a new object).
 * If the object is already raw-encoded just increment the ref count. */
static robj *getDecodedObject(robj *o) {
    robj *dec;

    if (o->encoding == REDIS_ENCODING_RAW) {
        incrRefCount(o);
        return o;
    }
    if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT) {
        char buf[32];

        snprintf(buf,32,"%ld",(long)o->ptr);
        dec = createStringObject(buf,strlen(buf));
        return dec;
    } else {
        redisAssert(1 != 1);
    }
}

/*============================ RDB saving/loading =========================== */

/*================================== Commands =============================== */

/*=================================== Strings =============================== */

/* ========================= Type agnostic commands ========================= */

/* =================================== Lists ================================ */

/* ==================================== Sets ================================ */

/* ==================================== ZSets =============================== */


/* =================================== Hashes =============================== */

/* ========================= 非对象类型的命令  ==================== */

/**
 * 将一定数量的字节转换为可读字符串
 */
static void bytesToHuman(char *s, unsigned long long n) {
    double d;

    if (n < 1024) {
        /* Bytes */
        sprintf(s,"%lluB",n);
        return;
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    }
}

/**
 * 创建INFO命令返回的字符串。
 *
 * INFO命令本身解耦了这一点，因为我们需要报告关于内存损坏问题的相同信息。
 */
static sds genRedisInfoString(void) {
    sds info;
    time_t uptime = time(NULL)-server.stat_starttime;
    int j;
    char hmem[64];

    server.hash_max_zipmap_entries = REDIS_HASH_MAX_ZIPMAP_ENTRIES;
    server.hash_max_zipmap_value = REDIS_HASH_MAX_ZIPMAP_VALUE;

    bytesToHuman(hmem,zmalloc_used_memory());
    info = sdscatprintf(sdsempty(),
                        "redis_version:%s\r\n"
                        "arch_bits:%s\r\n"
                        "multiplexing_api:%s\r\n"
                        "process_id:%ld\r\n"
                        "uptime_in_seconds:%ld\r\n"
                        "uptime_in_days:%ld\r\n"
                        "connected_clients:%d\r\n"
                        "connected_slaves:%d\r\n"
                        "blocked_clients:%d\r\n"
                        "used_memory:%zu\r\n"
                        "used_memory_human:%s\r\n"
                        "changes_since_last_save:%lld\r\n"
                        "bgsave_in_progress:%d\r\n"
                        "last_save_time:%ld\r\n"
                        "bgrewriteaof_in_progress:%d\r\n"
                        "total_connections_received:%lld\r\n"
                        "total_commands_processed:%lld\r\n"
                        "hash_max_zipmap_entries:%ld\r\n"
                        "hash_max_zipmap_value:%ld\r\n"
                        "vm_enabled:%d\r\n"
                        "role:%s\r\n"
            ,REDIS_VERSION,
                        (sizeof(long) == 8) ? "64" : "32",
                        aeGetApiName(),
                        (long) getpid(),
                        uptime,
                        uptime/(3600*24),
                        listLength(server.clients)-listLength(server.slaves),
                        listLength(server.slaves),
                        server.blpop_blocked_clients,
                        zmalloc_used_memory(),
                        hmem,
                        server.dirty,
                        server.bgsavechildpid != -1,
                        server.lastsave,
                        server.bgrewritechildpid != -1,
                        server.stat_numconnections,
                        server.stat_numcommands,
                        server.hash_max_zipmap_entries,
                        server.hash_max_zipmap_value,
                        server.vm_enabled != 0,
                        server.masterhost == NULL ? "master" : "slave"
    );
    if (server.masterhost) {
        info = sdscatprintf(info,
                            "master_host:%s\r\n"
                            "master_port:%d\r\n"
                            "master_link_status:%s\r\n"
                            "master_last_io_seconds_ago:%d\r\n"
                ,server.masterhost,
                            server.masterport,
                            (server.replstate == REDIS_REPL_CONNECTED) ?
                            "up" : "down",
                            server.master ? ((int)(time(NULL)-server.master->lastinteraction)) : -1
        );
    }
    if (server.vm_enabled) {
        lockThreadedIO();
        info = sdscatprintf(info,
                            "vm_conf_max_memory:%llu\r\n"
                            "vm_conf_page_size:%llu\r\n"
                            "vm_conf_pages:%llu\r\n"
                            "vm_stats_used_pages:%llu\r\n"
                            "vm_stats_swapped_objects:%llu\r\n"
                            "vm_stats_swappin_count:%llu\r\n"
                            "vm_stats_swappout_count:%llu\r\n"
                            "vm_stats_io_newjobs_len:%lu\r\n"
                            "vm_stats_io_processing_len:%lu\r\n"
                            "vm_stats_io_processed_len:%lu\r\n"
                            "vm_stats_io_active_threads:%lu\r\n"
                            "vm_stats_blocked_clients:%lu\r\n"
                ,(unsigned long long) server.vm_max_memory,
                            (unsigned long long) server.vm_page_size,
                            (unsigned long long) server.vm_pages,
                            (unsigned long long) server.vm_stats_used_pages,
                            (unsigned long long) server.vm_stats_swapped_objects,
                            (unsigned long long) server.vm_stats_swapins,
                            (unsigned long long) server.vm_stats_swapouts,
                            (unsigned long) listLength(server.io_newjobs),
                            (unsigned long) listLength(server.io_processing),
                            (unsigned long) listLength(server.io_processed),
                            (unsigned long) server.io_active_threads,
                            (unsigned long) server.vm_blocked_clients
        );
        unlockThreadedIO();
    }
    for (j = 0; j < server.dbnum; j++) {
        long long keys, vkeys;

        keys = dictSize(server.db[j].dict);
        vkeys = dictSize(server.db[j].expires);
        if (keys || vkeys) {
            info = sdscatprintf(info, "db%d:keys=%lld,expires=%lld\r\n",
                                j, keys, vkeys);
        }
    }
    return info;
}

/* ================================= Expire ================================= */

/* ================================ MULTI/EXEC ============================== */

/* =========================== Blocking Operations  ========================= */

/* =============================== Replication  ============================= */

/* ============================ Maxmemory directive  ======================== */

/* ============================== Append Only file ========================== */

/* =================== Virtual Memory - Blocking Side  ====================== */

/* Test if the page is free */
static int vmFreePage(off_t page) {
    off_t byte = page/8;
    int bit = page&7;
    return (server.vm_bitmap[byte] & (1<<bit)) == 0;
}


/* Mark the page as free */
static void vmMarkPageFree(off_t page) {
    off_t byte = page/8;
    int bit = page&7;
    redisAssert(vmFreePage(page) == 0);
    server.vm_bitmap[byte] &= ~(1<<bit);
}

/* Mark N contiguous pages as free, with 'page' being the first. */
static void vmMarkPagesFree(off_t page, off_t count) {
    off_t j;

    for (j = 0; j < count; j++)
        vmMarkPageFree(page+j);
    server.vm_stats_used_pages -= count;
    redisLog(REDIS_DEBUG,"Mark FREE pages: %lld pages at %lld\n",
             (long long)count, (long long)page);
}

/* =================== Virtual Memory - Threaded I/O  ======================= */

static void freeIOJob(iojob *j) {
    if ((j->type == REDIS_IOJOB_PREPARE_SWAP ||
         j->type == REDIS_IOJOB_DO_SWAP ||
         j->type == REDIS_IOJOB_LOAD) && j->val != NULL)
        decrRefCount(j->val);
    decrRefCount(j->key);
    zfree(j);
}

/**
 * IO线程加锁
 */
static void lockThreadedIO(void) {
    pthread_mutex_lock(&server.io_mutex);
}

/**
 * IO线程解锁
 */
static void unlockThreadedIO(void) {
    pthread_mutex_unlock(&server.io_mutex);
}

/* Remove the specified object from the threaded I/O queue if still not
 * processed, otherwise make sure to flag it as canceled. */
static void vmCancelThreadedIOJob(robj *o) {
    list *lists[3] = {
            server.io_newjobs,      /* 0 */
            server.io_processing,   /* 1 */
            server.io_processed     /* 2 */
    };
    int i;

    assert(o->storage == REDIS_VM_LOADING || o->storage == REDIS_VM_SWAPPING);
    again:
    lockThreadedIO();
    /* Search for a matching key in one of the queues */
    for (i = 0; i < 3; i++) {
        listNode *ln;
        listIter li;

        listRewind(lists[i],&li);
        while ((ln = listNext(&li)) != NULL) {
            iojob *job = ln->value;

            if (job->canceled) continue; /* Skip this, already canceled. */
            if (compareStringObjects(job->key,o) == 0) {
                redisLog(REDIS_DEBUG,"*** CANCELED %p (%s) (type %d) (LIST ID %d)\n",
                         (void*)job, (char*)o->ptr, job->type, i);
                /* Mark the pages as free since the swap didn't happened
                 * or happened but is now discarded. */
                if (i != 1 && job->type == REDIS_IOJOB_DO_SWAP)
                    vmMarkPagesFree(job->page,job->pages);
                /* Cancel the job. It depends on the list the job is
                 * living in. */
                switch(i) {
                    case 0: /* io_newjobs */
                        /* If the job was yet not processed the best thing to do
                         * is to remove it from the queue at all */
                        freeIOJob(job);
                        listDelNode(lists[i],ln);
                        break;
                    case 1: /* io_processing */
                        /* Oh Shi- the thread is messing with the Job:
                         *
                         * Probably it's accessing the object if this is a
                         * PREPARE_SWAP or DO_SWAP job.
                         * If it's a LOAD job it may be reading from disk and
                         * if we don't wait for the job to terminate before to
                         * cancel it, maybe in a few microseconds data can be
                         * corrupted in this pages. So the short story is:
                         *
                         * Better to wait for the job to move into the
                         * next queue (processed)... */

                        /* We try again and again until the job is completed. */
                        unlockThreadedIO();
                        /* But let's wait some time for the I/O thread
                         * to finish with this job. After all this condition
                         * should be very rare. */
                        usleep(1);
                        goto again;
                    case 2: /* io_processed */
                        /* The job was already processed, that's easy...
                         * just mark it as canceled so that we'll ignore it
                         * when processing completed jobs. */
                        job->canceled = 1;
                        break;
                }
                /* Finally we have to adjust the storage type of the object
                 * in order to "UNDO" the operaiton. */
                if (o->storage == REDIS_VM_LOADING)
                    o->storage = REDIS_VM_SWAPPED;
                else if (o->storage == REDIS_VM_SWAPPING)
                    o->storage = REDIS_VM_MEMORY;
                unlockThreadedIO();
                return;
            }
        }
    }
    unlockThreadedIO();
    assert(1 != 1); /* We should never reach this */
}

/* ============ Virtual Memory - Blocking clients on missing keys =========== */

/* ================================= Debugging ============================== */





/* =================================== Main! ================================ */

/**
 * 后台运行
 */
static void daemonize(void) {
    int fd;
    FILE *fp;

    if (fork() != 0) {
        exit(0); /* parent exits */
    }
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Redis is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
    /* Try to write the pid file */
    fp = fopen(server.pidfile,"w");
    if (fp) {
        fprintf(fp,"%d\n",getpid());
        fclose(fp);
    }
}

/**
 * 启动方法
 *
 * @param argc      参数数量
 * @param argv      参数数组指针
 * @return
 */
int main(int argc, char **argv) {
    // 记录开始时间
    time_t start;

    // 初始化服务端配置方法
    initServerConfig();

    // 根据启动参数加载文件
    if (argc == 2) {
        resetServerSaveParams();
        // 参数带有配置文件路径时，加载配置文件
        loadServerConfig(argv[1]);
    } else if (argc > 2) {
        fprintf(stderr,"Usage: ./redis-server [/path/to/redis.conf]\n");
        exit(1);
    } else {
        // 实用默认配置
        redisLog(REDIS_WARNING,"Warning: no config file specified, using the default config. In order to specify a config file use 'redis-server /path/to/redis.conf'");
    }

    // 是否后台运行
    if (server.daemonize) {
        daemonize();
    }

    // 初始化服务端相关对象
    initServer();
    redisLog(REDIS_NOTICE,"Server started, Redis version " REDIS_VERSION);
}

/* ============================= 回溯的支持 ========================= */

#ifdef HAVE_BACKTRACE
static char *findFuncName(void *pointer, unsigned long *offset);

static void *getMcontextEip(ucontext_t *uc) {
#if defined(__FreeBSD__)
    return (void*) uc->uc_mcontext.mc_eip;
#elif defined(__dietlibc__)
    return (void*) uc->uc_mcontext.eip;
#elif defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
    #if __x86_64__
    return (void*) uc->uc_mcontext->__ss.__rip;
  #else
    return (void*) uc->uc_mcontext->__ss.__eip;
  #endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
#if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    return (void*) uc->uc_mcontext->__ss.__rip;
#else
    return (void*) uc->uc_mcontext->__ss.__eip;
#endif
#elif defined(__i386__) || defined(__X86_64__) || defined(__x86_64__)
    return (void*) uc->uc_mcontext.gregs[REG_EIP]; /* Linux 32/64 bit */
#elif defined(__ia64__) /* Linux IA64 */
    return (void*) uc->uc_mcontext.sc_ip;
#else
    return NULL;
#endif
}

/**
 * 处理段错误的信号处理函数
 *
 * 当程序访问了无效的内存地址，导致段错误时，操作系统会发送 SIGSEGV 信号，如果程序设置了相应的信号处理函数，那么该函数就会被调用。
 *
 * @param sig
 * @param info
 * @param secret
 */
static void segvHandler(int sig, siginfo_t *info, void *secret) {
    void *trace[100];
    char **messages = NULL;
    int i, trace_size = 0;
    unsigned long offset=0;
    ucontext_t *uc = (ucontext_t*) secret;
    sds infostring;
    REDIS_NOTUSED(info);

    redisLog(REDIS_WARNING,
             "======= Ooops! Redis %s got signal: -%d- =======", REDIS_VERSION, sig);
    infostring = genRedisInfoString();
    redisLog(REDIS_WARNING, "%s",infostring);
    /* It's not safe to sdsfree() the returned string under memory
     * corruption conditions. Let it leak as we are going to abort */

    trace_size = backtrace(trace, 100);
    /* overwrite sigaction with caller's address */
    if (getMcontextEip(uc) != NULL) {
        trace[1] = getMcontextEip(uc);
    }
    messages = backtrace_symbols(trace, trace_size);

    for (i=1; i<trace_size; ++i) {
        char *fn = findFuncName(trace[i], &offset), *p;

        p = strchr(messages[i],'+');
        if (!fn || (p && ((unsigned long)strtol(p+1,NULL,10)) < offset)) {
            redisLog(REDIS_WARNING,"%s", messages[i]);
        } else {
            redisLog(REDIS_WARNING,"%d redis-server %p %s + %d", i, trace[i], fn, (unsigned int)offset);
        }
    }
    /* free(messages); Don't call free() with possibly corrupted memory. */
    _exit(0);
}

/**
 * 设置在发生段错误时的信号处理函数
 *
 * （Segmentation Fault，通常表示程序访问了无效的内存地址）
 */
static void setupSigSegvAction(void) {
    struct sigaction act;

    sigemptyset (&act.sa_mask);
    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
     * is used. Otherwise, sa_handler is used */
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = segvHandler;
    sigaction (SIGSEGV, &act, NULL);
    sigaction (SIGBUS, &act, NULL);
    sigaction (SIGFPE, &act, NULL);
    sigaction (SIGILL, &act, NULL);
    sigaction (SIGBUS, &act, NULL);
    return;
}

#else /* HAVE_BACKTRACE */
static void setupSigSegvAction(void) {
}
#endif /* HAVE_BACKTRACE */