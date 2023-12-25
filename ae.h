/**
 * 多路复用工具类
 */

#ifndef REDIS_1_3_6_REPRODUCTION_AE_H
#define REDIS_1_3_6_REPRODUCTION_AE_H

#define AE_SETSIZE (1024*10)    /* Max number of fd supported */

#define AE_OK 0
#define AE_ERR -1

#define AE_NONE 0
#define AE_READABLE 1
#define AE_WRITABLE 2

#define AE_FILE_EVENTS 1
#define AE_TIME_EVENTS 2
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
#define AE_DONT_WAIT 4

#define AE_NOMORE -1

struct aeEventLoop;

/* Types and data structures
 *
 * 类型和数据结构
 */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);


/* File event structure
 *
 * 文件事件结构体
 */
typedef struct aeFileEvent {
    int mask; /* one of AE_(READABLE|WRITABLE) */
    aeFileProc *rfileProc;
    aeFileProc *wfileProc;
    void *clientData;
} aeFileEvent;

/* Time event structure
 *
 * 时间事件结构体
 */
typedef struct aeTimeEvent {
    long long id; /* time event identifier. */
    long when_sec; /* seconds */
    long when_ms; /* milliseconds */
    aeTimeProc *timeProc;
    aeEventFinalizerProc *finalizerProc;
    void *clientData;
    struct aeTimeEvent *next;
} aeTimeEvent;

/* A fired event
 *
 * 已就绪的事件
 */
typedef struct aeFiredEvent {
    int fd;
    int mask;
} aeFiredEvent;

/**
 * 基于事件处理的结构体
 */
typedef struct aeEventLoop {
    int maxfd;                      /* 表示当前已注册事件中的最大文件描述符，用于提高事件处理的效率。 */
    long long timeEventNextId;      /* 是下一个时间事件的 ID，用于唯一标识每个时间事件。 */
    aeFileEvent events[AE_SETSIZE]; /* Registered events - 文件事件数组，用于保存已注册的文件 */
    aeFiredEvent fired[AE_SETSIZE]; /* Fired events - 已就绪的文件数组 */
    aeTimeEvent *timeEventHead;     /* 一个指向时间事件链表头节点的指针，用于保存所有注册的时间事件。 */
    int stop;
    void *apidata; /* This is used for polling API specific data */
    aeBeforeSleepProc *beforesleep;
} aeEventLoop;

/* Prototypes */
aeEventLoop *aeCreateEventLoop(void);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
                            aeTimeProc *proc, void *clientData,
                            aeEventFinalizerProc *finalizerProc);


/**
 * 返回当前事件库的名称
 *
 * Redis 使用不同的事件库来适应不同的系统平台和性能需求。
 * @return
 */
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                      aeFileProc *proc, void *clientData);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
int aeWait(int fd, int mask, long long milliseconds);
char *aeGetApiName(void);

#endif //REDIS_1_3_6_REPRODUCTION_AE_H
