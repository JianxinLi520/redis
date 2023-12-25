/**
 *  list
 *
 *  链表数据结构
 */


#ifndef __ADLIST_H
#define __ADLIST_H

/**
 * 链表结点
 */
typedef struct listNode {
    struct listNode *prev;
    struct listNode *next;
    void *value;
} listNode;

typedef struct listIter {
    listNode *next;
    int direction;
} listIter;

/**
 * 链表
 */
typedef struct list {
    // 头节点
    listNode *head;
    // 尾节点
    listNode *tail;
    void *(*dup)(void *ptr);
    void (*free)(void *ptr);
    int (*match)(void *ptr, void *key);
    unsigned int len;
} list;

/* Functions implemented as macros */
/* 以宏实现的函数 */
// 获取链表的长度
#define listLength(l) ((l)->len)
// 获取第一个元素
#define listFirst(l) ((l)->head)
// 获取最后一个元素
#define listLast(l) ((l)->tail)
// 获取下一个元素
#define listNextNode(n) ((n)->next)
// 获取节点的值
#define listNodeValue(n) ((n)->value)

#define listSetDupMethod(l,m) ((l)->dup = (m))
#define listSetFreeMethod(l,m) ((l)->free = (m))

/* Prototypes */
list *listCreate(void);
void listRelease(list *list);
list *listAddNodeHead(list *list, void *value);
list *listAddNodeTail(list *list, void *value);
void listDelNode(list *list, listNode *node);
listIter *listGetIterator(list *list, int direction);
listNode *listNext(listIter *iter);
void listReleaseIterator(listIter *iter);
listNode *listSearchKey(list *list, void *key);
void listRewind(list *list, listIter *li);

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif //__ADLIST_H
