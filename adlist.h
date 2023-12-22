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
    listNode *head;
    listNode *tail;
    void *(*dup)(void *ptr);
    void (*free)(void *ptr);
    int (*match)(void *ptr, void *key);
    unsigned int len;
} list;

/* Functions implemented as macros */
/* 以宏实现的函数 */
#define listLength(l) ((l)->len)
#define listFirst(l) ((l)->head)
#define listNodeValue(n) ((n)->value)

#define listSetFreeMethod(l,m) ((l)->free = (m))

/* Prototypes */
list *listCreate(void);
void listRelease(list *list);
list *listAddNodeHead(list *list, void *value);
void listDelNode(list *list, listNode *node);
listNode *listNext(listIter *iter);
void listRewind(list *list, listIter *li);

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif //__ADLIST_H
