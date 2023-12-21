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

#endif //__ADLIST_H
