//
// Created by li on 2023/12/21.
//

#ifndef __ADLIST_H
#define __ADLIST_H


typedef struct listNode {
    struct listNode *prev;
    struct listNode *next;
    void *value;
} listNode;


typedef struct list {
    listNode *head;
    listNode *tail;
    void *(*dup)(void *ptr);
    void (*free)(void *ptr);
    int (*match)(void *ptr, void *key);
    unsigned int len;
} list;

#endif //__ADLIST_H
