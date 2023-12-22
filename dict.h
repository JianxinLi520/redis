/**
 * dict
 *
 * dictionary 字典（哈希表） 数据结构
 */

#ifndef REDIS_1_3_6_REPRODUCTION_DICT_H
#define REDIS_1_3

#define DICT_OK 0
#define DICT_ERR 1

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void) V)

/**
 * 字典元素
 *
 *      void * 表示未知数据类型的指针。
 */
typedef struct dictEntry {
    void *key;
    void *val;
    struct dictEntry *next;     /* 指向下一个dictEntry的指针，用链表形式解决哈希冲突 */
} dictEntry;

/**
 * 包含一组用于对字典操作的函数指针的结构体
 */
typedef struct dictType {
    unsigned int (*hashFunction)(const void *key);
    void *(*keyDup)(void *privdata, const void *key);
    void *(*valDup)(void *privdata, const void *obj);
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);
    void (*keyDestructor)(void *privdata, void *key);
    void (*valDestructor)(void *privdata, void *obj);
} dictType;

/*
 * 字典
 */
typedef struct dict {
    dictEntry **table;          /* 存储键值对的基本结构*/
    dictType *type;             /* 对字典的操作 */
    unsigned long size;         /* 哈希桶的数量 */
    unsigned long sizemask;     /* 哈希表大小掩码，用于快速计算键的索引。hash & sizemask */
    unsigned long used;         /* 已使用的哈希表节点数，用于判断是否对哈希表扩容 */
    void *privdata;
} dict;

/* This is the initial size of every hash table */
#define DICT_HT_INITIAL_SIZE     4

/* ------------------------------- 宏 Macros ------------------------------------*/

#define dictFreeEntryVal(ht, entry) \
    if ((ht)->type->valDestructor) \
        (ht)->type->valDestructor((ht)->privdata, (entry)->val)

#define dictFreeEntryKey(ht, entry) \
    if ((ht)->type->keyDestructor) \
        (ht)->type->keyDestructor((ht)->privdata, (entry)->key)

#define dictHashKey(ht, key) (ht)->type->hashFunction(key)

#define dictGetEntryKey(he) ((he)->key)
#define dictGetEntryVal(he) ((he)->val)
#define dictSlots(ht) ((ht)->size)
#define dictSize(ht) ((ht)->used)

/* API */
dict *dictCreate(dictType *type, void *privDataPtr);
int dictExpand(dict *ht, unsigned long size);
void dictRelease(dict *ht);
int dictResize(dict *ht);
dictEntry *dictGetRandomKey(dict *ht);
unsigned int dictGenHashFunction(const unsigned char *buf, int len);

#endif //REDIS_1_3_6_REPRODUCTION_DICT_H
