/**
 * 动态内存分配
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#include "zmalloc.h"


#if defined(__sun)
#define PREFIX_SIZE sizeof(long long)
#else
#define PREFIX_SIZE sizeof(size_t)
#endif

/**
 * 记录增加系统已使用的内存量
 */
#define increment_used_memory(_n) do { \
    if (zmalloc_thread_safe) { \
        pthread_mutex_lock(&used_memory_mutex);  \
        used_memory += _n; \
        pthread_mutex_unlock(&used_memory_mutex); \
    } else { \
        used_memory += _n; \
    } \
} while(0)

/**
 * 记录减少系统已使用的内存量
 */
#define decrement_used_memory(_n) do { \
    if (zmalloc_thread_safe) { \
        pthread_mutex_lock(&used_memory_mutex);  \
        used_memory -= _n; \
        pthread_mutex_unlock(&used_memory_mutex); \
    } else { \
        used_memory -= _n; \
    } \
} while(0)

// 已使用的内存量
static size_t used_memory = 0;
// 内存是用来线程锁
static int zmalloc_thread_safe = 0;
pthread_mutex_t used_memory_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * 分配内存时产生 OOM 异常
 *
 * @param size
 */
static void zmalloc_oom(size_t size) {
    fprintf(stderr, "zmalloc: Out of memory trying to allocate %zu bytes\n",
            size);
    fflush(stderr);
    abort();
}

/**
 * 分配指定大小的内存块
 *
 * 类似于标准库的malloc()
 *
 * @param size
 * @return
 */
void *zmalloc(size_t size) {
    void *ptr = malloc(size+PREFIX_SIZE);
    if (!ptr) {
        zmalloc_oom(size);
    }
#ifdef HAVE_MALLOC_SIZE
    increment_used_memory(redis_malloc_size(ptr));
    return ptr;
#else
    *((size_t*)ptr) = size;
    increment_used_memory(size+PREFIX_SIZE);
    return (char*)ptr+PREFIX_SIZE;
#endif
}

void *zrealloc(void *ptr, size_t size) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
#endif
    size_t oldsize;
    void *newptr;

    if (ptr == NULL) return zmalloc(size);
#ifdef HAVE_MALLOC_SIZE
    oldsize = redis_malloc_size(ptr);
    newptr = realloc(ptr,size);
    if (!newptr) zmalloc_oom(size);

    decrement_used_memory(oldsize);
    increment_used_memory(redis_malloc_size(newptr));
    return newptr;
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    newptr = realloc(realptr,size+PREFIX_SIZE);
    if (!newptr) zmalloc_oom(size);

    *((size_t*)newptr) = size;
    decrement_used_memory(oldsize);
    increment_used_memory(size);
    return (char*)newptr+PREFIX_SIZE;
#endif
}

void zfree(void *ptr) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
    size_t oldsize;
#endif

    if (ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
    decrement_used_memory(redis_malloc_size(ptr));
    free(ptr);
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    decrement_used_memory(oldsize+PREFIX_SIZE);
    free(realptr);
#endif
}

/**
 * 复制输入字符串，分配足够的内存来存储复制后的字符串，并返回指向新分配内存的指针。
 *
 * @param s
 * @return
 */
char *zstrdup(const char *s) {
    size_t l = strlen(s)+1;
    char *p = zmalloc(l);

    memcpy(p,s,l);
    return p;
}
