/**
 * zmalloc - total amount of allocated memory aware version of malloc()
 *
 * zmalloc - 用于分配内存，基于malloc()函数
 */

#ifndef _ZMALLOC_H
#define _ZMALLOC_H

/**
 * 分配指定大小的内存块
 *
 * 类似于标准库的malloc()
 *
 * @param size
 * @return
 */
void *zmalloc(size_t size);

/**
 * 重新分配已分配内存块的大小，这对于动态调整内存大小很有用。
 *
 * 类似于标准库的 realloc()
 *
 * @param ptr
 * @param size
 * @return
 */
void *zrealloc(void *ptr, size_t size);

/**
 * 释放通过 zmalloc() 获取的内存块
 *
 * 类似于标准库的free()
 *
 * @param ptr
 */
void zfree(void *ptr);

/**
 * 复制输入字符串，分配足够的内存来存储复制后的字符串，并返回指向新分配内存的指针。
 *
 * @param s
 * @return
 */
char *zstrdup(const char *s);

/**
 * 获取zalloc()实用的内存总量
 * @return
 */
size_t zmalloc_used_memory(void);

#endif //_ZMALLOC_H
