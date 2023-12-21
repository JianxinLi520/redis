//
// Created by li on 2023/12/21.
//

#ifndef _ZMALLOC_H
#define _ZMALLOC_H

void *zmalloc(size_t size);
void *zrealloc(void *ptr, size_t size);
void zfree(void *ptr);
char *zstrdup(const char *s);

#endif //_ZMALLOC_H
