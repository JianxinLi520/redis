//
// Created by li on 2023/12/22.
//

#ifndef REDIS_1_3_6_REPRODUCTION_ZIPMAP_H
#define REDIS_1_3_6_REPRODUCTION_ZIPMAP_H

unsigned char *zipmapNew(void);
unsigned char *zipmapSet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char *val, unsigned int vlen, int *update);
unsigned char *zipmapDel(unsigned char *zm, unsigned char *key, unsigned int klen, int *deleted);
unsigned char *zipmapRewind(unsigned char *zm);
unsigned char *zipmapNext(unsigned char *zm, unsigned char **key, unsigned int *klen, unsigned char **value, unsigned int *vlen);
int zipmapGet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char **value, unsigned int *vlen);
int zipmapExists(unsigned char *zm, unsigned char *key, unsigned int klen);
unsigned int zipmapLen(unsigned char *zm);
void zipmapRepr(unsigned char *p);

#endif //REDIS_1_3_6_REPRODUCTION_ZIPMAP_H
