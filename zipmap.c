//
// Created by li on 2023/12/22.
//

#include "zipmap.h"
#include "zmalloc.h"

#define ZIPMAP_BIGLEN 253
#define ZIPMAP_EMPTY 254
#define ZIPMAP_END 255

/* Create a new empty zipmap. */
unsigned char *zipmapNew(void) {
    unsigned char *zm = zmalloc(2);

    zm[0] = 0; /* Status */
    zm[1] = ZIPMAP_END;
    return zm;
}