cmake_minimum_required(VERSION 3.21)
project(redis_1_3_6_reproduction C)

set(CMAKE_C_STANDARD 99)

add_executable(
        redis_1_3_6_reproduction redis.c dict.h zmalloc.c zmalloc.h adlist.c adlist.h anet.c anet.h
        ae.c sds.c sds.h config.h dict.c zipmap.c zipmap.h lzf.h lzfP.h lzf_c.c lzf_d.c)
