/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API    Hash数据类型的操作接口
 *----------------------------------------------------------------------------*/

/* Hash数据类型有两种编码方式：REDIS_ENCODING_ZIPLIST和REDIS_ENCODING_HT，在下面的注释中我们分别称之为ziplist编码和dict编码。 */

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
/*  检查argv数组中每个字符串的长度，看看是否需要将Hash类型对象o从ziplist编码转换为指针的dict编码。
    注意这里我们只检查REDIS_ENCODING_RAW编码的字符串对象，因为它们的长度可以在常数时间内获取到。*/
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    // 如果不是ziplist编码，直接返回
    if (o->encoding != REDIS_ENCODING_ZIPLIST) return;

    // 检查所有输入对象的长度，看看它们的字符串长度是否超过了指定值
    for (i = start; i <= end; i++) {
        // server.hash_max_ziplist_value在redis.conf配置，默认为64
        if (argv[i]->encoding == REDIS_ENCODING_RAW &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            // 将ziplist装换为dict存储
            hashTypeConvert(o, REDIS_ENCODING_HT);
            break;
        }
    }
}

/* Encode given objects in-place when the hash uses a dict. */
/*  当hashType类型对象是dict编码时候，尝试对o1和o2进行编码以节省空间。
    tryObjectEncoding的实现在objcet.c文件中。*/
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2) {
    if (subject->encoding == REDIS_ENCODING_HT) {
    	// tryObjectEncoding定义在object.c文件中
        if (o1) *o1 = tryObjectEncoding(*o1);
        if (o2) *o2 = tryObjectEncoding(*o2);
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
/*  从ziplist编码hashType类型对象中取出key对应的value值。
    ziplist中相邻的两个节点被当做一个键值对，如果value域是字符串则保存在参数vstr中，如果是整数，则保存在参数vll中。
    如果ziplist不存在这样的key节点，函数返回-1，否则返回0。 */
int hashTypeGetFromZiplist(robj *o, robj *field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);

    // 对输入的key值进行解析
    field = getDecodedObject(field);

    zl = o->ptr;
    // 获取第一个节点的首地址
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        // 调用ziplist的ziplistFind在ziplis中查找是否存在值为field->ptr的节点，也就是查找key节点
        fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            // 将相邻的两个节点看做是键值对，找到key节点则下一个节点就是value节点
            vptr = ziplistNext(zl, fptr);
            redisAssert(vptr != NULL);
        }
    }

    decrRefCount(field);

    if (vptr != NULL) {
        // 获取value值
        ret = ziplistGet(vptr, vstr, vlen, vll);
        redisAssert(ret);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
/*  从ziplist编码hashType类型对象中取出key对应的value值，该key由参数field指定。
    如果操作成功，函数返回0，否则返回-1，表示对应的key不存在。 */
int hashTypeGetFromHashTable(robj *o, robj *field, robj **value) {
    dictEntry *de;

    redisAssert(o->encoding == REDIS_ENCODING_HT);

    // 调用dict的dictFind找到指定key值的键值对
    de = dictFind(o->ptr, field);
    // 目标键值对不存在，返回-1
    if (de == NULL) return -1;
    // 获取value值
    *value = dictGetVal(de);
    return 0;
}

/* Higher level function of hashTypeGet*() that always returns a Redis
 * object (either new or with refcount incremented), so that the caller
 * can retain a reference or call decrRefCount after the usage.
 *
 * The lower level function can prevent copy on write so it is
 * the preferred way of doing read operations. */
/*  该函数是hashTypeGetFromZiplist和hashTypeGetFromHashTable的高层封装函数，
    总是返回一个redisObject类型的值对象。
    这是执行读操作的首选方式。*/
robj *hashTypeGetObject(robj *o, robj *field) {
    robj *value = NULL;

    // 处理ziplist编码的情况
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        // 从ziplist中查找
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) {
            // ziplist节点中存储的数据有字符串和整型之分
            if (vstr) {
                value = createStringObject((char*)vstr, vlen);
            } else {
                value = createStringObjectFromLongLong(vll);
            }
        }

    } 
    // 处理dict编码的情况
    else if (o->encoding == REDIS_ENCODING_HT) {
        // 从dict中查找
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) {
            incrRefCount(aux);
            value = aux;
        }
    } else {
        redisPanic("Unknown hash encoding");
    }
    // 返回一个值对象
    return value;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
/* 判断hashType类型中某个给定的key是否存在，如果存在返回1，否则返回0 */
int hashTypeExists(robj *o, robj *field) {
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // 调用hashTypeGetFromZiplist函数在ziplist查找指定key值
        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *aux;

        // 调用hashTypeGetFromHashTable在dict中查找指定key值
        if (hashTypeGetFromHashTable(o, field, &aux) == 0) return 1;
    } else {
        redisPanic("Unknown hash encoding");
    }
    return 0;
}

/* Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update.
 * This function will take care of incrementing the reference count of the
 * retained fields and value objects. */
/*  hashType类型的设置操作：如果指定key值不存在则执行添加操作，否则执行更新操作。
    如果函数返回0，则表示执行了添加操作，返回1，则表示执行了更新操作。
    该函数负责增加field和value对象的引用计数器值。 */
int hashTypeSet(robj *o, robj *field, robj *value) {
    int update = 0;

    // 处理ziplist编码的情况
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        // 解码操作
        field = getDecodedObject(field);
        value = getDecodedObject(value);

        zl = o->ptr;
        // 获取ziplist第一个节点的首地址
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 如果ziplist不为空，则尝试查找指定key值的一个节点
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                // Redis将ziplist中相邻的两个节点当做一个键值对，fptr所指向节点的下一个节点就是value节点
                vptr = ziplistNext(zl, fptr);
                redisAssert(vptr != NULL);
                update = 1;

                /* Delete value */
                // 将旧的value节点删除
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
                // 在原位置插入一个新的value节点
                zl = ziplistInsert(zl, vptr, value->ptr, sdslen(value->ptr));
            }
        }

        // 如果ziplist为空或者不存在指定key值得节点，则在其尾部插入两个节点，分别保存key值和value值
        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            zl = ziplistPush(zl, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
            zl = ziplistPush(zl, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
        }
        // ziplist的添加或删除会引起内存的重新分配，这里需要更新ptr指针
        o->ptr = zl;
        decrRefCount(field);
        decrRefCount(value);

        /* Check if the ziplist needs to be converted to a hash table */
        // 如果ziplist保存的键值对数量超过指定值，则将其转换为dict存储
        // server.hash_max_ziplist_entries配置在redis.conf中，初始值为512
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, REDIS_ENCODING_HT);
    } 
    // 处理dict编码的情况
    else if (o->encoding == REDIS_ENCODING_HT) {

        // dict中有专门的函数来实现setValue的功能，直接调用
        if (dictReplace(o->ptr, field, value)) { /* Insert */
            incrRefCount(field);
        } else { /* Update */
            update = 1;
        }
        incrRefCount(value);
    } else {
        redisPanic("Unknown hash encoding");
    }
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
/* 从hashType类型对象中删除一个指定key值的键值对，如果删除成功，返回1，如果未找到对应的key值则返回0。 */
int hashTypeDelete(robj *o, robj *field) {
    int deleted = 0;

    // 处理ziplist编码的情况
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {

        unsigned char *zl, *fptr;

        field = getDecodedObject(field);

        zl = o->ptr;
        // 获取ziplist第一个节点的首地址
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 在ziplist中查找指定key值得节点
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                // 如果找到目标节点，则连续删除该节点和下一个节点（相邻的两个节点看做一个键值对）
                zl = ziplistDelete(zl,&fptr);
                zl = ziplistDelete(zl,&fptr);
                o->ptr = zl;
                // 设置删除成功标识
                deleted = 1;
            }
        }

        decrRefCount(field);

    } 
    // 处理dict编码的情况
    else if (o->encoding == REDIS_ENCODING_HT) {
        // dict类型定义了专门的函数处理删除操作，直接调用
        if (dictDelete((dict*)o->ptr, field) == REDIS_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            // 检查是否需要进行收缩操作
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        redisPanic("Unknown hash encoding");
    }

    return deleted;
}

/* Return the number of elements in a hash. */
/* 获取hashType类型中保存的键值对个数 */
unsigned long hashTypeLength(robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        // ziplist中两个相邻的节点作为一个键值对，因此需要除以2
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == REDIS_ENCODING_HT) {
        // 直接调用dict的专属函数返回其键值对数量
        length = dictSize((dict*)o->ptr);
    } else {
        redisPanic("Unknown hash encoding");
    }

    return length;
}

/* 创建hashType类型的迭代器并返回，hashTypeIterator结构体定义在redis.h头文件中。 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {
    // 分配空间
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    // 设置源对象指针和编码方式
    hi->subject = subject;
    hi->encoding = subject->encoding;

    // 处理ziplist编码的情况
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    } 
    // 处理dict编码的情况
    else if (hi->encoding == REDIS_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        redisPanic("Unknown hash encoding");
    }

    return hi;
}

/* 释放hashType类型的迭代器 */
void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == REDIS_ENCODING_HT) {
        // 如果是dict类型的迭代器，需要先调用其专属函数来释放资源
        dictReleaseIterator(hi->di);
    }

    zfree(hi);
}

/* Move to the next entry in the hash. Return REDIS_OK when the next entry
 * could be found and REDIS_ERR when the iterator reaches the end. */
/*  通过迭代器获取下一个元素，迭代器有ziplist和dict两种，通过encoding字段区别。如果操作成功返回REDIS_OK
    否则返回REDIS_ERR。 */
int hashTypeNext(hashTypeIterator *hi) {
    // 处理ziplist编码的情况
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        /*  下面这段代码实现了一个简单的ziplist迭代器操作，具体是这样的：第一次调用hashTypeNext时迭代器的fptr指向
            ziplist第一个节点的指针（相当于key），而vptr指向下一个节点（相当于value）。下次调用hashTypeNext函数，
            fptr指向vptr的下一个节点（相当于key），而vptr又更新为指向fptr的下一个节点（相当于value）。这样每次调用
            hashTypeNext函数，迭代器的fptr和vptr都指向两个相邻的节点，相当于一个key-value对。
        */
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            redisAssert(vptr == NULL);
            // 获得第一个节点的首地址
            fptr = ziplistIndex(zl, 0);
        } else {
            /* Advance cursor */
            redisAssert(vptr != NULL);
            // 获得vptr节点的下一个节点的首地址
            fptr = ziplistNext(zl, vptr);
        }
        if (fptr == NULL) return REDIS_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        // vptr指向fptr的下一个节点，相当于value值
        vptr = ziplistNext(zl, fptr);
        redisAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        // 此时，fptr和vptr指向相邻的两个节点，相当于一个key-value对
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        // 如果是dict对象的迭代器，直接调用dict自身的迭代器实现
        if ((hi->de = dictNext(hi->di)) == NULL) return REDIS_ERR;
    } else {
        redisPanic("Unknown hash encoding");
    }
    return REDIS_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. */
/*  从ziplist编码的hashType对象中获取当前迭代器所指向的键值对的key值和value值。前面我们分析过，
    Redis将ziplist的迭代器指向的两个相邻的节点当做一个键值对。参数what指明获取key值还是value值。 */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    // 只支持ziplist结构
    redisAssert(hi->encoding == REDIS_ENCODING_ZIPLIST);

    // what的取值有REDIS_HASH_KEY和REDIS_HASH_VALUE之分
    if (what & REDIS_HASH_KEY) {
        // 取key值，也就是fptr指向节点的值
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        redisAssert(ret);
    } else {
        // 取value值，也就是vptr指向节点的值
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        redisAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromHashTable`. */
/* 根据当前迭代器的位置，从dict编码的hashType对象中获取相关的key值或value值。 */ 
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst) {
    // 该函数只处理dict编码的情况
    redisAssert(hi->encoding == REDIS_ENCODING_HT);

    if (what & REDIS_HASH_KEY) {
        *dst = dictGetKey(hi->de);
    } else {
        *dst = dictGetVal(hi->de);
    }
}

/* A non copy-on-write friendly but higher level version of hashTypeCurrent*()
 * that returns an object with incremented refcount (or a new object). It is up
 * to the caller to decrRefCount() the object if no reference is retained. */
/*  根据当前迭代器的位置，从hashType对象中获取指定的key值或value值，其实就是hashTypeCurrentFromZiplist
    和hashTypeCurrentFromHashTable函数的封装。
    该函数会返回一个增加了引用计数值的对象或者一个新对象，需要调用者调用decrRefCount函数减少引用计数值。 */
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what) {
    robj *dst;

    // 处理ziplist编码的情况
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        // ziplist中存储的数据有字符串和整型之分。
        // 这里创建了新对象
        if (vstr) {
            dst = createStringObject((char*)vstr, vlen);
        } else {
            dst = createStringObjectFromLongLong(vll);
        }

    } 
    // 处理dict编码的情况
    else if (hi->encoding == REDIS_ENCODING_HT) {
        hashTypeCurrentFromHashTable(hi, what, &dst);
        // 这里增加引用计数值
        incrRefCount(dst);

    } else {
        redisPanic("Unknown hash encoding");
    }

    return dst;
}

/* 判断给定名称相应的hash对象是否存在，如果不存在则创建 */
robj *hashTypeLookupWriteOrCreate(redisClient *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);
    } else {
    	// 如果该名称的对象并不是hash类型，则报错
        if (o->type != REDIS_HASH) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    return o;
}

/* 从ziplist编码到dict编码的转换，参数enc指明目标编码方式。 */
void hashTypeConvertZiplist(robj *o, int enc) {
    // 原对象必须为ziplist编码
    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);

    if (enc == REDIS_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == REDIS_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        // 获取待转换ziplist对象的迭代器
        hi = hashTypeInitIterator(o);
        // 创建一个空的dict结构
        dict = dictCreate(&hashDictType, NULL);

        // 从前往后遍历ziplist，每次取出两个节点作为key-value对添加到dict中
        while (hashTypeNext(hi) != REDIS_ERR) {
            robj *field, *value;

            // 获取当前迭代器指向的key值
            field = hashTypeCurrentObject(hi, REDIS_HASH_KEY);
            field = tryObjectEncoding(field);
            // 获取当前迭代器指向的value值
            value = hashTypeCurrentObject(hi, REDIS_HASH_VALUE);
            value = tryObjectEncoding(value);
            // 将当前的key和value添加到dict中
            ret = dictAdd(dict, field, value);
            if (ret != DICT_OK) {
                redisLogHexDump(REDIS_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                redisAssert(ret == DICT_OK);
            }
        }

        // 释放迭代器
        hashTypeReleaseIterator(hi);
        // 释放原ziplist对象空间
        zfree(o->ptr);

        // 更新redis object对象信息
        o->encoding = REDIS_ENCODING_HT;
        o->ptr = dict;

    } else {
        redisPanic("Unknown hash encoding");
    }
}

/* 内置存储类型转换，只支持从ziplist到dict的转换 */
void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        // 调用hashTypeConvertZiplist完成真正的转换操作
        hashTypeConvertZiplist(o, enc);
    } else if (o->encoding == REDIS_ENCODING_HT) {
        redisPanic("Not implemented");
    } else {
        redisPanic("Unknown hash encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Hash type commands Hash数据类型相关命令的实现
 *----------------------------------------------------------------------------*/

/* hset命令的实现，一次设置一个键值对 */
void hsetCommand(redisClient *c) {
    int update;
    robj *o;

    // 检测hash对象是否需要创建
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 检测该hash对象是否需要转换编码方式
    hashTypeTryConversion(o,c->argv,2,3);
    hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);
    // 正在执行set操作
    update = hashTypeSet(o,c->argv[2],c->argv[3]);

    // 下面这个函数我们暂时不予理会，以后遇到再继续分析
    addReply(c, update ? shared.czero : shared.cone);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty++;
}

//	hsetnx命令的实现（如果key不存在则创建，如果key存在则什么事也不做）
void hsetnxCommand(redisClient *c) {
    robj *o;

    // 检测hash对象是否需要创建
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 检测该hash对象是否需要转换编码方式
    hashTypeTryConversion(o,c->argv,2,3);

    // 先判断hash对象中是否有指定的key
    if (hashTypeExists(o, c->argv[2])) {
        addReply(c, shared.czero);
    } else {
    	// 如果key不存在，则创建之
        hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);
        hashTypeSet(o,c->argv[2],c->argv[3]);
        addReply(c, shared.cone);
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        server.dirty++;
    }
}

/* hmset命令的实现，一次设置多个键值对 */
void hmsetCommand(redisClient *c) {
    int i;
    robj *o;

    // 验证参数是否合法，如hmset myhash name fred age 23，参数的个数应为偶数个
    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    // 检测hash对象是否需要创建
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 检测该hash对象是否需要转换编码方式
    hashTypeTryConversion(o,c->argv,2,c->argc-1);
    // 两两一组组成一个键值对分别插入
    for (i = 2; i < c->argc; i += 2) {
        hashTypeTryObjectEncoding(o,&c->argv[i], &c->argv[i+1]);
        hashTypeSet(o,c->argv[i],c->argv[i+1]);
    }
    // 插入完成后的操作
    addReply(c, shared.ok);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty++;
}

/* hincrby命令的实现，对指定键进行增量操作 */
void hincrbyCommand(redisClient *c) {
    long long value, incr, oldvalue;
    robj *o, *current, *new;

    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != REDIS_OK) return;
    // 检测hash对象是否需要创建
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 在hash对象中获取指定key的value值
    if ((current = hashTypeGetObject(o,c->argv[2])) != NULL) {
        if (getLongLongFromObjectOrReply(c,current,&value,
            "hash value is not an integer") != REDIS_OK) {
            decrRefCount(current);
            return;
        }
        decrRefCount(current);
    } else {
        value = 0;
    }

    oldvalue = value;
    // 确保增量操作后不会超过Long Long数值的表示范围
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr;
    // 构造新值
    new = createStringObjectFromLongLong(value);
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);
    // 设置新值
    hashTypeSet(o,c->argv[2],new);

    decrRefCount(new);
    addReplyLongLong(c,value);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    server.dirty++;
}

/* hincrbyfloat命令的实现，类似hincrby命令  */
void hincrbyfloatCommand(redisClient *c) {
    double long value, incr;
    robj *o, *current, *new, *aux;

    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != REDIS_OK) return;
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    if ((current = hashTypeGetObject(o,c->argv[2])) != NULL) {
        if (getLongDoubleFromObjectOrReply(c,current,&value,
            "hash value is not a valid float") != REDIS_OK) {
            decrRefCount(current);
            return;
        }
        decrRefCount(current);
    } else {
        value = 0;
    }

    value += incr;
    new = createStringObjectFromLongDouble(value,1);
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);
    hashTypeSet(o,c->argv[2],new);
    addReplyBulk(c,new);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    aux = createStringObject("HSET",4);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,3,new);
    decrRefCount(new);
}

/* helper函数：将hashType对象中指定key对应的value值放入回复消息中。*/
static void addHashFieldToReply(redisClient *c, robj *o, robj *field) {
    int ret;

    // 目标对象为空
    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    // 处理ziplist编码的情况
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // 从ziplist中取出key指定的value值 
        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            // 字符串 or 整型
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    } 
    // 处理dict编码的情况
    else if (o->encoding == REDIS_ENCODING_HT) {
        robj *value;

        // 从dict中取出key指定的value值 
        ret = hashTypeGetFromHashTable(o, field, &value);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            addReplyBulk(c, value);
        }

    } else {
        redisPanic("Unknown hash encoding");
    }
}

/* hget命令实现 */
void hgetCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    addHashFieldToReply(c, o, c->argv[2]);
}

/* hmget命令实现 */
void hmgetCommand(redisClient *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    o = lookupKeyRead(c->db, c->argv[1]);
    if (o != NULL && o->type != REDIS_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    addReplyMultiBulkLen(c, c->argc-2);
    // 遍历所有输入的key值，取出相对应的value值
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]);
    }
}

/* hdel命令实现 */
void hdelCommand(redisClient *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    // 取出hashType对象并进行类型检查
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    // 遍历所有输入的待删除key，逐一执行删除操作
    for (j = 2; j < c->argc; j++) {
        // 删除指定key值和相应的value值（键值对）
        if (hashTypeDelete(o,c->argv[j])) {
            // 记录成功删除的键值对个数
            deleted++;

            // 如何hashType对象为空，删除之
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }

    // 如果至少有1个键值对被删除
    if (deleted) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }

    // 返回成功删除的键值对数量的个数
    addReplyLongLong(c,deleted);
}

/* hlen命令实现 */
void hlenCommand(redisClient *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    addReplyLongLong(c,hashTypeLength(o));
}

/* 根据当前迭代器，取出listType对象的key值或value值并添加到回复消息中。*/
static void addHashIteratorCursorToReply(redisClient *c, hashTypeIterator *hi, int what) {
    // 处理ziplist编码的情况
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }

    } 
    // 处理dict编码的情况
    else if (hi->encoding == REDIS_ENCODING_HT) {
        robj *value;

        hashTypeCurrentFromHashTable(hi, what, &value);
        addReplyBulk(c, value);

    } else {
        redisPanic("Unknown hash encoding");
    }
}

/* 取出hashType对象中的所有key值或value值，由参数flags决定。*/
void genericHgetallCommand(redisClient *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0;

    // 取出listType对象并进行类型检查
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
        || checkType(c,o,REDIS_HASH)) return;

    if (flags & REDIS_HASH_KEY) multiplier++;
    if (flags & REDIS_HASH_VALUE) multiplier++;

    length = hashTypeLength(o) * multiplier;
    addReplyMultiBulkLen(c, length);

    // 获取迭代器
    hi = hashTypeInitIterator(o);
    // 遍历键值对，取出key或value
    while (hashTypeNext(hi) != REDIS_ERR) {
        if (flags & REDIS_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_KEY);
            count++;
        }
        if (flags & REDIS_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_VALUE);
            count++;
        }
    }

    hashTypeReleaseIterator(hi);
    redisAssert(count == length);
}

/* hkeys命令实现 */
void hkeysCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_KEY);
}

/* hvals命令实现 */
void hvalsCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_VALUE);
}

/* hgetall命令实现 */
void hgetallCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_KEY|REDIS_HASH_VALUE);
}

/* hexists命令实现 */
void hexistsCommand(redisClient *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    addReply(c, hashTypeExists(o,c->argv[2]) ? shared.cone : shared.czero);
}

/* hscan命令实现 */
void hscanCommand(redisClient *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;
    // scanGenericCommand命令定义在db.c命令中
    scanGenericCommand(c,o,cursor);
}
