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

#include <signal.h>
#include <ctype.h>

void SlotToKeyAdd(robj *key);
void SlotToKeyDel(robj *key);

/*-----------------------------------------------------------------------------
 * C-level DB API
 *----------------------------------------------------------------------------*/

/*  从Redis数据库db中取出指定key的对象（即五种不同的数据类型对象），
    如果key存在，则返回相应对象，否则返回NULL。   */
robj *lookupKey(redisDb *db, robj *key) {
    // 查找指定key对应的dictEntry结构体，里面存放键值对信息
    dictEntry *de = dictFind(db->dict,key->ptr);
    // 节点存在
    if (de) {
        // 取得相应的value，即目标对象
        robj *val = dictGetVal(de);

        /* Update the access time for the ageing algorithm.
         * Don't do it if we have a saving child, as this will trigger
         * a copy on write madness. */
        /* 更新时间信息 */
        if (server.rdb_child_pid == -1 && server.aof_child_pid == -1)
            val->lru = server.lruclock;
        return val;
    } 
    // 节点不存在
    else {
        return NULL;
    }
}

/*  从函数名可以看出，该函数是为读操作而从数据库db中取出指定key的对象。
    如果敢函数执行成功则返回目标对象，否则返回NULL。
    该函数最后还根据是否成功取出对象更新服务器的命中/不命中次数。 */
robj *lookupKeyRead(redisDb *db, robj *key) {
    robj *val;

    // 如果key已过期，删除该key
    expireIfNeeded(db,key);
    // 从数据库db中找到指定key的对象
    val = lookupKey(db,key);
    if (val == NULL)
        // 更新“未命中”次数
        server.stat_keyspace_misses++;
    else
        // 更新“命中”次数
        server.stat_keyspace_hits++;
    return val;
}

/*  从函数名可以看出，该函数是为写操作而从数据库db中取出指定key的对象。
    如果敢函数执行成功则返回目标对象，否则返回NULL。*/
robj *lookupKeyWrite(redisDb *db, robj *key) {
    // 如果key已过期，删除该key
    expireIfNeeded(db,key);
    // 从数据库db中找到指定key的对象
    return lookupKey(db,key);
}

/*  为读操作而从数据库db中取出指定key的对象，如果目标对象存在则直接返回。
    如果目标对象不存在，则向客户端发送参数reply指定的消息然后返回NULL。  */
robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply) {
    // 查找目标对象
    robj *o = lookupKeyRead(c->db, key);
    // 目标对象不存在，向客户端发送消息
    if (!o) addReply(c,reply);
    return o;
}

/*  为写操作而从数据库db中取出指定key的对象，如果目标对象存在则直接返回。
    如果目标对象不存在，则向客户端发送参数reply指定的消息然后返回NULL。  */
robj *lookupKeyWriteOrReply(redisClient *c, robj *key, robj *reply) {
    // 查找目标对象
    robj *o = lookupKeyWrite(c->db, key);
    // 目标对象不存在，向客户端发送消息
    if (!o) addReply(c,reply);
    return o;
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
/*  往数据库db中添加一个由参数key和参数val指定的键值对，由调用者负责增加key和val的引用计数值。
    如果该key已经存在，则程序abort。*/
void dbAdd(redisDb *db, robj *key, robj *val) {
    // 复制key
    sds copy = sdsdup(key->ptr);
    // 往db中添加键值对
    int retval = dictAdd(db->dict, copy, val);

    redisAssertWithInfo(NULL,key,retval == REDIS_OK);
    if (val->type == REDIS_LIST) signalListAsReady(db, key);
 }

/* Overwrite an existing key with a new value. Incrementing the reference
 * count of the new value is up to the caller.
 * This function does not modify the expire time of the existing key.
 *
 * The program is aborted if the key was not already present. */
/*  重写一个key关联的值，即为一个存在的key设置一个新值，由函数调用者增加val的引用计数值。
    该函数不会修改key的过期时间。
    如果指定key不存在，则程序abort。  */
void dbOverwrite(redisDb *db, robj *key, robj *val) {
    // 在db中查找指定的键值对
    struct dictEntry *de = dictFind(db->dict,key->ptr);

    // 如果指定key的键值对不存在，则abort
    redisAssertWithInfo(NULL,key,de != NULL);
    // 赋新值
    dictReplace(db->dict, key->ptr, val);
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The ref count of the value object is incremented.
 * 2) clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent). */
/*  上层set操作，该函数的作用在于：为一个key设置新值，不管该key是否已经存在。
    具体的策略是：如果key存在，则重写value值，如果key不存在，则添加之。

    注意点：
    （1）、该函数负责增加key的引用计数值。
    （2）、监视该key的客户端会收到相应的通知。
    （3）、键的过期时间被重置，变为永久有效。   */
void setKey(redisDb *db, robj *key, robj *val) {
    // 先确定指定key是否存在
    if (lookupKeyWrite(db,key) == NULL) {
        // 指定key不存在，添加
        dbAdd(db,key,val);
    } else {
        // 指定key存在，重写
        dbOverwrite(db,key,val);
    }
    // 增加val的引用计数值
    incrRefCount(val);
    // 删除key的过期时间
    removeExpire(db,key);
    // 发送通知
    signalModifiedKey(db,key);
}

/* 判断某个key是否存在 */
int dbExists(redisDb *db, robj *key) {
    return dictFind(db->dict,key->ptr) != NULL;
}

/* Return a random key, in form of a Redis object.
 * If there are no keys, NULL is returned.
 *
 * The function makes sure to return keys not already expired. */
/*  随机从数据库db中取出一个key并返回。
    如果数据库db为空，则返回NULL。
    该函数保证返回的key是未过期的。   */
robj *dbRandomKey(redisDb *db) {
    struct dictEntry *de;

    // 一直随机查找，知道找到未过期的键
    while(1) {
        sds key;
        robj *keyobj;

        // 随机获取一个键值对，dictGetRandomKey是dict内部的函数
        de = dictGetRandomKey(db->dict);
        // 如果数据库db为空，返回NULL
        if (de == NULL) return NULL;

        // 获取键值对中的key
        key = dictGetKey(de);
        keyobj = createStringObject(key,sdslen(key));
        // 判断该key是否过期
        if (dictFind(db->expires,key)) {
            // 如果key过期，则删除该key
            if (expireIfNeeded(db,keyobj)) {
                decrRefCount(keyobj);
                // 继续查找
                continue; /* search for another key. This expired. */
            }
        }
        // 返回key
        return keyobj;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
/*  从数据库中删除一个给定的key、相应的值value以及该key的过期时间。   
    如果操作成功返回1，否则返回0。    */
int dbDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    //  从db->expires中删除一个键值对并不会释放key字符串对象，因为db->expires和db->dict是共享
    //  key字符串对象的。

    // 删除key的过期时间
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);
    // 从键空间中删除key和相应的value
    if (dictDelete(db->dict,key->ptr) == DICT_OK) {
        return 1;
    } else {
        return 0;
    }
}

/* Prepare the string object stored at 'key' to be modified destructively
 * to implement commands like SETBIT or APPEND.
 *
 * An object is usually ready to be modified unless one of the two conditions
 * are true:
 *
 * 1) The object 'o' is shared (refcount > 1), we don't want to affect
 *    other users.
 * 2) The object encoding is not "RAW".
 *
 * If the object is found in one of the above conditions (or both) by the
 * function, an unshared / not-encoded copy of the string object is stored
 * at 'key' in the specified 'db'. Otherwise the object 'o' itself is
 * returned.
 *
 * USAGE:
 *
 * The object 'o' is what the caller already obtained by looking up 'key'
 * in 'db', the usage pattern looks like this:
 *
 * o = lookupKeyWrite(db,key);
 * if (checkType(c,o,REDIS_STRING)) return;
 * o = dbUnshareStringValue(db,key,o);
 *
 * At this point the caller is ready to modify the object, for example
 * using an sdscat() call to append some data, or anything else.
 */
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o) {
    redisAssert(o->type == REDIS_STRING);
    if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW) {
        robj *decoded = getDecodedObject(o);
        o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
        decrRefCount(decoded);
        dbOverwrite(db,key,o);
    }
    return o;
}

/* 情况服务器中的所有数据，返回并删除key的数量。 */
long long emptyDb(void(callback)(void*)) {
    int j;
    long long removed = 0;

    // 将该服务器中所有的数据库db清空
    for (j = 0; j < server.dbnum; j++) {
        // 记录被删除key的数量
        removed += dictSize(server.db[j].dict);
        dictEmpty(server.db[j].dict,callback);
        dictEmpty(server.db[j].expires,callback);
    }
    return removed;
}

/* 切换为参数id指定的数据库，如果操作成功返回REDIS_OK，否则返回REDIS_ERR。   */
int selectDb(redisClient *c, int id) {
    // 验证参数id是否正确，server.dbnum默认值为16
    if (id < 0 || id >= server.dbnum)
        return REDIS_ERR;
    // 切换数据库，就是简单地设置指针
    c->db = &server.db[id];
    return REDIS_OK;
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *----------------------------------------------------------------------------*/

/*  每当数据库中的key被修改后，都要调用该函数。*/
void signalModifiedKey(redisDb *db, robj *key) {
    touchWatchedKey(db,key);
}

/*  每当一个数据库被flushdb命令情况后都要调用该函数。 */
void signalFlushedDb(int dbid) {
    touchWatchedKeysOnFlush(dbid);
}

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *  键空间中与类型无关的命令
 *----------------------------------------------------------------------------*/

/* flushdb命令实现，清空指定数据库  */
void flushdbCommand(redisClient *c) {
    server.dirty += dictSize(c->db->dict);
    signalFlushedDb(c->db->id);
    // 清空键空间和过期时间
    dictEmpty(c->db->dict,NULL);
    dictEmpty(c->db->expires,NULL);
    // 发送回复信息
    addReply(c,shared.ok);
}

/* 清空所有数据库  */
void flushallCommand(redisClient *c) {
    signalFlushedDb(-1);
    // 清空所有数据库
    server.dirty += emptyDb(NULL);
    addReply(c,shared.ok);
    // 如果正在保存RDB，取消该操作。关于RDB我们以后再分析
    if (server.rdb_child_pid != -1) {
        kill(server.rdb_child_pid,SIGUSR1);
        rdbRemoveTempFile(server.rdb_child_pid);
    }
    // 更新RDB文件
    if (server.saveparamslen > 0) {
        /* Normally rdbSave() will reset dirty, but we don't want this here
         * as otherwise FLUSHALL will not be replicated nor put into the AOF. */
        // rdbSave函数会重置server.dirty属性，为了确保FLUSHALL正常传播，需要在rdbSave函数后还原server.dirty属性
        int saved_dirty = server.dirty;
        rdbSave(server.rdb_filename);
        server.dirty = saved_dirty;
    }
    server.dirty++;
}

/* DEL命令实现，删除key。 */
void delCommand(redisClient *c) {
    int deleted = 0, j;

    // 遍历所有的输入key，逐一删除
    for (j = 1; j < c->argc; j++) {
        // 如果该key已过期，删除
        expireIfNeeded(c->db,c->argv[j]);
        // 调用dbDelete删除key，若删除成功返回1
        if (dbDelete(c->db,c->argv[j])) {
            // 成功删除key，发送通知
            signalModifiedKey(c->db,c->argv[j]);
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,
                "del",c->argv[j],c->db->id);
            server.dirty++;
            // 记录成功被删除key的数量
            deleted++;
        }
    }
    addReplyLongLong(c,deleted);
}

/* EXISTS命令实现，检查给定的key是否存在。 */
void existsCommand(redisClient *c) {
    // 检查该key是否过期，如果过期则删除
    expireIfNeeded(c->db,c->argv[1]);

    // 调用dbExists函数在数据库db中检查该键是否存在
    if (dbExists(c->db,c->argv[1])) {
        addReply(c, shared.cone);
    } else {
        addReply(c, shared.czero);
    }
}

/* SELECT命令，切换到指定数据库。*/
void selectCommand(redisClient *c) {
    long id;

    // 取得目标数据库id，如果输入值不合法则返回
    if (getLongFromObjectOrReply(c, c->argv[1], &id,
        "invalid DB index") != REDIS_OK)
        return;

    // 切换到指定数据库
    if (selectDb(c,id) == REDIS_ERR) {
        addReplyError(c,"invalid DB index");
    } else {
        addReply(c,shared.ok);
    }
}

/* RANDOMKEY命令实现，随机返回一个key。*/
void randomkeyCommand(redisClient *c) {
    robj *key;

    // 通过调用dbRandomKey函数实现
    if ((key = dbRandomKey(c->db)) == NULL) {
        // 数据库db为空，发送空回复
        addReply(c,shared.nullbulk);
        return;
    }

    addReplyBulk(c,key);
    decrRefCount(key);
}

/* KEYS命令，返回所有符合给定模式的key。*/
void keysCommand(redisClient *c) {
    dictIterator *di;
    dictEntry *de;
    // 获取模式字符串
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    void *replylen = addDeferredMultiBulkLength(c);

    di = dictGetSafeIterator(c->db->dict);
    // 如果模式字符串的值为‘*’，表示匹配任意值
    allkeys = (pattern[0] == '*' && pattern[1] == '\0');
    // 遍历键空间字典，将每个key与模式字符串比较
    while((de = dictNext(di)) != NULL) {
        // 获取当前key
        sds key = dictGetKey(de);
        robj *keyobj;

        // 将key于模式字符串比较
        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            keyobj = createStringObject(key,sdslen(key));
            // 检查该key是否已经过期
            if (expireIfNeeded(c->db,keyobj) == 0) {
                // 如果该key未过期，加入到回复消息中
                addReplyBulk(c,keyobj);
                // 记录返回结果数量
                numkeys++;
            }
            decrRefCount(keyobj);
        }
    }
    // 释放迭代器对象
    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c,replylen,numkeys);
}

/* This callback is used by scanGenericCommand in order to collect elements
 * returned by the dictionary iterator into a list. */
/*  该函数为scanGenericCommand的回调函数，将给定dictEntry结构中的key和value存入一个链表list中。*/
void scanCallback(void *privdata, const dictEntry *de) {
    // 由下面的scanGenericCommand函数可以知道，privdata[0]指向相应的key，privdata[1]指向相应的对象;
    void **pd = (void**) privdata;
    list *keys = pd[0];
    robj *o = pd[1];
    robj *key, *val = NULL;

    // 根据当前遍历的不同类型的对象进行相应的处理
    if (o == NULL) {
        sds sdskey = dictGetKey(de);
        key = createStringObject(sdskey, sdslen(sdskey));
    } else if (o->type == REDIS_SET) {
        key = dictGetKey(de);
        incrRefCount(key);
    } else if (o->type == REDIS_HASH) {
        key = dictGetKey(de);
        incrRefCount(key);
        // 如果是哈希类型对象，还要取出key对应的值
        val = dictGetVal(de);
        incrRefCount(val);
    } else if (o->type == REDIS_ZSET) {
        key = dictGetKey(de);
        incrRefCount(key);
        val = createStringObjectFromLongDouble(*(double*)dictGetVal(de),0);
    } else {
        redisPanic("Type not handled in SCAN callback.");
    }

    listAddNodeTail(keys, key);
    // 如果是哈希类型对象，将其值也加入链表list中
    if (val) listAddNodeTail(keys, val);
}

/* Try to parse a SCAN cursor stored at object 'o':
 * if the cursor is valid, store it as unsigned integer into *cursor and
 * returns REDIS_OK. Otherwise return REDIS_ERR and send an error to the
 * client. */
/*  尝试从对象o中解析出SCAN游标：
    如果游标合法，将其当做一个无符号整型并存入参数cursor中，函数返回REDIS_OK，否则返回REDIS_ERR并
    向客户端发送一个错误消息。*/
int parseScanCursorOrReply(redisClient *c, robj *o, unsigned long *cursor) {
    char *eptr;

    /* Use strtoul() because we need an *unsigned* long, so
     * getLongLongFromObject() does not cover the whole cursor space. */
    errno = 0;
    *cursor = strtoul(o->ptr, &eptr, 10);
    if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' || errno == ERANGE)
    {
        addReplyError(c, "invalid cursor");
        return REDIS_ERR;
    }
    return REDIS_OK;
}

/* This command implements SCAN, HSCAN and SSCAN commands.
 * If object 'o' is passed, then it must be a Hash or Set object, otherwise
 * if 'o' is NULL the command will operate on the dictionary associated with
 * the current database.
 *
 * When 'o' is not NULL the function assumes that the first argument in
 * the client arguments vector is a key so it skips it before iterating
 * in order to parse options.
 *
 * In the case of a Hash object the function returns both the field and value
 * of every element on the Hash. */
/*  该函数实现了SCAN、HSCAN、SSCAN命令的操作。
    如果指定对象o，则该对象必须是一个hash对象或set对象。如果参数o为NULL，函数将在当前数据库db上进行操作。
    如果参数o非NULL，
    如果参数o是一个hash对象，则该函数返回的hash对象中的key和值value。*/
void scanGenericCommand(redisClient *c, robj *o, unsigned long cursor) {
    int i, j;
    list *keys = listCreate();
    listNode *node, *nextnode;
    // count选项默认值为10
    long count = 10;
    sds pat;
    int patlen, use_pattern = 0;
    dict *ht;

    /* Object must be NULL (to iterate keys names), or the type of the object
     * must be Set, Sorted Set, or Hash. */
    // 参数o只能是NULL、hash对象、set对象或者sorted set对象
    redisAssert(o == NULL || o->type == REDIS_SET || o->type == REDIS_HASH ||
                o->type == REDIS_ZSET);

    /* Set i to the first option argument. The previous one is the cursor. */
    // 将i设置为第一个选项参数的下标，i的前一个为游标参数
    // 如果o为NULL，则默认在数据库db上进行操作，否则必须指明key。前者i为2，后者为3
    // 示例：scan 0 和 scan myset 0
    i = (o == NULL) ? 2 : 3; /* Skip the key argument if needed. */

    /* Step 1: Parse options. */
    // 步骤1：解析选项参数
    while (i < c->argc) {
        j = c->argc - i;
        // count选项
        if (!strcasecmp(c->argv[i]->ptr, "count") && j >= 2) {
            // 读取count选项值，如果读取失败，进行相应的清理操作
            if (getLongFromObjectOrReply(c, c->argv[i+1], &count, NULL)
                != REDIS_OK)
            {
                goto cleanup;
            }

            if (count < 1) {
                addReply(c,shared.syntaxerr);
                goto cleanup;
            }

            i += 2;
        } 
        // match选项
        else if (!strcasecmp(c->argv[i]->ptr, "match") && j >= 2) {
            pat = c->argv[i+1]->ptr;
            patlen = sdslen(pat);

            /* The pattern always matches if it is exactly "*", so it is
             * equivalent to disabling it. */
            // 判断match的选项是否为‘*’，即匹配全部
            use_pattern = !(pat[0] == '*' && patlen == 1);

            i += 2;
        } 
        // 除了count、match，其余都是错误的
        else {
            addReply(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    /* Step 2: Iterate the collection.
     *
     * Note that if the object is encoded with a ziplist, intset, or any other
     * representation that is not a hash table, we are sure that it is also
     * composed of a small number of elements. So to avoid taking state we
     * just return everything inside the object in a single call, setting the
     * cursor to zero to signal the end of the iteration. */
    //  步骤2：迭代集合
    //  如果目标对象是ziplist、intset或者其它非哈希表编码，那么该对象只包含少量的元素。
    //  在这种情况下，为了避免服务器记录迭代状态，我们将一次性返回该对象的所有元素，同时将游标设置为0表示迭代介绍。

    /* Handle the case of a hash table. */
    // 下面处理哈希表编码的情况
    ht = NULL;
    // 目标对象为NULL，在数据库db上操作
    if (o == NULL) {
        ht = c->db->dict;
    } else if (o->type == REDIS_SET && o->encoding == REDIS_ENCODING_HT) {
        ht = o->ptr;
    } else if (o->type == REDIS_HASH && o->encoding == REDIS_ENCODING_HT) {
        ht = o->ptr;
        // 如果是哈希表编码，既要返回key又要返回value，故需乘2。下同
        count *= 2; /* We return key / value for this type. */
    } else if (o->type == REDIS_ZSET && o->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        ht = zs->dict;
        count *= 2; /* We return key / value for this type. */
    }

    // 处理哈希表编码
    if (ht) {
        void *privdata[2];
        /* We set the max number of iterations to ten times the specified
         * COUNT, so if the hash table is in a pathological state (very
         * sparsely populated) we avoid to block too much time at the cost
         * of returning no or very few elements. */
        // 将最大的迭代次数设置为COUNT值的10倍大小
        long maxiterations = count*10;

        /* We pass two pointers to the callback: the list to which it will
         * add new elements, and the object containing the dictionary so that
         * it is possible to fetch more data in a type-dependent way. */
        // 我们向回调函数中传入两个指针：一个是用来存放迭代元素的链表list，另一个是保存字典结构的迭代对象
        privdata[0] = keys;
        privdata[1] = o;
        do {
            cursor = dictScan(ht, cursor, scanCallback, privdata);
        } while (cursor &&
              maxiterations-- &&
              listLength(keys) < (unsigned long)count);
    } 
    // 处理inset编码的zet对象
    else if (o->type == REDIS_SET) {
        int pos = 0;
        int64_t ll;

        // 一次性遍历inset，将当前元素放入keys链表中
        while(intsetGet(o->ptr,pos++,&ll))
            listAddNodeTail(keys,createStringObjectFromLongLong(ll));
        // 游标置0
        cursor = 0;
    } 
    // 处理ziplist编码的hash对象和zset对象
    else if (o->type == REDIS_HASH || o->type == REDIS_ZSET) {
        unsigned char *p = ziplistIndex(o->ptr,0);
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;

        // 一次性遍历ziplist，将当前元素放入keys链表中
        while(p) {
            ziplistGet(p,&vstr,&vlen,&vll);
            listAddNodeTail(keys,
                (vstr != NULL) ? createStringObject((char*)vstr,vlen) :
                                 createStringObjectFromLongLong(vll));
            p = ziplistNext(o->ptr,p);
        }
        // 游标置0
        cursor = 0;
    } else {
        redisPanic("Not handled encoding in SCAN.");
    }

    /* Step 3: Filter elements. */
    // 步骤3：过滤元素
    node = listFirst(keys);
    // 遍历keys链表中的元素，根据match选项指定的模式进行过滤操作
    while (node) {
        robj *kobj = listNodeValue(node);
        nextnode = listNextNode(node);
        int filter = 0;

        /* Filter element if it does not match the pattern. */
        if (!filter && use_pattern) {
            // 当前元素如果是整型编码，先转换为字符串后再比较
            if (kobj->encoding == REDIS_ENCODING_INT) {
                char buf[REDIS_LONGSTR_SIZE];
                int len;

                redisAssert(kobj->encoding == REDIS_ENCODING_INT);
                len = ll2string(buf,sizeof(buf),(long)kobj->ptr);
                if (!stringmatchlen(pat, patlen, buf, len, 0)) filter = 1;
            } 
            // 字符串匹配
            else {
                if (!stringmatchlen(pat, patlen, kobj->ptr, sdslen(kobj->ptr), 0))
                    filter = 1;
            }
        }

        /* Filter element if it is an expired key. */
        // 如果该元素与给定模式匹配，继续判断该key是否已经过期
        if (!filter && o == NULL && expireIfNeeded(c->db, kobj)) filter = 1;

        /* Remove the element and its associted value if needed. */
        // 删除不符合条件的节点
        if (filter) {
            decrRefCount(kobj);
            listDelNode(keys, node);
        }

        /* If this is a hash or a sorted set, we have a flat list of
         * key-value elements, so if this element was filtered, remove the
         * value, or skip it if it was not filtered: we only match keys. */
        // 如果目标对象o为hash或zset对象，list中存储着key节点和value节点，下面就是用来处理这种情况的
        if (o && (o->type == REDIS_ZSET || o->type == REDIS_HASH)) {
            // 删除或跳过value节点
            node = nextnode;
            nextnode = listNextNode(node);
            if (filter) {
                kobj = listNodeValue(node);
                decrRefCount(kobj);
                listDelNode(keys, node);
            }
        }
        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    // 步骤4：回复客户端
    addReplyMultiBulkLen(c, 2);
    addReplyBulkLongLong(c,cursor);

    addReplyMultiBulkLen(c, listLength(keys));
    while ((node = listFirst(keys)) != NULL) {
        robj *kobj = listNodeValue(node);
        addReplyBulk(c, kobj);
        decrRefCount(kobj);
        listDelNode(keys, node);
    }

cleanup:
    listSetFreeMethod(keys,decrRefCountVoid);
    listRelease(keys);
}

/* The SCAN command completely relies on scanGenericCommand. */
/*  SCAN命令实现，完全依赖scanGenericCommand函数。*/
void scanCommand(redisClient *c) {
    unsigned long cursor;
    if (parseScanCursorOrReply(c,c->argv[1],&cursor) == REDIS_ERR) return;
    scanGenericCommand(c,NULL,cursor);
}

/*  dbsize命令，返回所有key的数目。*/
void dbsizeCommand(redisClient *c) {
    addReplyLongLong(c,dictSize(c->db->dict));
}

/* LASTSAVE命令，返回最后一次Redis成功将数据保存到磁盘上的时间。*/
void lastsaveCommand(redisClient *c) {
    addReplyLongLong(c,server.lastsave);
}

/* TYPE命令，返回key对应的值的类型。*/
void typeCommand(redisClient *c) {
    robj *o;
    char *type;

    // 先从数据库中取出对象
    o = lookupKeyRead(c->db,c->argv[1]);
    if (o == NULL) {
        type = "none";
    } else {
        switch(o->type) {
        case REDIS_STRING: type = "string"; break;
        case REDIS_LIST: type = "list"; break;
        case REDIS_SET: type = "set"; break;
        case REDIS_ZSET: type = "zset"; break;
        case REDIS_HASH: type = "hash"; break;
        default: type = "unknown"; break;
        }
    }
    addReplyStatus(c,type);
}

/* SHUTDOWN命令实现 */
void shutdownCommand(redisClient *c) {
    int flags = 0;

    // SHUTDOWN命令只能指定nosave或save选项，否则出错
    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    } 
    // 获取选项参数
    else if (c->argc == 2) {
        if (!strcasecmp(c->argv[1]->ptr,"nosave")) {
            flags |= REDIS_SHUTDOWN_NOSAVE;
        } else if (!strcasecmp(c->argv[1]->ptr,"save")) {
            flags |= REDIS_SHUTDOWN_SAVE;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }
    /* When SHUTDOWN is called while the server is loading a dataset in
     * memory we need to make sure no attempt is performed to save
     * the dataset on shutdown (otherwise it could overwrite the current DB
     * with half-read data).
     *
     * Also when in Sentinel mode clear the SAVE flag and force NOSAVE. */
    if (server.loading || server.sentinel_mode)
        flags = (flags & ~REDIS_SHUTDOWN_SAVE) | REDIS_SHUTDOWN_NOSAVE;
    if (prepareForShutdown(flags) == REDIS_OK) exit(0);
    addReplyError(c,"Errors trying to SHUTDOWN. Check logs.");
}

/* RENAME命令的底层函数 */
void renameGenericCommand(redisClient *c, int nx) {
    robj *o;
    long long expire;

    /* To use the same key as src and dst is probably an error */
    // 如果指定key的旧名称和新名称相同，则出错
    if (sdscmp(c->argv[1]->ptr,c->argv[2]->ptr) == 0) {
        addReply(c,shared.sameobjecterr);
        return;
    }

    // 根据key的旧值取出对象
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr)) == NULL)
        return;
    // 增加对象的引用计数值，必不可少的步骤。因为新的key也会引用这个对象，然后删除旧的key
    incrRefCount(o);
    // 取出key的过期时间
    expire = getExpire(c->db,c->argv[1]);
    // 检查指定的key的新名称是否已经存在
    if (lookupKeyWrite(c->db,c->argv[2]) != NULL) {
        // 用于RENAMENX命令，当指定的新名称已经存在则直接返回
        if (nx) {
            decrRefCount(o);
            addReply(c,shared.czero);
            return;
        }
        /* Overwrite: delete the old key before creating the new one
         * with the same name. */
        // 当指定的key的新名称已经存在时， RENAME命令将覆盖旧值
        dbDelete(c->db,c->argv[2]);
    }
    // 将指定的新key和原key所对应的对象组成一个键值对插入数据库db中
    dbAdd(c->db,c->argv[2],o);
    // 如果有需要，设置过期时间
    if (expire != -1) setExpire(c->db,c->argv[2],expire);
    // 删除该key的旧值
    dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);
    notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"rename_from",
        c->argv[1],c->db->id);
    notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"rename_to",
        c->argv[2],c->db->id);
    server.dirty++;
    addReply(c,nx ? shared.cone : shared.ok);
}

/* RENAME命令 */
void renameCommand(redisClient *c) {
    renameGenericCommand(c,0);
}

/* RENAMENX命令 */
void renamenxCommand(redisClient *c) {
    renameGenericCommand(c,1);
}

/* MOVE命令，将当前数据库中的key移动到指定数据库中。*/
void moveCommand(redisClient *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid;
    long long dbid, expire;

    /* Obtain source and target DB pointers */
    // 获取源数据库和其id号
    src = c->db;
    srcid = c->db->id;

    // 获取目标数据库的id号并切换到目标数据库
    if (getLongLongFromObject(c->argv[2],&dbid) == REDIS_ERR ||
        dbid < INT_MIN || dbid > INT_MAX ||
        selectDb(c,dbid) == REDIS_ERR)
    {
        addReply(c,shared.outofrangeerr);
        return;
    }
    // 获取目标数据库
    dst = c->db;
    // 切换为原数据库
    selectDb(c,srcid); /* Back to the source DB */

    /* If the user is moving using as target the same
     * DB as the source DB it is probably an error. */
    // 如果源数据库和目标数据库相同，显然是错误的
    if (src == dst) {
        addReply(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    // 取出要移动的key所对应的类型对象，即目标对象
    o = lookupKeyWrite(c->db,c->argv[1]);
    // 如果目标对象不存在，返回
    if (!o) {
        addReply(c,shared.czero);
        return;
    }
    // 出错过期时间
    expire = getExpire(c->db,c->argv[1]);

    /* Return zero if the key already exists in the target DB */
    // 如果该key已经存在于目标数据库中，直接返回
    if (lookupKeyWrite(dst,c->argv[1]) != NULL) {
        addReply(c,shared.czero);
        return;
    }
    // 将key和对应的类型对象组成一个键值对添加到目标数据库中
    dbAdd(dst,c->argv[1],o);
    // 设置过期时间
    if (expire != -1) setExpire(dst,c->argv[1],expire);
    // 增加目标对象的引用计数值，因为后面需要在源数据库中删除该对象
    incrRefCount(o);

    /* OK! key moved, free the entry in the source DB */
    // 到这里，key已经移动到目标数据库，最后还需要从源数据库中删除
    dbDelete(src,c->argv[1]);
    server.dirty++;
    addReply(c,shared.cone);
}

/*-----------------------------------------------------------------------------
 * Expires API  过期操作
 *----------------------------------------------------------------------------*/

/* 移除指定key的过期时间 */
int removeExpire(redisDb *db, robj *key) {
    /* An expire may only be removed if there is a corresponding entry in the
     * main dict. Otherwise, the key will never be freed. */
    // 检查该key是否存在
    redisAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    // 删除过期时间
    return dictDelete(db->expires,key->ptr) == DICT_OK;
}

/* 为指定key设置过期时间 */
void setExpire(redisDb *db, robj *key, long long when) {
    dictEntry *kde, *de;

    /* Reuse the sds from the main dict in the expire dict */
    // db->dict和db->expires是共用key字符串对象的
    // 取出key
    kde = dictFind(db->dict,key->ptr);
    redisAssertWithInfo(NULL,key,kde != NULL);
    // 取出过期时间
    de = dictReplaceRaw(db->expires,dictGetKey(kde));
    // 重置key的过期时间
    dictSetSignedIntegerVal(de,when);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
/* 返回指定key的过期时间，如果该key没有设置过期时间则返回-1。*/
long long getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    // 如果db->expires为空，或者指定key没有设置过期时间，返回-1
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key->ptr)) == NULL) return -1;

    /* The entry was found in the expire dict, this means it should also
     * be present in the main dict (safety check). */
    // 为了确保安全，再次检查该key是否存在于db->dict中
    redisAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    // 返回过期时间
    return dictGetSignedIntegerVal(de);
}

/* Propagate expires into slaves and the AOF file.
 * When a key expires in the master, a DEL operation for this key is sent
 * to all the slaves and the AOF file if enabled.
 *
 * This way the key expiry is centralized in one place, and since both
 * AOF and the master->slave link guarantee operation ordering, everything
 * will be consistent even if we allow write operations against expiring
 * keys. */
/*  将指定key的过期时间传播到slave节点和AOF文件。
    当一个key在master节点过期时，master节点发送一个该key的DEL命令到所有slave节点和AOF文件。
    这种做法使得对key的过期操作可以集中在一个地方处理。另外，由于AOF和slave可以保证按序执行，所以
    即使有针对过期key的写操作执行，也能保证数据一致性。
*/
void propagateExpire(redisDb *db, robj *key) {
    robj *argv[2];

    // 构造一个DEL命令
    argv[0] = shared.del;
    argv[1] = key;
    incrRefCount(argv[0]);
    incrRefCount(argv[1]);

    // 传播到AOF文件
    if (server.aof_state != REDIS_AOF_OFF)
        feedAppendOnlyFile(server.delCommand,db->id,argv,2);
    // 传播到slave节点
    replicationFeedSlaves(server.slaves,db->id,argv,2);

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
}

/*  检查key是否已经过期，如果过期则删除之。
    如果指定key已经过期且被删除则返回1，否则返回0，表示该key未过期或并没有设置过期时间。*/
int expireIfNeeded(redisDb *db, robj *key) {
    // 获取key的过期时间
    mstime_t when = getExpire(db,key);
    mstime_t now;

    // 如果该key没有过期时间，返回0
    if (when < 0) return 0; /* No expire for this key */

    /* Don't expire anything while loading. It will be done later. */
    // 如果服务器正在加载操作中，则不进行过期检查，返回0
    if (server.loading) return 0;

    /* If we are in the context of a Lua script, we claim that time is
     * blocked to when the Lua script started. This way a key can expire
     * only the first time it is accessed and not in the middle of the
     * script execution, making propagation to slaves / AOF consistent.
     * See issue #1525 on Github for more information. */
    now = server.lua_caller ? server.lua_time_start : mstime();

    /* If we are running in the context of a slave, return ASAP:
     * the slave key expiration is controlled by the master that will
     * send us synthesized DEL operations for expired keys.
     *
     * Still we try to return the right information to the caller,
     * that is, 0 if we think the key should be still valid, 1 if
     * we think the key is expired at this time. */
    // 如果当前程序运行在slave节点，该key的过期操作是由master节点控制的（master节点会发出DEL操作）
    // 在这种情况下该函数先返回一个正确值，即如果key未过期返回0，否则返回1。
    // 真正的删除操作等待master节点发来的DEL命令后再执行
    if (server.masterhost != NULL) return now > when;

    /* Return when this key has not expired */
    // 如果未过期，返回0
    if (now <= when) return 0;

    /* Delete the key */
    // 如果已过期，删除该key
    server.stat_expiredkeys++;
    propagateExpire(db,key);
    notifyKeyspaceEvent(REDIS_NOTIFY_EXPIRED,
        "expired",key,db->id);
    return dbDelete(db,key);
}

/*-----------------------------------------------------------------------------
 * Expires Commands    过期操作命令
 *----------------------------------------------------------------------------*/

/* This is the generic command implementation for EXPIRE, PEXPIRE, EXPIREAT
 * and PEXPIREAT. Because the commad second argument may be relative or absolute
 * the "basetime" argument is used to signal what the base time is (either 0
 * for *AT variants of the command, or the current time for relative expires).
 *
 * unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
 * the argv[2] parameter. The basetime is always specified in milliseconds. */
/*  这是EXPIRE、PEXPIRE、EXPIREAT和PEXPIREAT命令的底层函数。
    这些命令的第二个参数可能是相对值或绝对值。
    参数basetime用来指明基准时间，对于*AT命令，basetime的值为0，对于其它命令，basetime的值为当前时间。
    参数unit用于指定argv[2]的格式：UNIT_SECONDS或者UNIT_MILLISECONDS。而参数basetime总是以
    毫秒为单位的。*/
void expireGenericCommand(redisClient *c, long long basetime, int unit) {
    robj *key = c->argv[1], *param = c->argv[2];
    // 以毫秒为单位的unix时间戳
    long long when; /* unix time in milliseconds when the key will expire. */

    // 获取过期时间
    if (getLongLongFromObjectOrReply(c, param, &when, NULL) != REDIS_OK)
        return;

    // 如果传入的过期时间是以秒为单位，则转换为毫秒为单位
    if (unit == UNIT_SECONDS) when *= 1000;
    // 加上basetime得到过期时间戳
    when += basetime;

    /* No key, return zero. */
    // 取出key，如果该key不存在直接返回
    if (lookupKeyRead(c->db,key) == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
     * should never be executed as a DEL when load the AOF or in the context
     * of a slave instance.
     *
     * Instead we take the other branch of the IF statement setting an expire
     * (possibly in the past) and wait for an explicit DEL from the master. */
    // 当加载AOF或当前服务器为slave节点时，即使EXPIRE的TTL为负数，或则EXPIREAT的时间戳已经过期，
    // 服务器也不会执行DEL命令删除该key
    // 我们在另一个if分支语句中设置该key的过期时间（可能已经过期）然后等待master发来的一个显示DEL命令后再删除
    if (when <= mstime() && !server.loading && !server.masterhost) {
        // 如果when指定的时间已经过期，而且当前为服务器的主节点，并且目前没有载入数据
        robj *aux;

        redisAssertWithInfo(c,key,dbDelete(c->db,key));
        server.dirty++;

        /* Replicate/AOF this as an explicit DEL. */
        // 传播一个显式的DEL命令
        aux = createStringObject("DEL",3);
        rewriteClientCommandVector(c,2,aux,key);
        decrRefCount(aux);
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);
        addReply(c, shared.cone);
        return;
    } else {
        // 设置key的过期时间（when提供的时间可能已经过期）
        setExpire(c->db,key,when);
        addReply(c,shared.cone);
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"expire",key,c->db->id);
        server.dirty++;
        return;
    }
}

void expireCommand(redisClient *c) {
    expireGenericCommand(c,mstime(),UNIT_SECONDS);
}

void expireatCommand(redisClient *c) {
    expireGenericCommand(c,0,UNIT_SECONDS);
}

void pexpireCommand(redisClient *c) {
    expireGenericCommand(c,mstime(),UNIT_MILLISECONDS);
}

void pexpireatCommand(redisClient *c) {
    expireGenericCommand(c,0,UNIT_MILLISECONDS);
}

/* 返回指定key的剩余生存时间，参数output_ms指定返回值的格式（毫秒 or 秒）。 */
void ttlGenericCommand(redisClient *c, int output_ms) {
    long long expire, ttl = -1;

    /* If the key does not exist at all, return -2 */
    // 取出key
    if (lookupKeyRead(c->db,c->argv[1]) == NULL) {
        addReplyLongLong(c,-2);
        return;
    }
    /* The key exists. Return -1 if it has no expire, or the actual
     * TTL value otherwise. */
    // 取出该key的过期时间
    expire = getExpire(c->db,c->argv[1]);
    if (expire != -1) {
        // 计算剩余生存时间
        ttl = expire-mstime();
        if (ttl < 0) ttl = 0;
    }
    if (ttl == -1) {
        addReplyLongLong(c,-1);
    } else {
        // 如果output_ms = 1，返回以毫秒为单位的的生存时间
        // 如果output_ms = 0，返回以秒为单位的生存时间
        addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
    }
}

/* TTL命令，以秒为单位，返回给定 key 的剩余生存时间。*/
void ttlCommand(redisClient *c) {
    ttlGenericCommand(c, 0);
}

/* PTTL命令，以毫秒为单位返回 key 的剩余的过期时间。*/
void pttlCommand(redisClient *c) {
    ttlGenericCommand(c, 1);
}

/* PERSIST命令，移除key的过期时间，key将持久保持.*/
void persistCommand(redisClient *c) {
    dictEntry *de;

    // 取出key对应的键值对
    de = dictFind(c->db->dict,c->argv[1]->ptr);
    if (de == NULL) {
        addReply(c,shared.czero);
    } else {
        // 删除该key的过期时间
        if (removeExpire(c->db,c->argv[1])) {
            addReply(c,shared.cone);
            server.dirty++;
        } else {
            addReply(c,shared.czero);
        }
    }
}

/* -----------------------------------------------------------------------------
 * API to get key arguments from commands
 * ---------------------------------------------------------------------------*/

int *getKeysUsingCommandTable(struct redisCommand *cmd,robj **argv, int argc, int *numkeys) {
    int j, i = 0, last, *keys;
    REDIS_NOTUSED(argv);

    if (cmd->firstkey == 0) {
        *numkeys = 0;
        return NULL;
    }
    last = cmd->lastkey;
    if (last < 0) last = argc+last;
    keys = zmalloc(sizeof(int)*((last - cmd->firstkey)+1));
    for (j = cmd->firstkey; j <= last; j += cmd->keystep) {
        redisAssert(j < argc);
        keys[i++] = j;
    }
    *numkeys = i;
    return keys;
}

int *getKeysFromCommand(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
    if (cmd->getkeys_proc) {
        return cmd->getkeys_proc(cmd,argv,argc,numkeys,flags);
    } else {
        return getKeysUsingCommandTable(cmd,argv,argc,numkeys);
    }
}

void getKeysFreeResult(int *result) {
    zfree(result);
}

int *noPreloadGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
    if (flags & REDIS_GETKEYS_PRELOAD) {
        *numkeys = 0;
        return NULL;
    } else {
        return getKeysUsingCommandTable(cmd,argv,argc,numkeys);
    }
}

int *renameGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
    if (flags & REDIS_GETKEYS_PRELOAD) {
        int *keys = zmalloc(sizeof(int));
        *numkeys = 1;
        keys[0] = 1;
        return keys;
    } else {
        return getKeysUsingCommandTable(cmd,argv,argc,numkeys);
    }
}

int *zunionInterGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
    int i, num, *keys;
    REDIS_NOTUSED(cmd);
    REDIS_NOTUSED(flags);

    num = atoi(argv[2]->ptr);
    /* Sanity check. Don't return any key if the command is going to
     * reply with syntax error. */
    if (num > (argc-3)) {
        *numkeys = 0;
        return NULL;
    }
    keys = zmalloc(sizeof(int)*num);
    for (i = 0; i < num; i++) keys[i] = 3+i;
    *numkeys = num;
    return keys;
}
