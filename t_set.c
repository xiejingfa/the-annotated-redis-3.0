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

/*-----------------------------------------------------------------------------
 * Set Commands  集合set命令
 *----------------------------------------------------------------------------*/

 /* Set数据类型有两种编码方式：REDIS_ENCODING_INSET和REDIS_ENCODING_HT，在下面的注释中我们分别称之为inset编码和dict编码。 
    当使用dict编码的时候，往集合中插入一个元素key相当于往dict中插入<key, NULL>这样一个键值对。*/

void sunionDiffGenericCommand(redisClient *c, robj **setkeys, int setnum, robj *dstkey, int op);

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a regular
 * hash table. */
/*  工厂方法，返回一个可以用来保存参数value的集合对象。
    当value的值可以编码为整数时，返回一个inset编码的集合对象，否则返回一个dict编码的集合对象。*/
robj *setTypeCreate(robj *value) {
    // 判断参数value是否可以编码为整型
    if (isObjectRepresentableAsLongLong(value,NULL) == REDIS_OK)
        return createIntsetObject();
    return createSetObject();
}

/* 往setType对象中添加一个元素，如果操作成功，返回1，否则返回0。*/
int setTypeAdd(robj *subject, robj *value) {
    long long llval;
    // 处理dict编码的情况
    if (subject->encoding == REDIS_ENCODING_HT) {
        // 当使用dict编码的时候，往集合中插入一个元素key相当于往dict中插入<key, NULL>这样一个键值对
        if (dictAdd(subject->ptr,value,NULL) == DICT_OK) {
            incrRefCount(value);
            return 1;
        }
    } 
    // 处理inset编码的情况
    else if (subject->encoding == REDIS_ENCODING_INTSET) {
        // 先判断待插入元素value是否可以编码为整型
        if (isObjectRepresentableAsLongLong(value,&llval) == REDIS_OK) {
            uint8_t success = 0;
            // 往inset中添加一个元素
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */
                // 如果inset中元素个数超过set_max_intset_entries（默认值为512）时，转换为dict编码
                if (intsetLen(subject->ptr) > server.set_max_intset_entries)
                    setTypeConvert(subject,REDIS_ENCODING_HT);
                return 1;
            }
        } 
        // 待插入元素value不可以编码为整型，执行下列分支
        else {
            /* Failed to get integer from object, convert to regular set. */
            // 待插入元素value不可以编码为整型，先将集合set转换为dict编码
            setTypeConvert(subject,REDIS_ENCODING_HT);

            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
            // 转换编码后再执行插入操作
            redisAssertWithInfo(NULL,value,dictAdd(subject->ptr,value,NULL) == DICT_OK);
            incrRefCount(value);
            return 1;
        }
    } else {
        redisPanic("Unknown set encoding");
    }
    return 0;
}

/* 从setType对象中删除一个元素，成功删除返回1，否则返回0。*/
int setTypeRemove(robj *setobj, robj *value) {
    long long llval;
    // 处理dict编码的情况
    if (setobj->encoding == REDIS_ENCODING_HT) {
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
            // 成功删除一个元素后检查是否对dict进行容量调整
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }
    } 
    // 处理inset编码的情况
    else if (setobj->encoding == REDIS_ENCODING_INTSET) {
        // 先判断待插入元素value是否可以编码为整型，将编码的整数放在llval中
        if (isObjectRepresentableAsLongLong(value,&llval) == REDIS_OK) {
            int success;
            // 删除元素
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else {
        redisPanic("Unknown set encoding");
    }
    return 0;
}

/* 判断setType对象中是否包含某个元素。*/
int setTypeIsMember(robj *subject, robj *value) {
    long long llval;
    // 处理dict编码的情况
    if (subject->encoding == REDIS_ENCODING_HT) {
        return dictFind((dict*)subject->ptr,value) != NULL;
    } 
    // 处理inset编码的情况
    else if (subject->encoding == REDIS_ENCODING_INTSET) {
        // 先判断待插入元素value是否可以编码为整型，将编码的整数放在llval中
        if (isObjectRepresentableAsLongLong(value,&llval) == REDIS_OK) {
            return intsetFind((intset*)subject->ptr,llval);
        }
    } else {
        redisPanic("Unknown set encoding");
    }
    return 0;
}

/*  创建并返回一个setType对象的迭代器。setTypeIterator结构体定义在redis.h文件中 */
setTypeIterator *setTypeInitIterator(robj *subject) {
    // 分配空间
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
    // 设置源对象和编码方式
    si->subject = subject;
    si->encoding = subject->encoding;

    // 处理dict编码的情况
    if (si->encoding == REDIS_ENCODING_HT) {
        si->di = dictGetIterator(subject->ptr);
    } 
    // 处理inset编码的情况
    else if (si->encoding == REDIS_ENCODING_INTSET) {
        // 设置索引值
        si->ii = 0;
    } else {
        redisPanic("Unknown set encoding");
    }
    return si;
}

/* 释放setType对象的迭代器 */
void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == REDIS_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * Since set elements can be internally be stored as redis objects or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (eobj) or (llobj) accordingly.
 *
 * When there are no longer elements -1 is returned.
 * Returned objects ref count is not incremented, so this function is
 * copy on write friendly. */
/*  获取当前迭代器所指向的元素，并移动迭代器使其指向下一个元素。

    由于集合set可能是dict编码或者intset编码，所以该函数返回set集合对象的编码方式，方便调用者判断。
    如果set对象的inset编码，则将当前元素保存在参数llele中，如果set对象是dict编码，则将当前元素保存在objele中。

    当集合中如果没有别的元素则返回-1。该函数所返回的对象并没有增加其引用计数值。*/
int setTypeNext(setTypeIterator *si, robj **objele, int64_t *llele) {
    // 处理dict编码的情况
    if (si->encoding == REDIS_ENCODING_HT) {
        dictEntry *de = dictNext(si->di);
        if (de == NULL) return -1;
        // 获取元素值，即key值
        *objele = dictGetKey(de);
    } 
    // 处理inset编码的情况
    else if (si->encoding == REDIS_ENCODING_INTSET) {
        // 注意si->ii++，表示先获取下标为ii的元素，再使迭代器移动到下一个元素
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
    }
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new objects
 * or incrementing the ref count of returned objects. So if you don't
 * retain a pointer to this object you should call decrRefCount() against it.
 *
 * This function is the way to go for write operations where COW is not
 * an issue as the result will be anyway of incrementing the ref count. */
/*  该函数是setTypeNext函数的非copy-on-write版本实现，比较容易使用，而且总是返回一个新的对象或者是已经增加了引用计数值后的对象。
    所以，如果调用者在使用完毕后需要调用decrRefCount减少其引用计数值。 */
robj *setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    robj *objele;
    int encoding;

    // 调用setTypeNext函数取出当前元素并更新迭代器
    encoding = setTypeNext(si,&objele,&intele);
    // 根据setTypeNext的返回值进一步处理，主要是创建对象或更新引用计数值
    switch(encoding) {
        case -1:    return NULL;
        case REDIS_ENCODING_INTSET:
            return createStringObjectFromLongLong(intele);
        case REDIS_ENCODING_HT:
            incrRefCount(objele);
            return objele;
        default:
            redisPanic("Unsupported encoding");
    }
    return NULL; /* just to suppress warnings */
}

/* Return random element from a non empty set.
 * The returned element can be a int64_t value if the set is encoded
 * as an "intset" blob of integers, or a redis object if the set
 * is a regular set.
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointer was populated.
 *
 * When an object is returned (the set was a real set) the ref count
 * of the object is not incremented so this function can be considered
 * copy on write friendly. */
/*  从非空集合中随机选取一个元素并返回。
    如果该集合set为inset编码，则将随机返回的元素保存在参数llele中。
    如果该集合set为dict编码，则将随机返回的元素保存在参数objele中。

    调用者需要提供参数objele和llele供函数存放相应的对象。函数的返回值是setType对象的编码方式，
    这样调用者就可以方便地判断出那个指针保存了元素的值。

    该函数并没有增加所返回对象的引用计数值，所以这个函数可以视为copy-on-write友好的。*/
int setTypeRandomElement(robj *setobj, robj **objele, int64_t *llele) {
    // 处理dict编码的情况
    if (setobj->encoding == REDIS_ENCODING_HT) {
        // 调用dict内部函数实现
        dictEntry *de = dictGetRandomKey(setobj->ptr);
        // 获取key值
        *objele = dictGetKey(de);
    } 
    // 处理intset编码的情况
    else if (setobj->encoding == REDIS_ENCODING_INTSET) {
        // 调用inset内部函数实现
        *llele = intsetRandom(setobj->ptr);
    } else {
        redisPanic("Unknown set encoding");
    }
    // 返回setTy对象的编码方式
    return setobj->encoding;
}

/* 返回集合对象中保存的元素个数 */
unsigned long setTypeSize(robj *subject) {
    if (subject->encoding == REDIS_ENCODING_HT) {
        return dictSize((dict*)subject->ptr);
    } else if (subject->encoding == REDIS_ENCODING_INTSET) {
        return intsetLen((intset*)subject->ptr);
    } else {
        redisPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */
/*  将集合set转换为指定的编码方式，从实现上看，该函数只能讲inset编码的set转换为dict编码。
    新创建的dict会被预先分配与原集合元素个数一样大的空间。*/
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;
    // 检查参数setobj对象的类型和编码方式
    redisAssertWithInfo(NULL,setobj,setobj->type == REDIS_SET &&
                             setobj->encoding == REDIS_ENCODING_INTSET);

    // 只支持目标编码方式为dict编码的转换
    if (enc == REDIS_ENCODING_HT) {
        int64_t intele;
        // 创建一个dict
        dict *d = dictCreate(&setDictType,NULL);
        robj *element;

        /* Presize the dict to avoid rehashing */
        // 预先分配与原集合元素个数一样大的空间，避免在插入元素过程中发生rehashing操作
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        // 遍历原集合中的每个元素并插入到dict中
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si,NULL,&intele) != -1) {
            element = createStringObjectFromLongLong(intele);
            redisAssertWithInfo(NULL,element,dictAdd(d,element,NULL) == DICT_OK);
        }
        // 释放迭代器
        setTypeReleaseIterator(si);

        // 更新原对象的编码方式
        setobj->encoding = REDIS_ENCODING_HT;
        zfree(setobj->ptr);
        setobj->ptr = d;
    } else {
        redisPanic("Unsupported set conversion");
    }
}

/* sadd命令实现，往集合中添加一个或多个元素。 */
void saddCommand(redisClient *c) {
    robj *set;
    int j, added = 0;

    // 为写操作取出集合对象
    set = lookupKeyWrite(c->db,c->argv[1]);
    // 如果数据库db中不存在该集合则创建一个集合对象并添加到数据库中
    if (set == NULL) {
        set = setTypeCreate(c->argv[2]);
        dbAdd(c->db,c->argv[1],set);
    } 
    // 如果找到该对象，则进一步检查其类型
    else {
        if (set->type != REDIS_SET) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }

    // 遍历所有的输入元素，逐一添加到集合set中
    for (j = 2; j < c->argc; j++) {
        // 对输入元素进行编码
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        // 执行真正的添加操作，如果该元素已经存在，setTypeAdd函数将返回0
        if (setTypeAdd(set,c->argv[j])) added++;
    }
    // 如果至少往集合中添加了一个元素，则执行下面的操作
    if (added) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }
    server.dirty += added;
    addReplyLongLong(c,added);
}

/* srem命令，删除元素 */
void sremCommand(redisClient *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    // 取出集合对象并进行类型检查
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    // 遍历所有的输入元素，逐一删除
    for (j = 2; j < c->argc; j++) {
        // setTypeRemove删除成功返回1，这样可以记录真正被删除的元素个数
        if (setTypeRemove(set,c->argv[j])) {
            deleted++;
            // 如果集合为空，删除之
            if (setTypeSize(set) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }

    // 如果至少有一个元素被删除，执行下列操作
    if (deleted) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_SET,"srem",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

/* smove命令，将集合source中的指定元素移动到集合dest中 */
void smoveCommand(redisClient *c) {
    robj *srcset, *dstset, *ele;
    // 找到源集合对象source
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    // 找到目标集合对象dest
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    // 对指定的需要移动的值进行编码
    ele = c->argv[3] = tryObjectEncoding(c->argv[3]);

    /* If the source key does not exist return 0 */
    // 如果源集合不存在，直接返回
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    // 对源集合和目标集合进行类型检查，两者如果有一个不是REDIS_SET编码则返回
    if (checkType(c,srcset,REDIS_SET) ||
        (dstset && checkType(c,dstset,REDIS_SET))) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    // 如果源集合和目标集合是同一个，则无需移动
    if (srcset == dstset) {
        addReply(c,setTypeIsMember(srcset,ele) ? shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    // 尝试从源集合中删除指定元素，如果该元素并不存在与源集合中setTypeRemove函数返回0，当前函数直接返回
    if (!setTypeRemove(srcset,ele)) {
        addReply(c,shared.czero);
        return;
    }
    notifyKeyspaceEvent(REDIS_NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
    // 移除目标元素后如果源集合为空，删除之
    if (setTypeSize(srcset) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }
    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);
    server.dirty++;

    /* Create the destination set when it doesn't exist */
    // 如果目标集合不存在，则创建一个并关联到数据库db中
    if (!dstset) {
        dstset = setTypeCreate(ele);
        dbAdd(c->db,c->argv[2],dstset);
    }

    /* An extra key has changed when ele was successfully added to dstset */
    // 将指定元素添加到目标集合中
    if (setTypeAdd(dstset,ele)) {
        server.dirty++;
        notifyKeyspaceEvent(REDIS_NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }
    addReply(c,shared.cone);
}

/*  sismember命令，判断集合中某个元素是否存在 */
void sismemberCommand(redisClient *c) {
    robj *set;

    // 获取集合对象并进行类型检查
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    // 对给定元素进行编码
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    // 调用setTypeIsMember判断集合中某个元素是否存在
    if (setTypeIsMember(set,c->argv[2]))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

/* scard命令，返回集合中元素个数 */
void scardCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_SET)) return;

    addReplyLongLong(c,setTypeSize(o));
}

/* spop命令，随机移除并返回集合中的一个元素 */
void spopCommand(redisClient *c) {
    robj *set, *ele, *aux;
    int64_t llele;
    int encoding;

    // 获取集合对象并进行类型检查
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    // 调用setTypeRandomElement函数从集合中随机获取一个元素
    encoding = setTypeRandomElement(set,&ele,&llele);
    // 从原集合中删除该元素
    if (encoding == REDIS_ENCODING_INTSET) {
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        incrRefCount(ele);
        setTypeRemove(set,ele);
    }
    notifyKeyspaceEvent(REDIS_NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */
    // 将该命令看做是srem命令进行传播，保证数据一致性
    aux = createStringObject("SREM",4);
    rewriteClientCommandVector(c,3,aux,c->argv[1],ele);
    decrRefCount(ele);
    decrRefCount(aux);

    addReplyBulk(c,ele);
    // 如果集合变为空，删除之
    if (setTypeSize(set) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. */
/*  SRANDMEMBER命令有一个可选的count参数。
    srandmemberWithCountCommand函数用来处理SRANDMEMBER key <count>命令。
    而普通版本的SRANDMEMBER key命令则由srandmemberCommand函数实现。*/

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

void srandmemberWithCountCommand(redisClient *c) {
    long l;
    unsigned long count, size;
    // uniq为1，表示结构中不存在重复元素，uniq为0，表示结构中可能存在重复元素
    int uniq = 1;
    robj *set, *ele;
    int64_t llele;
    int encoding;

    dict *d;

    // 取出count参数
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != REDIS_OK) return;
    if (l >= 0) {
        // count >= 0，表示返回数组中的元素各不相同
        count = (unsigned) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        // 如果count参数小于0，表示数组中的可能存在重复元素。count保存的是返回元素个数，由uniq标识是否有重复元素
        count = -l;
        uniq = 0;
    }

    // 获取set对象并进行类型检查
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk))
        == NULL || checkType(c,set,REDIS_SET)) return;
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    // 如果count为0，直接返回
    if (count == 0) {
        addReply(c,shared.emptymultibulk);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. */
    // case 1：count为负数，当前函数的作用是：返回N个随机元素。在这种情况下可能存在重复元素，不需要使用额外的数据结构
    if (!uniq) {
        addReplyMultiBulkLen(c,count);
        // 调用count次setTypeRandomElement函数从集合中随机获取一个元素，然后加入到回复消息中
        while(count--) {
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == REDIS_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulk(c,ele);
            }
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    // case 2：如果count指定的元素个数比集合中的元素个数还要多，则直接返回整个集合
    if (count >= size) {
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,REDIS_OP_UNION);
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    // 对于case 3 和case 4，我们需要一个额外的dict结构
    d = dictCreate(&setDictType,NULL);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requsted elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 3 is highly inefficient. */
    // case 3： 如果count和SRANDMEMBER_SUB_STRATEGY_MUL的乘积大于集合中元素个数。
    // 在这种情况下，我们先创建一个原集合的副本（即前面创建的d变量），然后从该副本中随机删除元素直到集合中的元素个数等于count
    // 为什么要这样做呢？原因是当count的数值很接近集合中元素个数时，采用随机获取count个元素的方法是很低效的。
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        // 遍历集合中的每一个元素并将其添加到字典d中
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            if (encoding == REDIS_ENCODING_INTSET) {
                retval = dictAdd(d,createStringObjectFromLongLong(llele),NULL);
            } else if (ele->encoding == REDIS_ENCODING_RAW) {
                retval = dictAdd(d,dupStringObject(ele),NULL);
            } else if (ele->encoding == REDIS_ENCODING_INT) {
                retval = dictAdd(d,
                    createStringObjectFromLongLong((long)ele->ptr),NULL);
            }
            redisAssert(retval == DICT_OK);
        }
        // 此时字典dict中存放着集合中的所有元素
        setTypeReleaseIterator(si);
        redisAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        // 从该字典dict中随机删除元素直到其元素个数等于count
        while(size > count) {
            dictEntry *de;
            // 随机从字典中获取一个元素
            de = dictGetRandomKey(d);
            // 删除该元素
            dictDelete(d,dictGetKey(de));
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    // case 4： 集合中元素数量远比count的数值大，在这种情况下，我们随机从集合中获取一个元素并添加到字典d中直到
    //  字典d中的元素个数等于count为止
    else {
        // added表示往字典d中添加的元素个数，初始时字典d中元素个数为0
        unsigned long added = 0;

        while(added < count) {
            // 从集合中随机获取一个元素
            encoding = setTypeRandomElement(set,&ele,&llele);
            // 根据集合的编码方式创建相应的字符串对象
            if (encoding == REDIS_ENCODING_INTSET) {
                ele = createStringObjectFromLongLong(llele);
            } else if (ele->encoding == REDIS_ENCODING_RAW) {
                ele = dupStringObject(ele);
            } else if (ele->encoding == REDIS_ENCODING_INT) {
                ele = createStringObjectFromLongLong((long)ele->ptr);
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            // 尝试将当前元素添加到字典d中。注意：如果该元素已经存在于字典d中，dictAdd返回0
            if (dictAdd(d,ele,NULL) == DICT_OK)
                added++;
            else
                decrRefCount(ele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    // 这是case 3和case 4的后续步骤，将结果返回给客户端
    {
        dictIterator *di;
        dictEntry *de;

        addReplyMultiBulkLen(c,count);
        // 遍历字典d，将每个元素都添加到回复消息中
        di = dictGetIterator(d);
        while((de = dictNext(di)) != NULL)
            addReplyBulk(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

void srandmemberCommand(redisClient *c) {
    robj *set, *ele;
    int64_t llele;
    int encoding;

    // 带有count选项参数，由srandmemberWithCountCommand函数来处理该命令
    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
    } 
    // 参数错误
    else if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    // 取出集合对象并进行类型检查
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    // 随机获取集合中的一个元素
    encoding = setTypeRandomElement(set,&ele,&llele);
    if (encoding == REDIS_ENCODING_INTSET) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulk(c,ele);
    }
}

/* 计算集合s1的元素个数与集合s2的元素个数之差 */
int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    return setTypeSize(*(robj**)s1)-setTypeSize(*(robj**)s2);
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
/*  计算集合s2的元素个数与集合s1的元素个数之差，可以处理参数为NULL的情况。 */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;

    return  (o2 ? setTypeSize(o2) : 0) - (o1 ? setTypeSize(o1) : 0);
}

/*  sinter命令，返回所有给定集合的交集中的所有成员。
    参数setkey为给定的所有集合所分别关联的key，参数setnum指明输入集合的个数。
    参数dstkey主要用于sinterstore，用来存放给定所有集合的交集。 */
void sinterGenericCommand(redisClient *c, robj **setkeys, unsigned long setnum, robj *dstkey) {
    // 集合数组，即用来存放所有给定的集合对象
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *eleobj, *dstset = NULL;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding;

    for (j = 0; j < setnum; j++) {
        // 取出集合对象。对于sinterstore命令，第一个输入的是目标集合destkey
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        // 集合对象不存在
        if (!setobj) {
            zfree(sets);
            if (dstkey) {
                if (dbDelete(c->db,dstkey)) {
                    signalModifiedKey(c->db,dstkey);
                    server.dirty++;
                }
                addReply(c,shared.czero);
            } else {
                addReply(c,shared.emptymultibulk);
            }
            return;
        }
        // 检查当前对象的类型
        if (checkType(c,setobj,REDIS_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }
    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    // 对集合按元素个数从少到多排序，这样可以提高算法效率
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    // 这里我们首先要做的是输出结果集合中的元素个数，但目前我们并不知道这个结果交集中会有多少元素。
    // 所以这里我们使用了一个小技巧：先往结果数组添加一个空对象并保存指向该对象的指针，最后再填充正确的长度信息
    if (!dstkey) {
        replylen = addDeferredMultiBulkLength(c);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        // 如果目标集合为空，创建一个
        dstset = createIntsetObject();
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    // 遍历包含元素个数最少的集合中的每一个元素，然后测试该元素是不是在其它集合中。只要有
    // 一个集合不包含该元素，那么就简单抛弃它，因为这个元素肯定不属于交集

    // 经过前面排序，sets[0]是元素个数最少的集合，为了方便描述，我们这里称之为“基准集合”，因为它的每一个元素都要跟
    // 其它集合比较
    si = setTypeInitIterator(sets[0]);
    while((encoding = setTypeNext(si,&eleobj,&intobj)) != -1) {
        // 分别与其它集合比较，看看其它集合是否都包含了该元素
        for (j = 1; j < setnum; j++) {
            // 如果是同一个集合，则直接跳过
            if (sets[j] == sets[0]) continue;
            // 处理“基准集合”为inset编码的情况
            if (encoding == REDIS_ENCODING_INTSET) {
                // 与“基准集合”比较的集合可能是inset编码或者dict编码，这里需要分别比较
                /* intset with intset is simple... and fast */
                if (sets[j]->encoding == REDIS_ENCODING_INTSET &&
                    !intsetFind((intset*)sets[j]->ptr,intobj))
                {
                    break;
                /* in order to compare an integer with an object we
                 * have to use the generic function, creating an object
                 * for this */
                } else if (sets[j]->encoding == REDIS_ENCODING_HT) {
                    eleobj = createStringObjectFromLongLong(intobj);
                    if (!setTypeIsMember(sets[j],eleobj)) {
                        decrRefCount(eleobj);
                        break;
                    }
                    decrRefCount(eleobj);
                }
            } 
            // 处理“基准集合”为dict编码的情况
            else if (encoding == REDIS_ENCODING_HT) {
                // 与“基准集合”比较的集合可能是inset编码或者dict编码，这里需要分别比较
                /* Optimization... if the source object is integer
                 * encoded AND the target set is an intset, we can get
                 * a much faster path. */
                if (eleobj->encoding == REDIS_ENCODING_INT &&
                    sets[j]->encoding == REDIS_ENCODING_INTSET &&
                    !intsetFind((intset*)sets[j]->ptr,(long)eleobj->ptr))
                {
                    break;
                /* else... object to object check is easy as we use the
                 * type agnostic API here. */
                } else if (!setTypeIsMember(sets[j],eleobj)) {
                    break;
                }
            }
        }

        /* Only take action when all sets contain the member */
        // 只有当所有的集合都包含当前元素时，该元素才是交集中的成员，这是执行下面代码
        if (j == setnum) {
            // 目标集合dstkey为空，执行sinter命令
            if (!dstkey) {
                if (encoding == REDIS_ENCODING_HT)
                    addReplyBulk(c,eleobj);
                else
                    addReplyBulkLongLong(c,intobj);
                cardinality++;
            } 
            // 目标集合dstkey不为空，执行sinterstore命令。
            else {
                if (encoding == REDIS_ENCODING_INTSET) {
                    eleobj = createStringObjectFromLongLong(intobj);
                    // 将当前元素添加到目标集合中
                    setTypeAdd(dstset,eleobj);
                    decrRefCount(eleobj);
                } else {
                    // 将当前元素添加到目标集合中
                    setTypeAdd(dstset,eleobj);
                }
            }
        }
    }
    setTypeReleaseIterator(si);

    // 目标集合dstkey不为空，执行sinterstore命令，需要将目标集合添加到数据库中
    if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        // 如果数据库db中已经存在该key，删除之
        int deleted = dbDelete(c->db,dstkey);
        // 如果目标集合不为空，添加到数据库db中
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(REDIS_NOTIFY_SET,"sinterstore",
                dstkey,c->db->id);
        } 
        // 目标集合为空
        else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
            if (deleted)
                notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",
                    dstkey,c->db->id);
        }
        signalModifiedKey(c->db,dstkey);
        server.dirty++;
    } else {
        // 更新结果集合中的元素个数信息
        setDeferredMultiBulkLength(c,replylen,cardinality);
    }
    zfree(sets);
}

/* sinter命令 */
void sinterCommand(redisClient *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

/* sinterstore命令 */
void sinterstoreCommand(redisClient *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}

// 操作类型
#define REDIS_OP_UNION 0    // 并集
#define REDIS_OP_DIFF 1     // 差集
#define REDIS_OP_INTER 2    // 交集

/*  参数setkeys是给定的所有集合所关联的key数组。
    参数setnum指明了输入集合的数量。
    参数dstkey主要用于XXXstore命令，指明目标集合所关联的key。
    参数op为操作类型。*/
void sunionDiffGenericCommand(redisClient *c, robj **setkeys, int setnum, robj *dstkey, int op) {
    // 为集合数组分配空间
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *ele, *dstset = NULL;
    int j, cardinality = 0;
    int diff_algo = 1;

    // 从数据库db中取出集合对象，并赋值给集合数组sets中相应的元素
    for (j = 0; j < setnum; j++) {
        // 如果是XXXstore命令，dstkey不为NULL，第一个输入key是目标集合
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        // 对于不存在的集合，看做为NULL
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }
        // 对当前集合进行类型检查
        if (checkType(c,setobj,REDIS_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    //  选择使用哪个DIFF算法进行计算
    //  算法1的时间复杂度为O(n * m)，其中n是第一个集合的元素个数，m为集合的数量
    //  算法2的时间复杂度为O(n)，其中n是所有集合中元素个数之和
    //  我们通过考察数据来决定使用哪个算法
    if (op == REDIS_OP_DIFF && sets[0]) {
        long long algo_one_work = 0, algo_two_work = 0;

        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;
            // 计算n * m的值，也就是算法1的时间复杂度
            algo_one_work += setTypeSize(sets[0]);
            // 计算所有集合的元素个数之和，也就是算法2的时间复杂度
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        // 通常来看，算法1的时间复杂度比算法2好，这里algo_one_work除以2也就是优先选取算法1来执行
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            // 如果使用算法1，对sets中除sets[0]以外的集合按包含元素个数从大到小排序
            qsort(sets+1,setnum-1,sizeof(robj*),
                qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    // 我们需要一个临时的集合来保存union操作的结果。如果当前执行的是SUNIONSTORE命令，那么这个临时集合就被
    // 当做结果集合关联到目标key上
    dstset = createIntsetObject();

    // 执行union操作，求并集
    if (op == REDIS_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        // union操作就是讲所有集合中的所有元素注意添加到临时集合dstset中（集合对象自己会负责取出重复数据）
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                // 只有待插入元素不存在时，setTypeAdd才会真正添加元素并返回1
                if (setTypeAdd(dstset,ele)) cardinality++;
                decrRefCount(ele);
            }
            setTypeReleaseIterator(si);
        }
    } 
    // 使用算法1执行求差集操作
    else if (op == REDIS_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        //  算法1的执行过程是：
        //  我们遍历第一个集合（也就是set[0]）中的每一个元素，只有当该元素不存在于其它集合时才将该元素添加到目标集合
        //  这种方法最多执行N * M步操作，其中N是第一个集合的元素个数，M是集合的个数
        si = setTypeInitIterator(sets[0]);
        while((ele = setTypeNextObject(si)) != NULL) {
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) continue; /* no key is an empty set. */
                if (sets[j] == sets[0]) break; /* same set! */
                // 检查该元素是否存在于其它集合
                if (setTypeIsMember(sets[j],ele)) break;
            }
            // 只有当该元素不存在于其它集合时才将该元素添加到目标集合
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                setTypeAdd(dstset,ele);
                cardinality++;
            }
            decrRefCount(ele);
        }
        setTypeReleaseIterator(si);
    } 
    // 使用算法2执行求差集运行
    else if (op == REDIS_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        // 算法的执行过程是：
        // 先将第一个集合（也就是set[0]）中的所有元素添加到目标集合，然后遍历剩余的其它集合，将这些集合中的所有元素从
        // 目标集合中删去。也就是目标集合中的某个元素如果出现在其它集合中就删除这个元素。
        // 算法的时间复杂度为O(n)，其中n为所有集合元素个数之和。
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                // 如果j == 0，执行插入操作，将该集合中的所有元素插入到目标集合
                if (j == 0) {
                    if (setTypeAdd(dstset,ele)) cardinality++;
                } 
                // 如果j != 0，执行删除操作，将该集合中的所有元素从目标集合中删除
                else {
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
                decrRefCount(ele);
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            // 如果目标集合变为空，则无需进行后面的删除操作，直接跳出for循环
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    // dstkey为NULL，执行的是非XXXstore操作，打印出结果集
    if (!dstkey) {
        addReplyMultiBulkLen(c,cardinality);
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL) {
            addReplyBulk(c,ele);
            decrRefCount(ele);
        }
        setTypeReleaseIterator(si);
        decrRefCount(dstset);
    } 
    // dstkey不为NULL，执行的是XXXstore操作，将结果集添加到数据库db中
    else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        // 如果数据库db中存在一个为dstkey的对象，先删除之
        int deleted = dbDelete(c->db,dstkey);
        // 结果集不为空，添加到数据库db中
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(REDIS_NOTIFY_SET,
                op == REDIS_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
        } 
        // 结果集为空
        else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
            if (deleted)
                notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",
                    dstkey,c->db->id);
        }
        signalModifiedKey(c->db,dstkey);
        server.dirty++;
    }
    zfree(sets);
}

/* sunion命令 */
void sunionCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,REDIS_OP_UNION);
}

/* sunionstore命令 */
void sunionstoreCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],REDIS_OP_UNION);
}

/* sdiff命令 */
void sdiffCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,REDIS_OP_DIFF);
}

/* sdiffstore命令 */
void sdiffstoreCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],REDIS_OP_DIFF);
}

/* sscanf命令 */
void sscanCommand(redisClient *c) {
    robj *set;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,set,REDIS_SET)) return;
    scanGenericCommand(c,set,cursor);
}
