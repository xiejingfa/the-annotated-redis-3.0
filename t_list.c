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
 * List API
 *----------------------------------------------------------------------------*/

 /*********************************************************************************
	List类型底层有ziplist和linked list两种结构，我们将其统称为listType。
	其中，linked list是指Redis的底层内置结构list
 ************************************************************************************/

/* Check the argument length to see if it requires us to convert the ziplist
 * to a real list. Only check raw-encoded objects because integer encoded
 * objects are never too long. */
/* 参数value是待插入subject中的一个字符串对象，检查参数value的长度，如果其长度超过list_max_ziplist_value
	则将subject的底层结构从ziplist转换为双向链表list。只需要对REDIS_ENCODING_RAW编码的对象进行检查即可，
	因为一个整数编码的值不可能太长。 */
void listTypeTryConversion(robj *subject, robj *value) {
    if (subject->encoding != REDIS_ENCODING_ZIPLIST) return;

    if (value->encoding == REDIS_ENCODING_RAW &&
    	// list_max_ziplist_value的默认值为64
        sdslen(value->ptr) > server.list_max_ziplist_value)
        	// 将list类型的底层实现从ziplist转换为双向链表
            listTypeConvert(subject,REDIS_ENCODING_LINKEDLIST);
}

/* The function pushes an element to the specified list object 'subject',
 * at head or tail position as specified by 'where'.
 *
 * There is no need for the caller to increment the refcount of 'value' as
 * the function takes care of it if needed. */
/*	该函数用于往list类型中添加一个元素，可以使用参数where指定添加到表头还是表尾。
	该函数会自动处理value的引用计数值，调用者无需关心。 */
void listTypePush(robj *subject, robj *value, int where) {
    /* Check if we need to convert the ziplist */
    // 检查是否需要转换编码（REDIS_ENCODING_ZIPLIST => REDIS_ENCODING_LINKEDLIST）
    listTypeTryConversion(subject,value);
    if (subject->encoding == REDIS_ENCODING_ZIPLIST &&
    	// list_max_ziplist_entries的默认值为512，如果ziplist中存放的节点数超过该值也需要转换编码
        ziplistLen(subject->ptr) >= server.list_max_ziplist_entries)
            listTypeConvert(subject,REDIS_ENCODING_LINKEDLIST);

    // 分别处理以ziplist和linked list编码的情况
    if (subject->encoding == REDIS_ENCODING_ZIPLIST) {
    	/* 处理底层结构为ziplist的情况 */

    	// 确定新元素是插入到头部还是尾部
        int pos = (where == REDIS_HEAD) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
        value = getDecodedObject(value);
        // 直接调用ziplist的内部函数实现插入操作
        subject->ptr = ziplistPush(subject->ptr,value->ptr,sdslen(value->ptr),pos);
        decrRefCount(value);
    } else if (subject->encoding == REDIS_ENCODING_LINKEDLIST) {
    	/* 下面处理底层结构为linked list的情况 */
        if (where == REDIS_HEAD) {
            listAddNodeHead(subject->ptr,value);
        } else {
            listAddNodeTail(subject->ptr,value);
        }
        incrRefCount(value);
    } else {
        redisPanic("Unknown list encoding");
    }
}

/* 从listType中pop出一个元素 */
robj *listTypePop(robj *subject, int where) {
    robj *value = NULL;
    if (subject->encoding == REDIS_ENCODING_ZIPLIST) {
    	// 下面处理底层为ziplist的情况
        unsigned char *p;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        // 确定待删除元素的位置
        int pos = (where == REDIS_HEAD) ? 0 : -1;
        // 根据索引值获取节点
        p = ziplistIndex(subject->ptr,pos);
        // 获取该节点的值，有可能是字符串或整型
        if (ziplistGet(p,&vstr,&vlen,&vlong)) {
            if (vstr) {
                value = createStringObject((char*)vstr,vlen);
            } else {
                value = createStringObjectFromLongLong(vlong);
            }
            /* We only need to delete an element when it exists */
            // 删除节点
            subject->ptr = ziplistDelete(subject->ptr,&p);
        }
    } else if (subject->encoding == REDIS_ENCODING_LINKEDLIST) {
    	// 下面处理底层为linked list的情况

    	// 获取双向链表list对象
        list *list = subject->ptr;
        listNode *ln;
        // 获取头部或尾部节点
        if (where == REDIS_HEAD) {
            ln = listFirst(list);
        } else {
            ln = listLast(list);
        }
        if (ln != NULL) {
        	// 删除节点
            value = listNodeValue(ln);
            incrRefCount(value);
            listDelNode(list,ln);
        }
    } else {
        redisPanic("Unknown list encoding");
    }
    // 返回被删除节点
    return value;
}

/* 返回listType中存储的节点数量 */
unsigned long listTypeLength(robj *subject) {
	// 处理底层为ziplist的情况
    if (subject->encoding == REDIS_ENCODING_ZIPLIST) {
        return ziplistLen(subject->ptr);
    } 
    // 处理底层为linked list的情况
    else if (subject->encoding == REDIS_ENCODING_LINKEDLIST) {
        return listLength((list*)subject->ptr);
    } else {
        redisPanic("Unknown list encoding");
    }
}

/* Initialize an iterator at the specified index. */
/*	listTypeIterator为列表迭代器，定义在redis.h文件中。
	listType是双向列表，可以向前或向后迭代。
	该函数创建并返回一个列表迭代器，参数index指定开始迭代的节点索引，参数direction指定迭代方向。*/
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction) {
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));
    // 设置迭代器各属性
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    if (li->encoding == REDIS_ENCODING_ZIPLIST) {
    	// 获取当前迭代器指向节点
        li->zi = ziplistIndex(subject->ptr,index);
    } else if (li->encoding == REDIS_ENCODING_LINKEDLIST) {
    	// 获取当前迭代器指向节点
        li->ln = listIndex(subject->ptr,index);
    } else {
        redisPanic("Unknown list encoding");
    }
    return li;
}

/* Clean up the iterator. */
/* 释放listType的迭代器 */
void listTypeReleaseIterator(listTypeIterator *li) {
    zfree(li);
}

/* Stores pointer to current the entry in the provided entry structure
 * and advances the position of the iterator. Returns 1 when the current
 * entry is in fact an entry, 0 otherwise. */
/* 将迭代器中指向当前节点的指针存储在参数entry中，并将迭代器的指针移动到下一个节点。
	如果当前节点是一个合法的节点则返回1（表示listType中还有元素可迭代），否则返回0。
	listTypeEntry结构定义在redis.h中*/
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* Protect from converting when iterating */
    redisAssert(li->subject->encoding == li->encoding);

    entry->li = li;
    // 处理底层结构为ziplist的情况
    if (li->encoding == REDIS_ENCODING_ZIPLIST) {
    	// 将当前迭代器所指节点的指针存储在entry中
        entry->zi = li->zi;
        // 迭代到下一个节点，实质调用ziplist的内部函数
        if (entry->zi != NULL) {
            if (li->direction == REDIS_TAIL)
                li->zi = ziplistNext(li->subject->ptr,li->zi);
            else
                li->zi = ziplistPrev(li->subject->ptr,li->zi);
            return 1;
        }
    } 
    // 处理底层结构为linked list的情况
    else if (li->encoding == REDIS_ENCODING_LINKEDLIST) {
    	// 将当前迭代器所指节点的指针存储在entry中
        entry->ln = li->ln;
        // 迭代到下一个节点，简单的指针运算
        if (entry->ln != NULL) {
            if (li->direction == REDIS_TAIL)
                li->ln = li->ln->next;
            else
                li->ln = li->ln->prev;
            return 1;
        }
    } else {
        redisPanic("Unknown list encoding");
    }
    return 0;
}

/* Return entry or NULL at the current position of the iterator. */
/* 返回当前listTypeEntry结构所保存的节点。ListTypeEntry结构体是对ziplist节点和linked list节点的封装。*/
robj *listTypeGet(listTypeEntry *entry) {
	// 获得相应的迭代器对象
    listTypeIterator *li = entry->li;
    robj *value = NULL;
    // 处理底层结构为ziplist的情况
    if (li->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        redisAssert(entry->zi != NULL);
        // ziplist节点中存放的可能是整数或字符串
        if (ziplistGet(entry->zi,&vstr,&vlen,&vlong)) {
            if (vstr) {
                value = createStringObject((char*)vstr,vlen);
            } else {
                value = createStringObjectFromLongLong(vlong);
            }
        }
    } 
    // 处理底层结构为linked list的情况
    else if (li->encoding == REDIS_ENCODING_LINKEDLIST) {
        redisAssert(entry->ln != NULL);
        value = listNodeValue(entry->ln);
        incrRefCount(value);
    } else {
        redisPanic("Unknown list encoding");
    }
    return value;
}

/*	往listType对象指定节点前面或后面插入一个新节点，由参数where决定。*/
void listTypeInsert(listTypeEntry *entry, robj *value, int where) {
	// 获取listType对象
    robj *subject = entry->li->subject;
    // 处理底层结构为ziplist的情况
    if (entry->li->encoding == REDIS_ENCODING_ZIPLIST) {
    	// 返回待插入对象的未编码值
        value = getDecodedObject(value);
        // 新节点插入到指定节点之后
        if (where == REDIS_TAIL) {
            unsigned char *next = ziplistNext(subject->ptr,entry->zi);

            /* When we insert after the current element, but the current element
             * is the tail of the list, we need to do a push. */
            if (next == NULL) {
            	// 当前节点是最后一个节点，则新节点直接加到ziplist尾部
                subject->ptr = ziplistPush(subject->ptr,value->ptr,sdslen(value->ptr),REDIS_TAIL);
            } else {
            	// 新节点插入到当前节点之后
                subject->ptr = ziplistInsert(subject->ptr,next,value->ptr,sdslen(value->ptr));
            }
        } else {
        	// 新节点插入到当前节点前面
            subject->ptr = ziplistInsert(subject->ptr,entry->zi,value->ptr,sdslen(value->ptr));
        }
        decrRefCount(value);
    } 
    // 处理底层结构为linked list的情况
    else if (entry->li->encoding == REDIS_ENCODING_LINKEDLIST) {
        if (where == REDIS_TAIL) {
            listInsertNode(subject->ptr,entry->ln,value,AL_START_TAIL);
        } else {
            listInsertNode(subject->ptr,entry->ln,value,AL_START_HEAD);
        }
        incrRefCount(value);
    } else {
        redisPanic("Unknown list encoding");
    }
}

/* Compare the given object with the entry at the current position. */
/* 将当前节点和给定对象o比较，如果相等返回1，否则返回0。*/
int listTypeEqual(listTypeEntry *entry, robj *o) {
	// 获取当前迭代器
    listTypeIterator *li = entry->li;
    if (li->encoding == REDIS_ENCODING_ZIPLIST) {
        redisAssertWithInfo(NULL,o,o->encoding == REDIS_ENCODING_RAW);
        return ziplistCompare(entry->zi,o->ptr,sdslen(o->ptr));
    } else if (li->encoding == REDIS_ENCODING_LINKEDLIST) {
    	// equalStringObjects定义在redis.h中，用于比较两个redisObject对象是否相等
        return equalStringObjects(o,listNodeValue(entry->ln));
    } else {
        redisPanic("Unknown list encoding");
    }
}

/* Delete the element pointed to. */
/* 删除entry指向的当前节点 */
void listTypeDelete(listTypeEntry *entry) {
	// 获取当前迭代器
    listTypeIterator *li = entry->li;
    // 处理底层结构为ziplist的情况
    if (li->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *p = entry->zi;
        // 调用ziplistDelete后p指向被删除节点的下一个节点
        li->subject->ptr = ziplistDelete(li->subject->ptr,&p);

        /* Update position of the iterator depending on the direction */
        // 删除节点后需要更新迭代器指向的节点
        if (li->direction == REDIS_TAIL)
            li->zi = p;
        else
            li->zi = ziplistPrev(li->subject->ptr,p);
    } 
    // 处理底层结构是linked list的情况
    else if (entry->li->encoding == REDIS_ENCODING_LINKEDLIST) {
    	// 用于记录被删除节点的下一个节点
        listNode *next;
        if (li->direction == REDIS_TAIL)
            next = entry->ln->next;
        else
            next = entry->ln->prev;
        listDelNode(li->subject->ptr,entry->ln);
        // 更新迭代器指针
        li->ln = next;
    } else {
        redisPanic("Unknown list encoding");
    }
}

/* 将listType从REDIS_ENCODING_ZIPLIST编码转换为REDIS_ENCODING_LINKEDLIST编码*/
void listTypeConvert(robj *subject, int enc) {
    listTypeIterator *li;
    listTypeEntry entry;
    redisAssertWithInfo(NULL,subject,subject->type == REDIS_LIST);

    if (enc == REDIS_ENCODING_LINKEDLIST) {
        list *l = listCreate();
        listSetFreeMethod(l,decrRefCountVoid);

        /* listTypeGet returns a robj with incremented refcount */
        // 遍历ziplist，并将每一个节点添加到linked list中，然后释放迭代器
        li = listTypeInitIterator(subject,0,REDIS_TAIL);
        while (listTypeNext(li,&entry)) listAddNodeTail(l,listTypeGet(&entry));
        listTypeReleaseIterator(li);

        // 更新编码方式
        subject->encoding = REDIS_ENCODING_LINKEDLIST;
        // 释放原来ziplist的空间
        zfree(subject->ptr);
        /* 更新对象指针 */
        subject->ptr = l;
    } else {
        redisPanic("Unsupported list conversion");
    }
}

/*-----------------------------------------------------------------------------
 * List Commands List类型相关命令的实现
 *----------------------------------------------------------------------------*/

/* 下面是List类型命令的实现，操作成功后会涉及诸如发送事件通知等的操作，这里我们暂时忽略，以后分析到相应源码的时候再解释 */

/* push命令的底层函数，参数where指定从头部插入和是从尾部插入 */
void pushGenericCommand(redisClient *c, int where) {
    int j, waiting = 0, pushed = 0;
    // 从db中取出listType
    robj *lobj = lookupKeyWrite(c->db,c->argv[1]);

    // 如果指定key的对象并不是listType，报错
    if (lobj && lobj->type != REDIS_LIST) {
        addReply(c,shared.wrongtypeerr);
        return;
    }

    // 遍历每个输入值并添加到listType中
    for (j = 2; j < c->argc; j++) {
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        // 如果listType不存在，则创建一个。默认是ziplist编码的
        if (!lobj) {
            lobj = createZiplistObject();
            dbAdd(c->db,c->argv[1],lobj);
        }
        listTypePush(lobj,c->argv[j],where);
        // 记录共添加了多少个元素
        pushed++;
    }
    addReplyLongLong(c, waiting + (lobj ? listTypeLength(lobj) : 0));

    // 如果至少有一个元素则发送响应信息,暂时我们忽略之
    if (pushed) {
        char *event = (where == REDIS_HEAD) ? "lpush" : "rpush";

        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_LIST,event,c->argv[1],c->db->id);
    }
    server.dirty += pushed;
}

/** lpush命令的实现 */
void lpushCommand(redisClient *c) {
    pushGenericCommand(c,REDIS_HEAD);
}

/* rpush命令实现 */
void rpushCommand(redisClient *c) {
    pushGenericCommand(c,REDIS_TAIL);
}

/* 处理listType插入操作的底层函数 */
void pushxGenericCommand(redisClient *c, robj *refval, robj *val, int where) {
    robj *subject;
    listTypeIterator *iter;
    listTypeEntry entry;
    int inserted = 0;

    // 取出listType对象，如果不存在直接退出，不执行创建操作
    if ((subject = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,subject,REDIS_LIST)) return;

    /* 如果refval不为空，执行linsert命令，在refval前面或后面插入一个节点 */
    if (refval != NULL) {
        /* We're not sure if this value can be inserted yet, but we cannot
         * convert the list inside the iterator. We don't want to loop over
         * the list twice (once to see if the value can be inserted and once
         * to do the actual insert), so we assume this value can be inserted
         * and convert the ziplist to a regular list if necessary. */
		// 检查保存val值会不会引起listType编码转换
        listTypeTryConversion(subject,val);

        /* Seek refval from head to tail */
        // 遍历一遍listType以查找refval对象并在其前面或后面插入一个新节点
        iter = listTypeInitIterator(subject,0,REDIS_TAIL);
        while (listTypeNext(iter,&entry)) {
            if (listTypeEqual(&entry,refval)) {
                listTypeInsert(&entry,val,where);
                inserted = 1;
                break;
            }
        }
        // 释放迭代器
        listTypeReleaseIterator(iter);

        if (inserted) {
            /* Check if the length exceeds the ziplist length threshold. */
            // 插入一个新节点后引起listType节点数量增加，如果节点数量超过list_max_ziplist_entries
            // 则要转换编码。list_max_ziplist_entries默认值为512。
            if (subject->encoding == REDIS_ENCODING_ZIPLIST &&
                ziplistLen(subject->ptr) > server.list_max_ziplist_entries)
                    listTypeConvert(subject,REDIS_ENCODING_LINKEDLIST);
            signalModifiedKey(c->db,c->argv[1]);
            notifyKeyspaceEvent(REDIS_NOTIFY_LIST,"linsert",
                                c->argv[1],c->db->id);
            server.dirty++;
        } else {
            /* Notify client of a failed insert */
            // 如果refval不存在，则插入失败，发送相关信息
            addReply(c,shared.cnegone);
            return;
        }
    } 
    // refval为空，执行lpush命令或rpush命令
    else {
        char *event = (where == REDIS_HEAD) ? "lpush" : "rpush";

        listTypePush(subject,val,where);
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_LIST,event,c->argv[1],c->db->id);
        server.dirty++;
    }

    addReplyLongLong(c,listTypeLength(subject));
}

/* lpushnx命令实现 */
void lpushxCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    pushxGenericCommand(c,NULL,c->argv[2],REDIS_HEAD);
}

/* rpushnx命令实现 */
void rpushxCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    pushxGenericCommand(c,NULL,c->argv[2],REDIS_TAIL);
}

/* linsert命令实现，命令格式为linsert key after|before search value，对应其源码很好理解 */
void linsertCommand(redisClient *c) {
    c->argv[4] = tryObjectEncoding(c->argv[4]);
    if (strcasecmp(c->argv[2]->ptr,"after") == 0) {
        pushxGenericCommand(c,c->argv[3],c->argv[4],REDIS_TAIL);
    } else if (strcasecmp(c->argv[2]->ptr,"before") == 0) {
        pushxGenericCommand(c,c->argv[3],c->argv[4],REDIS_HEAD);
    } else {
        addReply(c,shared.syntaxerr);
    }
}

/* llen命令实现 */
void llenCommand(redisClient *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;
    addReplyLongLong(c,listTypeLength(o));
}

/* 返回指定下标的元素 */
void lindexCommand(redisClient *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;
    long index;
    robj *value = NULL;

    // 取出下标值
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != REDIS_OK))
        return;

    // 处理底层结构为ziplist的情况
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *p;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        // 根据下标值取出节点
        p = ziplistIndex(o->ptr,index);
        // 获取节点中存储的数值
        if (ziplistGet(p,&vstr,&vlen,&vlong)) {
            if (vstr) {
                value = createStringObject((char*)vstr,vlen);
            } else {
                value = createStringObjectFromLongLong(vlong);
            }
            addReplyBulk(c,value);
            decrRefCount(value);
        } else {
            addReply(c,shared.nullbulk);
        }
    } 
    // 处理底层结构为linked list的情况
    else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
    	// 根据下标值获取节点
        listNode *ln = listIndex(o->ptr,index);
        if (ln != NULL) {
            value = listNodeValue(ln);
            addReplyBulk(c,value);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else {
        redisPanic("Unknown list encoding");
    }
}

/* lset命令实现，用来设置指定下标元素的值 */
void lsetCommand(redisClient *c) {
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;
    long index;
    // 取出新值
    robj *value = (c->argv[3] = tryObjectEncoding(c->argv[3]));

    // 取出下标值
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != REDIS_OK))
        return;

    // 检查保存value值后是否需要编码转换
    listTypeTryConversion(o,value);
    // 不需要编码转换，直接在ziplis中更新
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *p, *zl = o->ptr;
        p = ziplistIndex(zl,index);
        if (p == NULL) {
            addReply(c,shared.outofrangeerr);
        } else {
        	// ziplist的节点更新操作：先删除旧值再插入新值
            o->ptr = ziplistDelete(o->ptr,&p);
            value = getDecodedObject(value);
            o->ptr = ziplistInsert(o->ptr,p,value->ptr,sdslen(value->ptr));
            decrRefCount(value);
            addReply(c,shared.ok);
            signalModifiedKey(c->db,c->argv[1]);
            notifyKeyspaceEvent(REDIS_NOTIFY_LIST,"lset",c->argv[1],c->db->id);
            server.dirty++;
        }
    } 
    // 处理底层结构为linked list的情况
    else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        listNode *ln = listIndex(o->ptr,index);
        if (ln == NULL) {
            addReply(c,shared.outofrangeerr);
        } else {
            decrRefCount((robj*)listNodeValue(ln));
            listNodeValue(ln) = value;
            incrRefCount(value);
            addReply(c,shared.ok);
            signalModifiedKey(c->db,c->argv[1]);
            notifyKeyspaceEvent(REDIS_NOTIFY_LIST,"lset",c->argv[1],c->db->id);
            server.dirty++;
        }
    } else {
        redisPanic("Unknown list encoding");
    }
}

/* pop命令的底层函数 */
void popGenericCommand(redisClient *c, int where) {
	// 取出listType对象
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;

    // 从表头或表尾（由where决定）弹出一个节点
    robj *value = listTypePop(o,where);
    // 根据弹出的元素是否为空执行后续操作

    if (value == NULL) {
    	// 弹出元素为空，发回消息
        addReply(c,shared.nullbulk);
    } else {
    	// 弹出元素不为空

        char *event = (where == REDIS_HEAD) ? "lpop" : "rpop";

        addReplyBulk(c,value);
        decrRefCount(value);
        notifyKeyspaceEvent(REDIS_NOTIFY_LIST,event,c->argv[1],c->db->id);
        // 检查弹出该元素后listType是否变成空列表，如果是则删除该listType
        if (listTypeLength(o) == 0) {
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",
                                c->argv[1],c->db->id);
            dbDelete(c->db,c->argv[1]);
        }
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }
}

/* lpop命令实现 */
void lpopCommand(redisClient *c) {
    popGenericCommand(c,REDIS_HEAD);
}

/* rpop命令实现 */
void rpopCommand(redisClient *c) {
    popGenericCommand(c,REDIS_TAIL);
}

/* lrange命令，获取指定下标范围的元素 */
void lrangeCommand(redisClient *c) {
    robj *o;
    long start, end, llen, rangelen;

    // 获取下标范围，主要是star开始下标值和end结束下标值
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

    // 取出listType对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
         || checkType(c,o,REDIS_LIST)) return;
    // 获得listType的长度
    llen = listTypeLength(o);

    /* convert negative indexes */
	// 将负数索引转换为正数索引，方便操作
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    // start越界重置
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    // 检查下标范围是否合法，不合法则直接返回
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    // end越界重置
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c,rangelen);
    // 处理底层结构为ziplist的情况
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
    	// 获取指定索引值的节点指针
        unsigned char *p = ziplistIndex(o->ptr,start);
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        // 遍历下标范围为[start, end]的元素，将其值添加到回复中
        while(rangelen--) {
            ziplistGet(p,&vstr,&vlen,&vlong);
            if (vstr) {
                addReplyBulkCBuffer(c,vstr,vlen);
            } else {
                addReplyBulkLongLong(c,vlong);
            }
            p = ziplistNext(o->ptr,p);
        }
    } 
    // 处理底层结构为linked list的情况
    else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        listNode *ln;

        /* If we are nearest to the end of the list, reach the element
         * starting from tail and going backward, as it is faster. */
        // 注意这里有一个加速查找的小技巧：如果start离linked list尾部近，则从其尾部开始查找
        if (start > llen/2) start -= llen;
        ln = listIndex(o->ptr,start);

        while(rangelen--) {
            addReplyBulk(c,ln->value);
            ln = ln->next;
        }
    } else {
        redisPanic("List encoding is not LINKEDLIST nor ZIPLIST!");
    }
}

/* ltrim命令实现 */
void ltrimCommand(redisClient *c) {
    robj *o;
    long start, end, llen, j, ltrim, rtrim;
    list *list;
    listNode *ln;

    // 获取下标范围，主要是star开始下标值和end结束下标值
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

   	// 取出listType对象
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    // 获得listType的长度
    llen = listTypeLength(o);

    /* convert negative indexes */
// 将负数索引转换为正数索引，方便操作
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    // 检查下标范围是否合法
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        // 如果start值超过下标范围或start > end，则裁剪后剩下空列表
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    // 处理底层结构为ziplist的情况
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
    	// 分别删除ziplis头部和尾部“多余”的元素
        o->ptr = ziplistDeleteRange(o->ptr,0,ltrim);
        o->ptr = ziplistDeleteRange(o->ptr,-rtrim,rtrim);
    } 
    // 处理底层结构为linked list的情形
    else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        list = o->ptr;
        // 删除左端元素
        for (j = 0; j < ltrim; j++) {
            ln = listFirst(list);
            listDelNode(list,ln);
        }
        // 删除右端元素
        for (j = 0; j < rtrim; j++) {
            ln = listLast(list);
            listDelNode(list,ln);
        }
    } else {
        redisPanic("Unknown list encoding");
    }

    notifyKeyspaceEvent(REDIS_NOTIFY_LIST,"ltrim",c->argv[1],c->db->id);
    // 如果列表为空，则从db中删除
    if (listTypeLength(o) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
    addReply(c,shared.ok);
}

/* lrem命令实现，删除指定值。命令格式为：lrem key count value */
void lremCommand(redisClient *c) {
    robj *subject, *obj;
    // 获取目标value值
    obj = c->argv[3] = tryObjectEncoding(c->argv[3]);
    long toremove;
    long removed = 0;
    listTypeEntry entry;

    // 取出count值
    if ((getLongFromObjectOrReply(c, c->argv[2], &toremove, NULL) != REDIS_OK))
        return;

    // 取出listType对象
    subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero);
    if (subject == NULL || checkType(c,subject,REDIS_LIST)) return;

    /* Make sure obj is raw when we're dealing with a ziplist */
    if (subject->encoding == REDIS_ENCODING_ZIPLIST)
        obj = getDecodedObject(obj);

    listTypeIterator *li;
    // 确定从表头开始删除还是从表尾开始删除
    if (toremove < 0) {
        toremove = -toremove;
        li = listTypeInitIterator(subject,-1,REDIS_HEAD);
    } else {
        li = listTypeInitIterator(subject,0,REDIS_TAIL);
    }

    // 遍历listType，并删除等于给定值的节点
    while (listTypeNext(li,&entry)) {
        if (listTypeEqual(&entry,obj)) {
            listTypeDelete(&entry);
            server.dirty++;
            // 记录被删除节点个数
            removed++;
            // 已经删除了足够数量的目标节点，停止并返回
            if (toremove && removed == toremove) break;
        }
    }
    listTypeReleaseIterator(li);

    /* Clean up raw encoded object */
    if (subject->encoding == REDIS_ENCODING_ZIPLIST)
        decrRefCount(obj);

    // 如果listType为空，则从db中删除
    if (listTypeLength(subject) == 0) dbDelete(c->db,c->argv[1]);
    addReplyLongLong(c,removed);
    if (removed) signalModifiedKey(c->db,c->argv[1]);
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */

/* rpoplpushCommand的辅助函数，将一个给定值lpush到目标listType */
void rpoplpushHandlePush(redisClient *c, robj *dstkey, robj *dstobj, robj *value) {
    /* Create the list if the key does not exist */
    /* 如果目标节点不存在则创建一个并加入到db中 */
    if (!dstobj) {
        dstobj = createZiplistObject();
        dbAdd(c->db,dstkey,dstobj);
    }
    signalModifiedKey(c->db,dstkey);
    listTypePush(dstobj,value,REDIS_HEAD);
    notifyKeyspaceEvent(REDIS_NOTIFY_LIST,"lpush",dstkey,c->db->id);
    /* Always send the pushed value to the client. */
    addReplyBulk(c,value);
}

/* rpoplpush命令实现，该命令为原子操作 */
void rpoplpushCommand(redisClient *c) {
    robj *sobj, *value;
    // 获取源列表，即执行rpop命令的listType对象
    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,sobj,REDIS_LIST)) return;

    // 源列表为空，则直接返回，因为没有元素可删除
    if (listTypeLength(sobj) == 0) {
        /* This may only happen after loading very old RDB files. Recent
         * versions of Redis delete keys of empty lists. */
        addReply(c,shared.nullbulk);
    } else {
    	// 获取目标列表对象
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        robj *touchedkey = c->argv[1];

        // 检查目标对象是否为List类型
        if (dobj && checkType(c,dobj,REDIS_LIST)) return;
        // 对源列表执行rpop命令
        value = listTypePop(sobj,REDIS_TAIL);
        /* We saved touched key, and protect it, since rpoplpushHandlePush
         * may change the client command argument vector (it does not
         * currently). */
        // 保存touchedKey
        incrRefCount(touchedkey);
        rpoplpushHandlePush(c,c->argv[2],dobj,value);

        /* listTypePop returns an object with its refcount incremented */
        decrRefCount(value);

        /* Delete the source list when it is empty */
        notifyKeyspaceEvent(REDIS_NOTIFY_LIST,"rpop",touchedkey,c->db->id);
        // 如果源列表为空，则从db中删除
        if (listTypeLength(sobj) == 0) {
            dbDelete(c->db,touchedkey);
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",
                                touchedkey,c->db->id);
        }
        signalModifiedKey(c->db,touchedkey);
        decrRefCount(touchedkey);
        server.dirty++;
    }
}

/*-----------------------------------------------------------------------------
 * Blocking POP operations  下面是pop操作的阻塞版本
 *----------------------------------------------------------------------------*/

/* This is how the current blocking POP works, we use BLPOP as example:
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if blocking is not required.
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blocking_keys) mapping keys to a list of clients
 *   blocking for this keys.
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we mark this key as "ready", and after the current command,
 *   MULTI/EXEC block, or script, is executed, we serve all the clients waiting
 *   for this list, from the one that blocked first, to the last, accordingly
 *   to the number of elements we have in the ready list.
 */
/*	下面以BLPOP操作讲解阻塞版POP操作的运行过程：
	（1）、如果用户执行BLPOP命令，且指定listType不为空，那么程序就直接调用非阻塞的LPOP命令
	（2）、如果用户执行BLPOP命令，且指定listType为空，这时需要阻塞操作。Redis将相应
	客户端的状态设置为“阻塞”状态，同时将该客户端添加到db->blocking_keys中。db->blocking_keys
	是一个字典结构，它的key为被阻塞的键，它的value是一个保存被阻塞客户端的列表。
	（3）、随后如果有PUSH命令往被阻塞的键中添加元素时，Redis将这个键标识为ready状态。当这个命令执
	行完毕后，Redis会按照先阻塞先服务的顺序将列表的元素返回给被阻塞的客户端，并且解除阻塞状态的客户端
	数量取决于PUSH命令添加的元素个数。
*/


/* Set a client in blocking mode for the specified key, with the specified
 * timeout */
/* 设置客户端对指定键的阻塞状态。参数keys可以指定任意数量的键，timeout指定超时时间，参数target即目标listType对象，
	主要用于brpoplpush命令，用户存放从源列表中pop出来的值。 */
void blockForKeys(redisClient *c, robj **keys, int numkeys, time_t timeout, robj *target) {
    dictEntry *de;
    list *l;
    int j;

    // 设置阻塞超时时间 
    c->bpop.timeout = timeout;
    // 设置目标选项，主要用于brpoplpush命令
    c->bpop.target = target;

    // target之拥入rpoplpush命令
    if (target != NULL) incrRefCount(target);

    // 在c->db->blocking_keys添加阻塞客户端和键的映射关系
    for (j = 0; j < numkeys; j++) {
        /* If the key already exists in the dict ignore it. */
        // bpop.keys记录所有阻塞的键
        if (dictAdd(c->bpop.keys,keys[j],NULL) != DICT_OK) continue;
        incrRefCount(keys[j]);

        /* And in the other "side", to map keys -> clients */
        // 维护阻塞键和被阻塞客户端的映射关系
        de = dictFind(c->db->blocking_keys,keys[j]);
        if (de == NULL) {
            int retval;

            /* For every key we take a list of clients blocked for it */
            // 如果该键对应的被阻塞客户端列表不存在，则创建一个
            l = listCreate();
            retval = dictAdd(c->db->blocking_keys,keys[j],l);
            incrRefCount(keys[j]);
            redisAssertWithInfo(c,keys[j],retval == DICT_OK);
        } else {
            l = dictGetVal(de);
        }
        // 并把当前被阻塞客户端阻塞列表中
        listAddNodeTail(l,c);
    }

    /* Mark the client as a blocked client */
    // 将客户端设置为“阻塞”状态
    c->flags |= REDIS_BLOCKED;
    server.bpop_blocked_clients++;
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP */
/* 解除某客户端的阻塞状态 */
void unblockClientWaitingData(redisClient *c) {
    dictEntry *de;
    dictIterator *di;
    list *l;

    redisAssertWithInfo(c,NULL,dictSize(c->bpop.keys) != 0);
    di = dictGetIterator(c->bpop.keys);
    /* The client may wait for multiple keys, so unblock it for every key. */
    // 一个客户端有可能因等待多个key而被阻塞，所以需要遍历所有的key，将对应的客户端从被阻塞列表中删除
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);

        /* Remove this client from the list of clients waiting for this key. */
        // 获取因为key而被阻塞的客户端列表
        l = dictFetchValue(c->db->blocking_keys,key);
        redisAssertWithInfo(c,key,l != NULL);
        // 从列表中删除指定客户端
        listDelNode(l,listSearchKey(l,c));
        /* If the list is empty we need to remove it to avoid wasting memory */
        // 如果列表为空，表示已经没有客户端因为key而阻塞，那么直接删除该列表以避免空间浪费
        if (listLength(l) == 0)
            dictDelete(c->db->blocking_keys,key);
    }
    dictReleaseIterator(di);

    /* Cleanup the client structure */
    // 情况bpop.keys
    dictEmpty(c->bpop.keys,NULL);
    if (c->bpop.target) {
        decrRefCount(c->bpop.target);
        c->bpop.target = NULL;
    }
    // 设置该客户端为“未阻塞”状态
    c->flags &= ~REDIS_BLOCKED;
    c->flags |= REDIS_UNBLOCKED;
    server.bpop_blocked_clients--;
    listAddNodeTail(server.unblocked_clients,c);
}

/* If the specified key has clients blocked waiting for list pushes, this
 * function will put the key reference into the server.ready_keys list.
 * Note that db->ready_keys is a hash table that allows us to avoid putting
 * the same key again and again in the list in case of multiple pushes
 * made by a script or in the context of MULTI/EXEC.
 *
 * The list will be finally processed by handleClientsBlockedOnLists() */
 /* 如果有客户端因为等待某个key的push操作而阻塞，则将该key放入server.ready_keys列表中。
 	db->ready_keys是一个哈希表，可以避免在一个事务或脚本中将同一个key一次又一次地添加到
 	列表中。

 	这个server.ready_keys列表最后会被handleClientsBlockedOnLists处理
 */
void signalListAsReady(redisDb *db, robj *key) {
	// readyList定义在redis.h中，表示server.ready_keys的一个节点
    readyList *rl;

    /* No clients blocking for this key? No need to queue it. */
    // 如果没有客户端因这个key而被阻塞，则直接返回
    if (dictFind(db->blocking_keys,key) == NULL) return;

    /* Key was already signaled? No need to queue it again. */
    // 如果这个key已经添加到ready_keys，为避免重复添加直接返回
    if (dictFind(db->ready_keys,key) != NULL) return;

    /* Ok, we need to queue this key into server.ready_keys. */
    // 创建一个readyList结构，然后添加到server.ready_keys尾部
    rl = zmalloc(sizeof(*rl));
    rl->key = key;
    rl->db = db;
    incrRefCount(key);
    listAddNodeTail(server.ready_keys,rl);

    /* We also add the key in the db->ready_keys dictionary in order
     * to avoid adding it multiple times into a list with a simple O(1)
     * check. */
    incrRefCount(key);
    // 将key添加到db->ready_keys中，避免重复添加
    redisAssert(dictAdd(db->ready_keys,key,NULL) == DICT_OK);
}

/* This is a helper function for handleClientsBlockedOnLists(). It's work
 * is to serve a specific client (receiver) that is blocked on 'key'
 * in the context of the specified 'db', doing the following:
 *
 * 1) Provide the client with the 'value' element.
 * 2) If the dstkey is not NULL (we are serving a BRPOPLPUSH) also push the
 *    'value' element on the destination list (the LPUSH side of the command).
 * 3) Propagate the resulting BRPOP, BLPOP and additional LPUSH if any into
 *    the AOF and replication channel.
 *
 * The argument 'where' is REDIS_TAIL or REDIS_HEAD, and indicates if the
 * 'value' element was popped fron the head (BLPOP) or tail (BRPOP) so that
 * we can propagate the command properly.
 *
 * The function returns REDIS_OK if we are able to serve the client, otherwise
 * REDIS_ERR is returned to signal the caller that the list POP operation
 * should be undone as the client was not served: This only happens for
 * BRPOPLPUSH that fails to push the value to the destination key as it is
 * of the wrong type. */
 /* 该函数是handleClientsBlockedOnLists的辅助函数，参数receiver表示被阻塞的客户端，参数key表示阻塞的键，
 	参数dstKey表示目标listType对象的键（用于brpoplpush命令），参数value表示一个新值。
	该函数执行下面的操作：
	（1）、将一个新值value提供给receiver
	（2）、如果dstKey不为空，则将新值添加到其指定的listType对象中（主要用于brpoplpush操作）
	（3）、将brpop、blpop和可能的lpush传播AOF和同步节点（这个我们以后在分析）

	参数where的取值有REDIS_TAIL和REDIS_HEAD，用于指明value是从列表头部还是尾部pop出来的。

	如果操作成功，该函数返回REDIS_OK，否则返回REDIS_ERR。
	如果操作失败，需要撤销对目标对象的pop操作，失败的情形只可能发生在brpoplpush操作中，比如pop操作成功执行，但是
	该命令指定的push操作的对象并不是一个listType类型，则操作失败。

 */
int serveClientBlockedOnList(redisClient *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int where)
{
    robj *argv[3];

    // 如果dstkey为空，则执行的是blpop或brpop命令
    if (dstkey == NULL) {
        /* Propagate the [LR]POP operation. */
        argv[0] = (where == REDIS_HEAD) ? shared.lpop :
                                          shared.rpop;
        argv[1] = key;
        propagate((where == REDIS_HEAD) ?
            server.lpopCommand : server.rpopCommand,
            db->id,argv,2,REDIS_PROPAGATE_AOF|REDIS_PROPAGATE_REPL);

        /* BRPOP/BLPOP */
        addReplyMultiBulkLen(receiver,2);
        addReplyBulk(receiver,key);
        addReplyBulk(receiver,value);
    } 
    // 如果dstkey为空，则执行的是brpoplpush命令
    else {
        /* BRPOPLPUSH */
        // 取出目标对象
        robj *dstobj =
            lookupKeyWrite(receiver->db,dstkey);
        // 如果目标对象存在则检查目标对象类型
        if (!(dstobj &&
             checkType(receiver,dstobj,REDIS_LIST)))
        {
            /* Propagate the RPOP operation. */
            argv[0] = shared.rpop;
            argv[1] = key;
            propagate(server.rpopCommand,
                db->id,argv,2,
                REDIS_PROPAGATE_AOF|
                REDIS_PROPAGATE_REPL);
            // 调用rpoplpushHandlePush将value添加到目标listType中
            rpoplpushHandlePush(receiver,dstkey,dstobj,
                value);
            /* Propagate the LPUSH operation. */
            argv[0] = shared.lpush;
            argv[1] = dstkey;
            argv[2] = value;
            propagate(server.lpushCommand,
                db->id,argv,3,
                REDIS_PROPAGATE_AOF|
                REDIS_PROPAGATE_REPL);
        } else {
            /* BRPOPLPUSH failed because of wrong
             * destination type. */
            return REDIS_ERR;
        }
    }
    return REDIS_OK;
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client.
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some PUSH operation are accumulated into
 * the server.ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BRPOPLPUSH we can have new blocking clients
 * to serve because of the PUSH side of BRPOPLPUSH. */
/*	该函数必须在Redis每次执行一条命令、一个事务或者lua脚本后都调用一次。
	
	对于所有key来说，如果有客户端被阻塞在该key上，只要该key被执行了push操作，那么这个key就会被放入
	server.ready_keys列表中。
	该函数会遍历server.ready_keys并将key中的元素弹出给被阻塞的客户端。
*/
void handleClientsBlockedOnLists(void) {
	// 遍历server.ready_keys列表
    while(listLength(server.ready_keys) != 0) {
        list *l;

        /* Point server.ready_keys to a fresh list and save the current one
         * locally. This way as we run the old list we are free to call
         * signalListAsReady() that may push new elements in server.ready_keys
         * when handling clients blocked into BRPOPLPUSH. */
        // 备份server.ready_keys，然后再给服务器创建一个新列表。接下来的操作都在备份server.ready_keys上进行
        l = server.ready_keys;
        server.ready_keys = listCreate();

        while(listLength(l) != 0) {
        	// 取出server.ready_keys的第一个节点
            listNode *ln = listFirst(l);
            readyList *rl = ln->value;

            /* First of all remove this key from db->ready_keys so that
             * we can safely call signalListAsReady() against this key. */
            // 从db->ready_keys删除就绪的key
            dictDelete(rl->db->ready_keys,rl->key);

            /* If the key exists and it's a list, serve blocked clients
             * with data. */
            // 获取listType对象
            robj *o = lookupKeyWrite(rl->db,rl->key);
            if (o != NULL && o->type == REDIS_LIST) {
                dictEntry *de;

                /* We serve clients in the same order they blocked for
                 * this key, from the first blocked to the last. */
                // 取出所有被这个key阻塞的客户端列表
                de = dictFind(rl->db->blocking_keys,rl->key);
                if (de) {
                    list *clients = dictGetVal(de);
                    int numclients = listLength(clients);

                    while(numclients--) {
                    	// 取出一个客户端
                        listNode *clientnode = listFirst(clients);
                        redisClient *receiver = clientnode->value;
                        // 设置pop出的目标对象
                        robj *dstkey = receiver->bpop.target;
                        // 从列表中弹出对象
                        int where = (receiver->lastcmd &&
                                     receiver->lastcmd->proc == blpopCommand) ?
                                    REDIS_HEAD : REDIS_TAIL;
                        robj *value = listTypePop(o,where);

                        // 如果listType还有元素，返回给相应客户端
                        if (value) {
                            /* Protect receiver->bpop.target, that will be
                             * freed by the next unblockClientWaitingData()
                             * call. */
                            if (dstkey) incrRefCount(dstkey);
                            // 解除相应客户端的阻塞状态
                            unblockClientWaitingData(receiver);

                            // 将pop出来的值返回给相应的客户端receiver
                            if (serveClientBlockedOnList(receiver,
                                rl->key,dstkey,rl->db,value,
                                where) == REDIS_ERR)
                            {
                                /* If we failed serving the client we need
                                 * to also undo the POP operation. */
                            	// 如果操作失败，则回滚（插入原listType对象）
                                    listTypePush(o,value,where);
                            }

                            if (dstkey) decrRefCount(dstkey);
                            decrRefCount(value);
                        } else {
                        	// 如果listType中没有元素了，没有元素可以返回剩余被阻塞客户端，只能等待以后的push操作
                            break;
                        }
                    }
                }

                // 如果列表元素已经为空，则删除之
                if (listTypeLength(o) == 0) dbDelete(rl->db,rl->key);
                /* We don't call signalModifiedKey() as it was already called
                 * when an element was pushed on the list. */
            }

            /* Free this item. */
            // 资源释放
            decrRefCount(rl->key);
            zfree(rl);
            listDelNode(l,ln);
        }
        listRelease(l); /* We have the new list on place at this point. */
    }
}

/* 获取timeout参数 */ 
int getTimeoutFromObjectOrReply(redisClient *c, robj *object, time_t *timeout) {
    long tval;

    if (getLongFromObjectOrReply(c,object,&tval,
        "timeout is not an integer or out of range") != REDIS_OK)
        return REDIS_ERR;

    if (tval < 0) {
        addReplyError(c,"timeout is negative");
        return REDIS_ERR;
    }

    if (tval > 0) tval += server.unixtime;
    *timeout = tval;

    return REDIS_OK;
}

/* Blocking RPOP/LPOP */
/* blpop和brpop命令的底层函数 */
void blockingPopGenericCommand(redisClient *c, int where) {
    robj *o;
    time_t timeout;
    int j;

    // 取出timeout参数
    if (getTimeoutFromObjectOrReply(c,c->argv[c->argc-1],&timeout) != REDIS_OK)
        return;

    // 遍历所有的输入键
    for (j = 1; j < c->argc-1; j++) {
    	// 取出相应的listType
        o = lookupKeyWrite(c->db,c->argv[j]);
        if (o != NULL) {
        	// 类型检查
            if (o->type != REDIS_LIST) {
                addReply(c,shared.wrongtypeerr);
                return;
            } else {
            	// 如果listType不为空，则转换为普通的pop操作
                if (listTypeLength(o) != 0) {
                    /* Non empty list, this is like a non normal [LR]POP. */
                    char *event = (where == REDIS_HEAD) ? "lpop" : "rpop";
                    robj *value = listTypePop(o,where);
                    redisAssert(value != NULL);

                    addReplyMultiBulkLen(c,2);
                    addReplyBulk(c,c->argv[j]);
                    addReplyBulk(c,value);
                    decrRefCount(value);
                    notifyKeyspaceEvent(REDIS_NOTIFY_LIST,event,
                                        c->argv[j],c->db->id);
                    // 如果弹出元素后列表为空，删除之
                    if (listTypeLength(o) == 0) {
                        dbDelete(c->db,c->argv[j]);
                        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",
                                            c->argv[j],c->db->id);
                    }
                    signalModifiedKey(c->db,c->argv[j]);
                    server.dirty++;

                    /* Replicate it as an [LR]POP instead of B[LR]POP. */
                    rewriteClientCommandVector(c,2,
                        (where == REDIS_HEAD) ? shared.lpop : shared.rpop,
                        c->argv[j]);
                    return;
                }
            }
        }
    }

    /* If we are inside a MULTI/EXEC and the list is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    // 如果用户在一个事务中执行阻塞命令，则返回一个空回复。这样做为了避免死等
    if (c->flags & REDIS_MULTI) {
        addReply(c,shared.nullmultibulk);
        return;
    }

    /* If the list is empty or the key does not exists we must block */
   	// 阻塞
    blockForKeys(c, c->argv + 1, c->argc - 2, timeout, NULL);
}

/* blpop命令实现 */
void blpopCommand(redisClient *c) {
    blockingPopGenericCommand(c,REDIS_HEAD);
}

/* brpop命令实现 */
void brpopCommand(redisClient *c) {
    blockingPopGenericCommand(c,REDIS_TAIL);
}

/* brpoplpush命令实现 */
void brpoplpushCommand(redisClient *c) {
    time_t timeout;

    // 获取timeout参数
    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout) != REDIS_OK)
        return;

    robj *key = lookupKeyWrite(c->db, c->argv[1]);

    if (key == NULL) {
        if (c->flags & REDIS_MULTI) {
            /* Blocking against an empty list in a multi state
             * returns immediately. */
            addReply(c, shared.nullbulk);
        } else {
            /* The list is empty and the client blocks. */
            // 阻塞之
            blockForKeys(c, c->argv + 1, 1, timeout, c->argv[2]);
        }
    } 
    // 如果listType非空，则转为执行rpoplpush命令
    else {
        if (key->type != REDIS_LIST) {
            addReply(c, shared.wrongtypeerr);
        } else {
            /* The list exists and has elements, so
             * the regular rpoplpushCommand is executed. */
            redisAssertWithInfo(c,key,listTypeLength(key) > 0);
            rpoplpushCommand(c);
        }
    }
}
