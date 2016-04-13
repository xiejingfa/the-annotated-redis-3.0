/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdint.h>

#ifndef __DICT_H
#define __DICT_H

// 成功 or 出错
#define DICT_OK 0
#define DICT_ERR 1

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void) V)

/* 保存键值（key - value）对的结构体，类似于STL的pair。Redis采用拉链法处理冲突，
    会把冲突的dictEntry组成一个链表。
*/
typedef struct dictEntry {
    // 关键字key定义
    void *key;  
    // 值value定义，这里采用了联合体，根据union的特点，联合体只能存放一个被选中的成员
    union {
        void *val;      // 自定义类型
        uint64_t u64;   // 无符号整形
        int64_t s64;    // 有符号整形
        double d;       // 浮点型
    } v;
    // 指向下一个键值对节点
    struct dictEntry *next;
} dictEntry;

/* 定义了字典操作的公共方法，类似于adlist.h文件中list的定义，将对节点的公共操作方法统一定义。搞不明白为什么要命名为dictType */
typedef struct dictType {
    /* hash方法，根据关键字计算哈希值 */
    unsigned int (*hashFunction)(const void *key);
    /* 复制key */
    void *(*keyDup)(void *privdata, const void *key);
    /* 复制value */
    void *(*valDup)(void *privdata, const void *obj);
    /* 关键字比较方法 */
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);
    /* 销毁key */
    void (*keyDestructor)(void *privdata, void *key);
    /* 销毁value */
    void (*valDestructor)(void *privdata, void *obj);
} dictType;

/* This is our hash table structure. Every dictionary has two of this as we
 * implement incremental rehashing, for the old to the new table. */
/* 哈希表结构 */
typedef struct dictht {
    // 散列数组。哈希表内部是基于数组，数组的元素是dictEntry *类型，所以这里是指针数组。
    dictEntry **table;
    // 散列数组的长度
    unsigned long size;
    // sizemask等于size减1
    unsigned long sizemask;
    // 散列数组中已经被使用的节点数量
    unsigned long used;
} dictht;

/* 字典的主操作类，对dictht结构再次包装  */
typedef struct dict {
    // 字典类型
    dictType *type;
    // 私有数据指针
    void *privdata;
    // 一个字典中有两个哈希表，后面的分析中，我们将ht[0]称作就表，ht[1]称作新表
    /*  dict的rehash。通常情况下，所有的数据都是存在放dict的ht[0]中，ht[1]只在rehash的时候使用。
        dict进行rehash操作的时候，将ht[0]中的所有数据rehash到ht[1]中。然后将ht[1]赋值给ht[0]，
        并清空ht[1]。rehash操作我们会在后面详细看到。
    */
    dictht ht[2];
    // 数据动态迁移的位置，如果rehashidx == -1说明当前没有执行rehash操作
    long rehashidx; /* rehashing not in progress if rehashidx == -1 */
    // 当前正在使用的迭代器的数量
    int iterators; /* number of iterators currently running */
} dict;

/* If safe is set to 1 this is a safe iterator, that means, you can call
 * dictAdd, dictFind, and other functions against the dictionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictNext()
 * should be called while iterating. */
 /* dict的迭代器定义，该迭代器有安全和不安全之分，这个跟dict的rehash操作有关，我们后面会详细说 */
typedef struct dictIterator {
    /* 当前使用的字典dict */
    dict *d;
    /* 当前迭代器下标 */
    long index;
    /* table指示字典中散列表下标，safe指明该迭代器是否安全 */
    int table, safe;
    /* 键值对节点指针 */
    dictEntry *entry, *nextEntry;
    /* unsafe iterator fingerprint for misuse detection. */
    long long fingerprint;
} dictIterator;

/* 遍历回调函数 */
typedef void (dictScanFunction)(void *privdata, const dictEntry *de);

/* This is the initial size of every hash table */
/* 散列数组的初始长度 */
#define DICT_HT_INITIAL_SIZE     4

/* 下面是节点（键值对）操作的宏定义 */

/* ------------------------------- Macros ------------------------------------*/
/* 释放节点value，实际上调用dictType中的valDestructor函数 */
#define dictFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d)->privdata, (entry)->v.val)

/* 设置节点value */
#define dictSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        entry->v.val = (d)->type->valDup((d)->privdata, _val_); \
    else \
        entry->v.val = (_val_); \
} while(0)

/* 设置节点的值value，类型为signed int */
#define dictSetSignedIntegerVal(entry, _val_) \
    do { entry->v.s64 = _val_; } while(0)

/* 设置节点的值value，类型为unsigned int */
#define dictSetUnsignedIntegerVal(entry, _val_) \
    do { entry->v.u64 = _val_; } while(0)

/* 设置节点的值value，类型为double */
#define dictSetDoubleVal(entry, _val_) \
    do { entry->v.d = _val_; } while(0)

/* 释放节点的键key */
#define dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d)->privdata, (entry)->key)

/* 设置节点的键key */
#define dictSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        entry->key = (d)->type->keyDup((d)->privdata, _key_); \
    else \
        entry->key = (_key_); \
} while(0)

// 判断两个key是否相等
#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d)->privdata, key1, key2) : \
        (key1) == (key2))

/* 获取指定key的哈希值*/
#define dictHashKey(d, key) (d)->type->hashFunction(key)
/* 获取节点的key值*/
#define dictGetKey(he) ((he)->key)
/* 获取节点的value值 */
#define dictGetVal(he) ((he)->v.val)
/* 获取节点的value值，类型为signed int */
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
/* 获取节点的value值，类型为unsigned int */
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
/* 获取节点的value值，类型为double */
#define dictGetDoubleVal(he) ((he)->v.d)
/* 获取字典中哈希表的总长度 */
#define dictSlots(d) ((d)->ht[0].size+(d)->ht[1].size)
/* 获取字典中哈希表中已经被使用的节点数量 */
#define dictSize(d) ((d)->ht[0].used+(d)->ht[1].used)
/* 判断字典dict是否正在执行rehash操作 */
#define dictIsRehashing(d) ((d)->rehashidx != -1)

/* API */
/* 下面定义了操作字典的方法 */

// 创建一个字典dict
dict *dictCreate(dictType *type, void *privDataPtr);
// 字典容量扩充
int dictExpand(dict *d, unsigned long size);
// 根据key和value往字典中添加一个键值对
int dictAdd(dict *d, void *key, void *val);
// 往字典中添加一个只有key的dictEntry结构
dictEntry *dictAddRaw(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
dictEntry *dictReplaceRaw(dict *d, void *key);
// 根据key删除字典中的一个键值对
int dictDelete(dict *d, const void *key);
int dictDeleteNoFree(dict *d, const void *key);
// 释放整个字典
void dictRelease(dict *d);
// 根据key在字典中查找一个键值对
dictEntry * dictFind(dict *d, const void *key);
// 根据key在字典中查找对应的value
void *dictFetchValue(dict *d, const void *key);
// 重新计算字典大小
int dictResize(dict *d);
// 获取字典的普通迭代器
dictIterator *dictGetIterator(dict *d);
// 获取字典的安全迭代器
dictIterator *dictGetSafeIterator(dict *d);
// 根据迭代器获取下一个键值对
dictEntry *dictNext(dictIterator *iter);
// 释放迭代器
void dictReleaseIterator(dictIterator *iter);
// 随机获取一个键值对
dictEntry *dictGetRandomKey(dict *d);
// 打印字典当前状态
void dictPrintStats(dict *d);
unsigned int dictGenHashFunction(const void *key, int len);
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len);
// 清空字典
void dictEmpty(dict *d, void(callback)(void*));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(unsigned int initval);
unsigned int dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, void *privdata);

/* Hash table types */
/* 哈希表类型 */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif /* __DICT_H */
