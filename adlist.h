/* adlist.h - A generic doubly linked list implementation
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

#ifndef __ADLIST_H__
#define __ADLIST_H__

/* Node, List, and Iterator are the only data structures used currently. */

/* 定义双向链表的节点 */
typedef struct listNode {
    // 前一个节点
    struct listNode *prev;
    // 下一个节点
    struct listNode *next;
    // 指向节点存储的数据。C语言中没有泛型的概念，利用void*指针来“模拟”泛型操作
    void *value;
} listNode;

/* 双向链表的迭代器 */
typedef struct listIter {
    // 指向当前节点
    listNode *next;
    // 迭代器方向
    int direction;
} listIter;

/* 双向链表的定义 */
typedef struct list {
    // 链表头节点
    listNode *head;
    // 链表为节点
    listNode *tail;

    /* 定义三个函数指针 
        为什么要定义这三个函数指针？因为listnode中的数据区域为一个void类型的指针，
        所指向的结构可能千差万别，而且这些内存需要手动释放。将常用的这几个函数定义
        在这里，可以在需要的时候直接回调。
    */
    void *(*dup)(void *ptr);    //  复制
    void (*free)(void *ptr);    //  释放
    int (*match)(void *ptr, void *key); // 匹配查找
    // 链表长度
    unsigned long len;
} list;

/* Functions implemented as macros */
/* 宏定义，主要是链表的一些基本操作 */
// 获取长度
#define listLength(l) ((l)->len)
// 获取头结点
#define listFirst(l) ((l)->head)
// 获取尾节点
#define listLast(l) ((l)->tail)
// 获取前一个节点
#define listPrevNode(n) ((n)->prev)
// 获取下一个节点
#define listNextNode(n) ((n)->next)
// 获取节点的值，是一个void类型的指针
#define listNodeValue(n) ((n)->value)

/* 下面三个宏定义主要用来设置list结构中的三个函数指针, 参数m为method的意思 */
#define listSetDupMethod(l,m) ((l)->dup = (m))
#define listSetFreeMethod(l,m) ((l)->free = (m))
#define listSetMatchMethod(l,m) ((l)->match = (m))

/* 下面三个宏定义主要用来获取list结构的单个函数指针 */
#define listGetDupMethod(l) ((l)->dup)
#define listGetFree(l) ((l)->free)
#define listGetMatchMethod(l) ((l)->match)

/* Prototypes */
/* 操作list相关的函数原型声明 */
// 创建一个list
list *listCreate(void);
// 销毁一个给定的list结构
void listRelease(list *list);
// 在表头插入一个节点
list *listAddNodeHead(list *list, void *value);
// 在表尾插入一个节点
list *listAddNodeTail(list *list, void *value);
// 在指定的位置上插入一个节点
list *listInsertNode(list *list, listNode *old_node, void *value, int after);
// 删除一个节点
void listDelNode(list *list, listNode *node);
// 获取一个给定方向上的迭代器
listIter *listGetIterator(list *list, int direction);
// 获取给定迭代器的下一个节点
listNode *listNext(listIter *iter);
// 释放迭代器
void listReleaseIterator(listIter *iter);
// 复制一个list
list *listDup(list *orig);
// 根据关键字查找list
listNode *listSearchKey(list *list, void *key);
// 根据下标索引查找list
listNode *listIndex(list *list, long index);
// 重置迭代器为链表头结点
void listRewind(list *list, listIter *li);
// 重置迭代器为链表尾节点
void listRewindTail(list *list, listIter *li);
// 从函数名看不出该函数的作用，后面我们看看源码就明白这个函数只是将最后一个节点移动到头部
void listRotate(list *list);

/* Directions for iterators */
/* 迭代器的迭代方向，AL_START_HEAD从前往后，AL_START_TAIL从后往前 */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */
