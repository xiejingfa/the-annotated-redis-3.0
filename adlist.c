/* adlist.c - A generic doubly linked list implementation
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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


#include <stdlib.h>
#include "adlist.h"
#include "zmalloc.h"

/* Create a new list. The created list can be freed with
 * AlFreeList(), but private value of every node need to be freed
 * by the user before to call AlFreeList().
 *
 * On error, NULL is returned. Otherwise the pointer to the new list. */
// 创建一个list
list *listCreate(void)
{
    struct list *list;

    // 申请空间，zmalloc函数是redis定义的空间配置函数，我们后面会分析其原理，现在就暂时把它等同于malloc
    if ((list = zmalloc(sizeof(*list))) == NULL)
        return NULL;
    // 进行“常规”的初始化，这些跟我们在《数据结构》中学习的过程基本一致，注意这里还把三个函数指针设为null
    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;
    return list;
}

/* Free the whole list.
 *
 * This function can't fail. */
 // 释放整个链表
void listRelease(list *list)
{
    // 先销毁链表中的所有节点
    unsigned long len;
    listNode *current, *next;

    current = list->head;
    len = list->len;
    while(len--) {
        next = current->next;
        // 通过list中的回调函数来释放每一节点数据域的内存空间
        if (list->free) list->free(current->value);
        // 销毁节点空间，zfree是redis定义的空间释放函数，和zmalloc对应
        zfree(current);
        current = next;
    }
    // 最后把链表也销毁
    zfree(list);
}

/* Add a new node to the list, to head, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
// 在表头插入一个节点
list *listAddNodeHead(list *list, void *value)
{
    // 定义一个新的节点并为其申请空间
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    // 设置节点的数据域
    node->value = value;
    // 往链表头部插入一个新节点需要考虑链表为空的情况
    if (list->len == 0) {   // 链表为空
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        // 链表不为空时的插入过程，主要是指针操作，最后新节点成为头结点。
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    // 最后别忘了更新链表长度
    list->len++;
    return list;
}

/* Add a new node to the list, to tail, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
// 在表尾插入一个节点
list *listAddNodeTail(list *list, void *value)
{
    // 定义一个新的节点并为其申请空间
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    // 设置节点的数据域
    node->value = value;
    // 往链表尾部插入一个新节点需要考虑链表为空的情况
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        // 链表不为空时的插入过程，主要是指针操作，最后新节点成为尾节点
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }
    // 最后更新链表长度
    list->len++;
    return list;
}

// 在old_value元素的前面或后面（由after参数决定）插入一个新节点
list *listInsertNode(list *list, listNode *old_node, void *value, int after) {
    // 定义一个新的节点并为其申请空间
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    // 设置节点的数据域
    node->value = value;
    if (after) {
        // 在目标节点的后面插入一个新节点，需要考虑目标节点为尾节点的情况
        node->prev = old_node;
        node->next = old_node->next;
        if (list->tail == old_node) {
            list->tail = node;
        }
    } else {
        // 在目标节点的前面插入一个新节点，需要考虑目标节点为头结点的情况
        node->next = old_node;
        node->prev = old_node->prev;
        if (list->head == old_node) {
            list->head = node;
        }
    }
    // 将插入节点前后的节点链接起来
    if (node->prev != NULL) {
        node->prev->next = node;
    }
    if (node->next != NULL) {
        node->next->prev = node;
    }
    // 更新链表长度
    list->len++;
    return list;
}

/* Remove the specified node from the specified list.
 * It's up to the caller to free the private value of the node.
 *
 * This function can't fail. */
 // 删除一个节点
void listDelNode(list *list, listNode *node)
{
    // 删除链表中的一个节点要考虑被删除节点是否为头结点或尾节点

    if (node->prev)
        //  如果node->prev存在，说明该节点不是头结点，直接将prev节点的next指向被删除节点的下一个
        node->prev->next = node->next;
    else
        // 被删除节点是头结点，让被删除节点的下一个节点成为头结点
        list->head = node->next;

    if (node->next)
        // 如果node->next存在，说明该节点不是尾节点
        node->next->prev = node->prev;
    else
        // 被删除节点是尾节点
        list->tail = node->prev;

    // 回调free函数删除节点的数据域
    if (list->free) list->free(node->value);
    // 释放节点空间
    zfree(node);
    // 最后更新链表长度
    list->len--;
}

/* Returns a list iterator 'iter'. After the initialization every
 * call to listNext() will return the next element of the list.
 *
 * This function can't fail. */
 // 获取一个给定方向上的迭代器
listIter *listGetIterator(list *list, int direction)
{
    // 定义一个迭代器并为其分配空间
    listIter *iter;

    // 这里需要注意，调用该函数时分配了动态内存，使用完需要手动释放。这一点我们可以在后面代码中看到
    // listGetIterator和listReleaseIterator是成对出现的
    if ((iter = zmalloc(sizeof(*iter))) == NULL) return NULL;
    if (direction == AL_START_HEAD)
        // 如果该迭代器的方向是从前往后，则迭代器的next指针指向头结点
        iter->next = list->head;
    else
        // 如果该迭代器的方向是从后往前，则迭代器的next指针指向尾节点
        iter->next = list->tail;
    // 设置迭代器方向
    iter->direction = direction;
    return iter;
}

/* Release the iterator memory */
// 释放迭代器
void listReleaseIterator(listIter *iter) {
    zfree(iter);
}

/* Create an iterator in the list private iterator structure */
// 重置迭代器为链表头结点
void listRewind(list *list, listIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

// 重置迭代器为链表尾结点
void listRewindTail(list *list, listIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}

/* Return the next element of an iterator.
 * It's valid to remove the currently returned element using
 * listDelNode(), but not to remove other elements.
 *
 * The function returns a pointer to the next element of the list,
 * or NULL if there are no more elements, so the classical usage patter
 * is:
 *
 * iter = listGetIterator(list,<direction>);
 * while ((node = listNext(iter)) != NULL) {
 *     doSomethingWith(listNodeValue(node));
 * }
 *
 * */
 // 获取给定迭代器的下一个节点
listNode *listNext(listIter *iter)
{
    // 获取当前迭代器所指向的节点（不要被iter->next中的next迷惑，listIter中的next字段指向一个节点）
    listNode *current = iter->next;

    // 根据迭代器的方向将下一个节点设置为当前节点。对于双向链表来说向前和向后移动都很方便
    if (current != NULL) {
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }
    return current;
}

/* Duplicate the whole list. On out of memory NULL is returned.
 * On success a copy of the original list is returned.
 *
 * The 'Dup' method set with listSetDupMethod() function is used
 * to copy the node value. Otherwise the same pointer value of
 * the original node is used as value of the copied node.
 *
 * The original list both on success or error is never modified. */
// 复制一个list
list *listDup(list *orig)
{
    list *copy;     // 复制后的新链表
    listIter *iter; // 迭代器
    listNode *node; //  链表节点

    if ((copy = listCreate()) == NULL)
        return NULL;
    // 设置新链表的三个函数指针
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;
    // 获取从前往后的迭代器，需要手动释放
    iter = listGetIterator(orig, AL_START_HEAD);
    // 遍历旧链表，将每个节点复制一份放入新链表
    while((node = listNext(iter)) != NULL) {
        // 复制一个节点的时候要考虑链表中的函数指针dup有没有设置
        void *value;

        if (copy->dup) {
            // 如果设置了节点数据域的复制方法dup，直接调用该方法即可
            value = copy->dup(node->value);
            if (value == NULL) {    // 出错处理
                listRelease(copy);
                listReleaseIterator(iter);
                return NULL;
            }
        } else
            // 如果没有定义dup方法，则直接复制指针，这样两者指向统一内存区域
            value = node->value;
            // 调用listAddNodeTail函数从链表尾部插入节点
        if (listAddNodeTail(copy, value) == NULL) {
            // 出错处理
            listRelease(copy);
            listReleaseIterator(iter);
            return NULL;
        }
    }
    // 最后释放迭代器
    listReleaseIterator(iter);
    return copy;
}

/* Search the list for a node matching a given key.
 * The match is performed using the 'match' method
 * set with listSetMatchMethod(). If no 'match' method
 * is set, the 'value' pointer of every node is directly
 * compared with the 'key' pointer.
 *
 * On success the first matching node pointer is returned
 * (search starts from head). If no matching node exists
 * NULL is returned. */
 // 根据关键字查找list
listNode *listSearchKey(list *list, void *key)
{
    listIter *iter; // 链表迭代器
    listNode *node; // 链表节点

    // 获取从前往后的迭代器，需要手动释放
    iter = listGetIterator(list, AL_START_HEAD);
    while((node = listNext(iter)) != NULL) {
        // 从前往后遍历遍历链表，直到找到目标节点
        if (list->match) {
            // 如果链表list设置了match函数，则用match函数判断当前节点是否和给点节点相同
            if (list->match(node->value, key)) {
                listReleaseIterator(iter);  // 释放迭代器
                return node;
            }
        } else {
            // 如果链表list没有设置了match函数，则比较指针是否相等
            if (key == node->value) {
                listReleaseIterator(iter);  // // 释放迭代器
                return node;
            }
        }
    }
    listReleaseIterator(iter);  // 释放迭代器
    return NULL;
}

/* Return the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range NULL is returned. */
 // 根据下标索引查找list
listNode *listIndex(list *list, long index) {
    listNode *n;

    // redis在这点上支持正向下标和反向下标，这点从其命令操作就可以知道
    if (index < 0) {
        // 如果index为负数，则从后往前数，最后一个节点的下标为-1
        index = (-index)-1;
        n = list->tail;
        while(index-- && n) n = n->prev;
    } else {
        // 如果index为正数数，则从前往后数，第一个节点的下标为0
        n = list->head;
        while(index-- && n) n = n->next;
    }
    return n;
}

/* Rotate the list removing the tail node and inserting it to the head. */
// 将最后一个节点移动到头部
void listRotate(list *list) {
    listNode *tail = list->tail;

    // 如果链表中的节点数目小于2，则直接返回
    if (listLength(list) <= 1) return;

    // 设置新的尾部节点（注意指针的操作顺序）
    /* Detach current tail */
    list->tail = tail->prev;
    list->tail->next = NULL;
    /* Move it as head */
    // 调整指针，设置新的头结点
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}
