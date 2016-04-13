/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

#define ZIPLIST_HEAD 0
#define ZIPLIST_TAIL 1

/* 创建一个空的ziplist */
unsigned char *ziplistNew(void);
/* 往ziplist的头部或尾部插入一个节点 */
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where);
/* 根据索引了获取ziplist节点 */
unsigned char *ziplistIndex(unsigned char *zl, int index);
 /* 获取ziplist中p节点的下一个节点,其实就是指针操作 */
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p);
/* 获取ziplist中p节点的前一个节点 */
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p);
 /* 获取p指针指向的当前节点的值 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sval, unsigned int *slen, long long *lval);
/* 在p指针指向的位置插入一个节点，底层通过__ziplistInsert实现 */
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);
 /* 删除p指针指向的节点，操作成功后p指向被删除节点下一个节点 */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p);
/* 删除连续的一批节点 */
unsigned char *ziplistDeleteRange(unsigned char *zl, unsigned int index, unsigned int num);
/* 将p指针指向的节点的值与sstr对应的值作比较，如果两则相等返回1，否则返回0 */
unsigned int ziplistCompare(unsigned char *p, unsigned char *s, unsigned int slen);
/* 在ziplist查找包含给定数据的节点，可以通过参数skip指定跳过的节点数 */
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip);
/* 获取ziplist链表中元素的个数 */
unsigned int ziplistLen(unsigned char *zl);
/* 获取整个ziplist占用的字节数 */
size_t ziplistBlobLen(unsigned char *zl);
