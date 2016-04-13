/* SDSLib, A C dynamic strings library
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

#ifndef __SDS_H
#define __SDS_H

/* 最大的可分配内存1024*1024bytes = 1M */
#define SDS_MAX_PREALLOC (1024*1024)

#include <sys/types.h>
#include <stdarg.h>

/* 为char *类型定义别名为sds */
typedef char *sds;

/* 字符串结构体（字符串就是字符数组） */
struct sdshdr {
	// 字符串当前长度
    unsigned int len;
    // 剩余可用长度
    unsigned int free;
    // 字符数组（具体存放字符串的地方）
    char buf[];
};

/*	下面两个是static函数，仅在本文件可见 */

/* 获取字符串长度 */
static inline size_t sdslen(const sds s) {
	//	sizeof(struct sdshdr)的值为8
	/*	这里为什么用s-(sizeof(struct sdshdr))就得到sdshdr *指针？
		从后面我们可以看到sds指向sdshdr结构的buf[]字符数组，所以
		s-(sizeof(struct sdshdr))就是sdshdr结构的地址。
	*/
    struct sdshdr *sh = (void*)(s-(sizeof(struct sdshdr)));
    return sh->len;
}

/* 获取字符数组中的可用空间 */
static inline size_t sdsavail(const sds s) {
	//	sizeof(struct sdshdr)的值为8
    struct sdshdr *sh = (void*)(s-(sizeof(struct sdshdr)));
    return sh->free;
}

/* 下面是字符串的操作函数，从其实现上看sds指向sdshdr结构的buf[]字符数组。所以
	所“创建sds”和“创建sdshdr结构”是一致的。
*/


/* 根据给初始化参数init和给定长度创建新的字符串 */
sds sdsnewlen(const void *init, size_t initlen);
/* 根据给定的值创建sds */
sds sdsnew(const char *init);
/* 创建一个空字符串 */
sds sdsempty(void);
/* 获取sds长度*/
size_t sdslen(const sds s);
/* 复制一个sds */
sds sdsdup(const sds s);
/* 释放sds的内存空间 */
void sdsfree(sds s);
/* 获取sds的可用空间 */
size_t sdsavail(const sds s);
/* 扩展字符串到指定的长度 */
sds sdsgrowzero(sds s, size_t len);
/* 字符串连接操作 */
sds sdscatlen(sds s, const void *t, size_t len);
/* 字符串连接操作 */
sds sdscat(sds s, const char *t);
/* 字符串连接操作 */
sds sdscatsds(sds s, const sds t);
/* 字符串复制操作 */
sds sdscpylen(sds s, const char *t, size_t len);
/* 字符串复制操作 */
sds sdscpy(sds s, const char *t);

/* 字符串格式化输出操作 */
sds sdscatvprintf(sds s, const char *fmt, va_list ap);
#ifdef __GNUC__
sds sdscatprintf(sds s, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
 /* 格式化输入，类似C语言中的sprintf函数 */
sds sdscatprintf(sds s, const char *fmt, ...);
#endif

/* 字符串格式化输出 */
sds sdscatfmt(sds s, char const *fmt, ...);
/* 字符串的trim操作，高级语言普遍提供 */
sds sdstrim(sds s, const char *cset);
/* 字符串截取 */
void sdsrange(sds s, int start, int end);
/* 更新字符串的长度，考虑下面这种情况
	s = sdsnew("foobar");
 	s[2] = '\0';
 	这是就需要调用sdsupdatelen(s)更新字符串长度，底层是使用strlen计算字符串长度
 */
void sdsupdatelen(sds s);
/* 清空字符串 */
void sdsclear(sds s);
/* 字符串比较操作 */
int sdscmp(const sds s1, const sds s2);
/* 字符串分割操作 */
sds *sdssplitlen(const char *s, int len, const char *sep, int seplen, int *count);
/* 释放sdssplitlen函数返回的sds数组 */
void sdsfreesplitres(sds *tokens, int count);
/* 统一转换为小写字符 */
void sdstolower(sds s);
/* 统一转换为大写字符 */
void sdstoupper(sds s);
/* 将一个long long类型的数字转换为字符串 */
sds sdsfromlonglong(long long value);
/* 添加引用字符串 */
sds sdscatrepr(sds s, const char *p, size_t len);
/* 参数解析 */
sds *sdssplitargs(const char *line, int *argc);
/* 字符替换操作 */
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen);
/* 将一个C风格的字符串数组用指定分隔符连接成一个字符串*/
sds sdsjoin(char **argv, int argc, char *sep);

/* Low level functions exposed to the user API */

/* 确保sds中的可用空间大于或等于addlen，如果当前字符串可用空间不满足则重新配置空间 */
sds sdsMakeRoomFor(sds s, size_t addlen);
/* 根据给定参数incr调整当前长度和可用空间大小 */
void sdsIncrLen(sds s, int incr);
/* 释放字符数组buf中的多余空间，使其刚好能存放当前字符数 */
sds sdsRemoveFreeSpace(sds s);
/* 获取sds实际分配的空间大小（包括最后的'\0'结束符） */
size_t sdsAllocSize(sds s);

#endif
