/* String -> String Map data structure optimized for size.
 *  zipmap是为了节省内存空间而设计的字符串-字符串映射结构（哈希结构）
 * 
 * This file implements a data structure mapping strings to other strings
 * implementing an O(n) lookup data structure designed to be very memory
 * efficient.
 *  zipmap实现了一种将字符串与字符串之间映射起来的数据结构，它支持O(n)的查找效率并具有良好的空间利用率。
 *
 * The Redis Hash type uses this data structure for hashes composed of a small
 * number of elements, to switch to a hash table once a given number of
 * elements is reached.
 *  Redis中内置的Hash结构在保存的元素数量较少时会采用zipmap来存放键值对，当元素的数量到达给定值
 *  后才会转为用哈希表来存储以节省内存。
 *
 * Given that many times Redis Hashes are used to represent objects composed
 * of few fields, this is a very big win in terms of used memory.
 *  Redis中的Hash结构经常被用来保存只有少量字段的对象，这种情况下使用zipmap能很大程度上节省内存
 *
 * --------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

/* Memory layout of a zipmap, for the map "foo" => "bar", "hello" => "world":
 *  zipmap的内存空间布局如下（假设存放着"foo" => "bar", "hello" => "world"数据）：
 *
 * <zmlen><len>"foo"<len><free>"bar"<len>"hello"<len><free>"world"
 *
 * <zmlen> is 1 byte length that holds the current size of the zipmap.
 * When the zipmap length is greater than or equal to 254, this value
 * is not used and the zipmap needs to be traversed to find out the length.
 *  zmlen是1个字节的无符号整型数，表示zipmap当前的键值对数量，最多只能表示253个键值对。当zipmap中的元素数量大于
 *  或等于254时，只能通过遍历一遍zipmap来确定其大小。
 *
 * <len> is the length of the following string (key or value).
 *  len表示随后的字符串的长度，这些字符串可能是key或者value
 *
 * <len> lengths are encoded in a single value or in a 5 bytes value.
 * If the first byte value (as an unsigned 8 bit value) is between 0 and
 * 253, it's a single-byte length. If it is 254 then a four bytes unsigned
 * integer follows (in the host byte ordering). A value of 255 is used to
 * signal the end of the hash.
 *  len字段的编码方式和ziplist有些类似，可以用1个字节或5个字节编码表示，具体如下：
 *      （1）如果随后的字符串长度小于或等于253，直接用1个字节表示其长度
 *      （2）如果随后的字符串长度超过254，则用5个字节表示，其中第一个字节值为254，接下来的4个字节才是字符串长度
 *
 * <free> is the number of free unused bytes after the string, resulting
 * from modification of values associated to a key. For instance if "foo"
 * is set to "bar", and later "foo" will be set to "hi", it will have a
 * free byte to use if the value will enlarge again later, or even in
 * order to add a key/value pair if it fits.
 *  free表示随后的value的空闲字节数。比如：假设zipmap存在"foo" => "bar"这样一个键值对，随后我们将“bar”设置为
 *  ”hi"，此时free = 1，表示value字符串后面有1个字节大小的空闲空间。
 *
 * <free> is always an unsigned 8 bit number, because if after an
 * update operation there are more than a few free bytes, the zipmap will be
 * reallocated to make sure it is as small as possible.
 *  free字段是一个占1个字节的整型数，它的值一般都比较小，如果空闲区间太大，zipmap会进行调整以使整个map尽可能小。
 *
 *  zipmap也存在一个结尾符，占用1个字节，其值为255。
 *  
 *  从上面的介绍可以看出：zipmap实质上是用一个字符串数组来依次保存key和value,查询时是依次遍列每个key-value对，直到查到为止。
 *
 * The most compact representation of the above two elements hash is actually:
 *
 * "\x02\x03foo\x03\x00bar\x05hello\x05\x00world\xff"
 *
 * Note that because keys and values are prefixed length "objects",
 * the lookup will take O(N) where N is the number of elements
 * in the zipmap and *not* the number of bytes needed to represent the zipmap.
 * This lowers the constant times considerably.
 */

#include <stdio.h>
#include <string.h>
#include "zmalloc.h"
#include "endianconv.h"

 /* 说明：为了描述方便，这里将key-value键值对的key称为key节点，value称为value节点 */

#define ZIPMAP_BIGLEN 254   // zipmap的元素个数超过253时的标识符
#define ZIPMAP_END 255      // zipmap的结尾符

/* The following defines the max value for the <free> field described in the
 * comments above, that is, the max number of trailing bytes in a value. */
/* free字段的最大值，也就是value后面的最大空闲字节数 */
#define ZIPMAP_VALUE_MAX_FREE 4

/* The following macro returns the number of bytes needed to encode the length
 * for the integer value _l, that is, 1 byte for lengths < ZIPMAP_BIGLEN and
 * 5 bytes for all the other lengths. */
/* 工具宏，用来确定len字段所占用的字节数。简单地测试第一个字节的值与254的大小关系 */
#define ZIPMAP_LEN_BYTES(_l) (((_l) < ZIPMAP_BIGLEN) ? 1 : sizeof(unsigned int)+1)

/* Create a new empty zipmap. */
/* 创建一个空的zipmap结构 */
unsigned char *zipmapNew(void) {
    // 初始化时只有2个字节,第1个字节表示zipmap保存的key-value对的个数，第2个字节为结尾符
    unsigned char *zm = zmalloc(2);

    // 当前保存的键值对个数为0
    zm[0] = 0; /* Length */
    zm[1] = ZIPMAP_END;
    return zm;
}

/* Decode the encoded length pointed by 'p' */
/* 获取len字段的数值（即随后字符串的长度），其原理很简单：查看第一个字节的数值，如果该数值小于254，则直接返回，
    否则读取接下来的4个字节内容表示的数值。 */
static unsigned int zipmapDecodeLength(unsigned char *p) {
    unsigned int len = *p;

    if (len < ZIPMAP_BIGLEN) return len;
    // 读取随后4个字节的内容
    memcpy(&len,p+1,sizeof(unsigned int));
    // 统一转化为小端模式表示
    memrev32ifbe(&len);
    return len;
}

/* Encode the length 'l' writing it in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. */
/* 将长度len编码到p指针指向的内存空间 */
static unsigned int zipmapEncodeLength(unsigned char *p, unsigned int len) {
    if (p == NULL) {
        return ZIPMAP_LEN_BYTES(len);
    } else {
        if (len < ZIPMAP_BIGLEN) {
            // 长度小于254，只需要1个字节表示
            p[0] = len;
            return 1;
        } else {
            // 长度大于等于254，第一个字节赋值为254，接下来的4歌字节才是真正的长度值
            p[0] = ZIPMAP_BIGLEN;
            memcpy(p+1,&len,sizeof(len));
            memrev32ifbe(p+1);
            return 1+sizeof(len);
        }
    }
}

/* Search for a matching key, returning a pointer to the entry inside the
 * zipmap. Returns NULL if the key is not found.
 *
 * If NULL is returned, and totlen is not NULL, it is set to the entire
 * size of the zimap, so that the calling function will be able to
 * reallocate the original zipmap to make room for more entries. */
 /* 按关键字key查找zipmap，如果totlen不为NULL，函数返回后存放zipmap占用的字节数 */
static unsigned char *zipmapLookupRaw(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned int *totlen) {
    // zipmap中第1个字节是zmlen字段，zm+1跳过第1个字节
    unsigned char *p = zm+1, *k = NULL;
    unsigned int l,llen;

    // 从前往后查找
    while(*p != ZIPMAP_END) {
        unsigned char free;

        /* Match or skip the key */
        // 确定key字符串的长度
        l = zipmapDecodeLength(p);
        // 确定保存key字符串长度所需要的字节数，也就是len字段所需要的字节数
        llen = zipmapEncodeLength(NULL,l);
        // 比较当前key与给定key是否匹配
        if (key != NULL && k == NULL && l == klen && !memcmp(p+llen,key,l)) {
            /* Only return when the user doesn't care
             * for the total length of the zipmap. */
            // 如果totlen为NULL，表示函数调用者不关心zipmap占用的字节数，此时直接返回p，否则先记录下p指针然后继续遍历
            if (totlen != NULL) {
                k = p;
            } else {
                return p;
            }
        }
        // p加上llen和l，到了value节点处
        p += llen+l;
        /* Skip the value as well */
        // 确定value字符串的长度
        l = zipmapDecodeLength(p);
        // 确定保存value字符串长度所需要的字节数，也就是len字段所需要的字节数
        p += zipmapEncodeLength(NULL,l);
        // 读出free字段的值（前面我们讲过：free只占用一个字节）
        free = p[0];
        // 跳到下一个key节点的
        p += l+1+free; /* +1 to skip the free byte */
    }
    // 到这里遍历完整个zipmap，得到其占用的字节数
    if (totlen != NULL) *totlen = (unsigned int)(p-zm)+1;
    return k;
}

/* 存储由长度为klen的key和长度为vlen的value组成的键值对所需要的字节数*/
static unsigned long zipmapRequiredLength(unsigned int klen, unsigned int vlen) {
    unsigned int l;

    l = klen+vlen+3;
    if (klen >= ZIPMAP_BIGLEN) l += 4;
    if (vlen >= ZIPMAP_BIGLEN) l += 4;
    return l;
}

/* Return the total amount used by a key (encoded length + payload) */
/* 获取key节点占用的字节数，即len字段 + key字符串长度 */
static unsigned int zipmapRawKeyLength(unsigned char *p) {
    // 获取key字符串的长度
    unsigned int l = zipmapDecodeLength(p);
    // 加上保存key字符串长度所需要的字节数
    return zipmapEncodeLength(NULL,l) + l;
}

/* Return the total amount used by a value
 * (encoded length + single byte free count + payload) */
/* 获取value节点占用的字节数，即len字段 + 1个字节free字段 + value字符串长度 + 空闲空间大小 */
static unsigned int zipmapRawValueLength(unsigned char *p) {
    // 获取value字符串的长度
    unsigned int l = zipmapDecodeLength(p);
    unsigned int used;

    // 获取保存value字符串长度所需要的字节数
    used = zipmapEncodeLength(NULL,l);
    // p[used]里面存储着空闲空间的大小
    used += p[used] + 1 + l;
    return used;
}

/* If 'p' points to a key, this function returns the total amount of
 * bytes used to store this entry (entry = key + associated value + trailing
 * free space if any). */
/* 如果p指向key节点，则该函数返回存储整个键值对所占用的字节数，包括key节点长度 + value节点长度 + 空闲字节数（如果有） */
static unsigned int zipmapRawEntryLength(unsigned char *p) {
    unsigned int l = zipmapRawKeyLength(p);
    return l + zipmapRawValueLength(p+l);
}

/* 重新调整zipmap的大小 */
static inline unsigned char *zipmapResize(unsigned char *zm, unsigned int len) {
    // 重新分配空间，注意是realloc，即在原空间重新分配
    zm = zrealloc(zm, len);
    // 设置结尾符
    zm[len-1] = ZIPMAP_END;
    return zm;
}

/* Set key to value, creating the key if it does not already exist.
 * If 'update' is not NULL, *update is set to 1 if the key was
 * already preset, otherwise to 0. */
/* 根据key设置value，如果key不存在则创建相应的键值对，参数update用来辨别更新操作和添加操作。 */
unsigned char *zipmapSet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char *val, unsigned int vlen, int *update) {
    unsigned int zmlen, offset;
    // 计算存储key和value所需要的字节数
    unsigned int freelen, reqlen = zipmapRequiredLength(klen,vlen);
    unsigned int empty, vempty;
    unsigned char *p;

    /************************************************************************
     *  下面这段代码用于在zipmap留出足够的空间来容纳新插入的键值对或新的value值，尚未写入
     ************************************************************************/
    freelen = reqlen;
    if (update) *update = 0;
    // 在zipmap中查找key，函数返回后zmlen中保存了zipmap所占用的字节数。
    p = zipmapLookupRaw(zm,key,klen,&zmlen);
    if (p == NULL) {
        /* Key not found: enlarge */
        // 如果key指定的键值对不存在，则对zipmap扩容，为容纳新的键值对准备内存空间
        // zipmapResize执行的是realloc操作
        zm = zipmapResize(zm, zmlen+reqlen);
        // 此时p指向扩容前zipmap的结尾符，将从这里添加新的键值对
        p = zm+zmlen-1;
        // 更新zipmap所占用的内存空间大小
        zmlen = zmlen+reqlen;

        /* Increase zipmap length (this is an insert) */
        // 更新zipmap中保存的键值对数量，即zmlen字段
        if (zm[0] < ZIPMAP_BIGLEN) zm[0]++;

    } else {
        /* Key found. Is there enough space for the new value? */
        /* 找到可对应的键值对，执行更新操作。这里需要考虑value节点的空间大小是否能够容纳新值 */
        /* Compute the total length: */
        if (update) *update = 1;
        // 求出旧value节点的空间大小
        freelen = zipmapRawEntryLength(p);
        if (freelen < reqlen) {
            /* Store the offset of this key within the current zipmap, so
             * it can be resized. Then, move the tail backwards so this
             * pair fits at the current position. */
             // 旧节点的空间太小，需要扩容操作，zipmapResize函数会重新分配空间，所以需要记录p指针的偏移量
            offset = p-zm;
            zm = zipmapResize(zm, zmlen-freelen+reqlen);
            p = zm+offset;

            /* The +1 in the number of bytes to be moved is caused by the
             * end-of-zipmap byte. Note: the *original* zmlen is used. */
            // 移动旧value节点以后的元素以确保有足够的空间容纳新值（ +1是将尾部结尾符一起移动）
            memmove(p+reqlen, p+freelen, zmlen-(offset+freelen+1));
            zmlen = zmlen-freelen+reqlen;
            freelen = reqlen;
        }
    }

    /* We now have a suitable block where the key/value entry can
     * be written. If there is too much free space, move the tail
     * of the zipmap a few bytes to the front and shrink the zipmap,
     * as we want zipmaps to be very space efficient. */
    // freelen表示经上步骤后流出来的空余空间大小，reqlen表示插入或更新键值对所需要的空间，两者的差就是free字段的
    // 的值，如果该值过大zipmap会自动调整。下面这段代码就是完成调整功能。
    empty = freelen-reqlen;
    if (empty >= ZIPMAP_VALUE_MAX_FREE) {
        /* First, move the tail <empty> bytes to the front, then resize
         * the zipmap to be <empty> bytes smaller. */
        offset = p-zm;
        memmove(p+reqlen, p+freelen, zmlen-(offset+freelen+1));
        zmlen -= empty;
        zm = zipmapResize(zm, zmlen);
        p = zm+offset;
        vempty = 0;
    } else {
        vempty = empty;
    }

    /******************************************
     *  下面的操作是讲key和value写入zipmap指定位置
     *******************************************/
    /* Just write the key + value and we are done. */
    /* Key: */
    // 对key的长度编码并写入zipmap中
    p += zipmapEncodeLength(p,klen);
    // 写入key字符串
    memcpy(p,key,klen);
    // 移动指针到value写入位置
    p += klen;
    /* Value: */
    // 对value的长度编码并写入zipmap中
    p += zipmapEncodeLength(p,vlen);
    // 写入free字段
    *p++ = vempty;
    // 写入value
    memcpy(p,val,vlen);
    return zm;
}

/* Remove the specified key. If 'deleted' is not NULL the pointed integer is
 * set to 0 if the key was not found, to 1 if it was found and deleted. */
/* 根据key删除指定的键值对 */
unsigned char *zipmapDel(unsigned char *zm, unsigned char *key, unsigned int klen, int *deleted) {
    unsigned int zmlen, freelen;
    // 看判断该键值对是否在zipmap中，如果不存在则直接返回
    unsigned char *p = zipmapLookupRaw(zm,key,klen,&zmlen);
    if (p) {
        // 下面三句代码执行删除操作，其实就是内存块的移动操作
        freelen = zipmapRawEntryLength(p);
        memmove(p, p+freelen, zmlen-((p-zm)+freelen+1));
        zm = zipmapResize(zm, zmlen-freelen);

        /* Decrease zipmap length */
        if (zm[0] < ZIPMAP_BIGLEN) zm[0]--;

        if (deleted) *deleted = 1;
    } else {
        if (deleted) *deleted = 0;
    }
    return zm;
}

/* Call before iterating through elements via zipmapNext() */
/* zipmapNext迭代器函数，还记得前面我们分析过zipmap第一个字节是zmlen字段吗？下面这个函数就是跳过第一个字节返回
    指向第一个键值对的首地址 */
unsigned char *zipmapRewind(unsigned char *zm) {
    return zm+1;
}

/* This function is used to iterate through all the zipmap elements.
 * In the first call the first argument is the pointer to the zipmap + 1.
 * In the next calls what zipmapNext returns is used as first argument.
 * Example:
 *
 * unsigned char *i = zipmapRewind(my_zipmap);
 * while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
 *     printf("%d bytes key at $p\n", klen, key);
 *     printf("%d bytes value at $p\n", vlen, value);
 * }
 */
/* zipmap的迭代器式遍历函数，典型用法如下：
 *
 *      unsigned char *i = zipmapRewind(my_zipmap);
 *      while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
 *          printf("%d bytes key at $p\n", klen, key);
 *          printf("%d bytes value at $p\n", vlen, value);
 *      }
 */
unsigned char *zipmapNext(unsigned char *zm, unsigned char **key, unsigned int *klen, unsigned char **value, unsigned int *vlen) {
    // 如果达到尾部，直接返回NULL
    if (zm[0] == ZIPMAP_END) return NULL;
    // 获取key
    if (key) {
        *key = zm;
        *klen = zipmapDecodeLength(zm);
        *key += ZIPMAP_LEN_BYTES(*klen);
    }
    zm += zipmapRawKeyLength(zm);
    // 获取value
    if (value) {
        // +1是为了跳过free字段，该字段占用一个字节
        *value = zm+1;
        *vlen = zipmapDecodeLength(zm);
        *value += ZIPMAP_LEN_BYTES(*vlen);
    }
    // 此时zm指向下一个键值对的首地址
    zm += zipmapRawValueLength(zm);
    return zm;
}

/* Search a key and retrieve the pointer and len of the associated value.
 * If the key is found the function returns 1, otherwise 0. */
/* 根据key值查找相应的value值，其实是对zipmapLookupRaw的包装 */
int zipmapGet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char **value, unsigned int *vlen) {
    unsigned char *p;

    if ((p = zipmapLookupRaw(zm,key,klen,NULL)) == NULL) return 0;
    p += zipmapRawKeyLength(p);
    *vlen = zipmapDecodeLength(p);
    *value = p + ZIPMAP_LEN_BYTES(*vlen) + 1;
    return 1;
}

/* Return 1 if the key exists, otherwise 0 is returned. */
/* 判断某个key是否存在 */
int zipmapExists(unsigned char *zm, unsigned char *key, unsigned int klen) {
    return zipmapLookupRaw(zm,key,klen,NULL) != NULL;
}

/* Return the number of entries inside a zipmap */
/* 返回zipmap中键值对的个数，如果zmlen字段的值小于254，值zmlen的值就是所要求得返回值，否则需要遍历整个zipmap */
unsigned int zipmapLen(unsigned char *zm) {
    unsigned int len = 0;
    if (zm[0] < ZIPMAP_BIGLEN) {
        len = zm[0];
    } else {
        unsigned char *p = zipmapRewind(zm);
        while((p = zipmapNext(p,NULL,NULL,NULL,NULL)) != NULL) len++;

        /* Re-store length if small enough */
        if (len < ZIPMAP_BIGLEN) zm[0] = len;
    }
    return len;
}

/* Return the raw size in bytes of a zipmap, so that we can serialize
 * the zipmap on disk (or everywhere is needed) just writing the returned
 * amount of bytes of the C array starting at the zipmap pointer. */
/* 获取整个zipmap占用的字节数，其实是对zipmapLookupRaw的包装 */
size_t zipmapBlobLen(unsigned char *zm) {
    unsigned int totlen;
    zipmapLookupRaw(zm,NULL,0,&totlen);
    return totlen;
}

#ifdef ZIPMAP_TEST_MAIN
/* 格式化输出函数 */
void zipmapRepr(unsigned char *p) {
    unsigned int l;

    printf("{status %u}",*p++);
    while(1) {
        if (p[0] == ZIPMAP_END) {
            printf("{end}");
            break;
        } else {
            unsigned char e;

            l = zipmapDecodeLength(p);
            printf("{key %u}",l);
            p += zipmapEncodeLength(NULL,l);
            if (l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l;

            l = zipmapDecodeLength(p);
            printf("{value %u}",l);
            p += zipmapEncodeLength(NULL,l);
            e = *p++;
            if (l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l+e;
            if (e) {
                printf("[");
                while(e--) printf(".");
                printf("]");
            }
        }
    }
    printf("\n");
}

/* 下面是一些测试代码 */
int main(void) {
    unsigned char *zm;

    zm = zipmapNew();

    zm = zipmapSet(zm,(unsigned char*) "name",4, (unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "surname",7, (unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "age",3, (unsigned char*) "foo",3,NULL);
    zipmapRepr(zm);

    zm = zipmapSet(zm,(unsigned char*) "hello",5, (unsigned char*) "world!",6,NULL);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "bar",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "!",1,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "12345",5,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm,(unsigned char*) "new",3, (unsigned char*) "xx",2,NULL);
    zm = zipmapSet(zm,(unsigned char*) "noval",5, (unsigned char*) "",0,NULL);
    zipmapRepr(zm);
    zm = zipmapDel(zm,(unsigned char*) "new",3,NULL);
    zipmapRepr(zm);

    printf("\nLook up large key:\n");
    {
        unsigned char buf[512];
        unsigned char *value;
        unsigned int vlen, i;
        for (i = 0; i < 512; i++) buf[i] = 'a';

        zm = zipmapSet(zm,buf,512,(unsigned char*) "long",4,NULL);
        if (zipmapGet(zm,buf,512,&value,&vlen)) {
            printf("  <long key> is associated to the %d bytes value: %.*s\n",
                vlen, vlen, value);
        }
    }

    printf("\nPerform a direct lookup:\n");
    {
        unsigned char *value;
        unsigned int vlen;

        if (zipmapGet(zm,(unsigned char*) "foo",3,&value,&vlen)) {
            printf("  foo is associated to the %d bytes value: %.*s\n",
                vlen, vlen, value);
        }
    }
    printf("\nIterate through elements:\n");
    {
        unsigned char *i = zipmapRewind(zm);
        unsigned char *key, *value;
        unsigned int klen, vlen;

        while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
            printf("  %d:%.*s => %d:%.*s\n", klen, klen, key, vlen, vlen, value);
        }
    }
    return 0;
}
#endif
