/* rio.c is a simple stream-oriented I/O abstraction that provides an interface
 * to write code that can consume/produce data using different concrete input
 * and output devices. For instance the same rdb.c code using the rio
 * abstraction can be used to read and write the RDB format using in-memory
 * buffers or files.
 *
 *  rio.c是一个简答的面向流的IO层抽象，它提供了一个可以处理不同输入输出设备的接口来编写代码。
 *  比如rdb.c使用rio来读写内存或文件中的RDB格式数据。
 *
 * A rio object provides the following methods:
 *  read: read from stream.
 *  write: write to stream.
 *  tell: get the current offset.
 *
 *  一个rio对象提供了下列方法：
 *  read：从流中读取数据
 *  write：将数据写入流中
 *  tell：获取当前偏移量
 *
 * It is also possible to set a 'checksum' method that is used by rio.c in order
 * to compute a checksum of the data written or read, or to query the rio object
 * for the current checksum.
 *
 *  它还还提供了checksum函数来计算写入或读取内容的校验和，或者通过当前的校验和来查询rio对象。
 *
 * ----------------------------------------------------------------------------
 *
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


#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h"
#include "util.h"
#include "crc64.h"
#include "config.h"
#include "redis.h"

/* ------------------------- Buffer I/O implementation ----------------------- */
/* 缓冲io实现 */


/* Returns 1 or 0 for success/failure. */
/* 将buf中指定长度len的内容追加到rio对象的缓冲区中，操作成功返回1，否则返回0。*/
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {
    // 调用sdscatlen实现append操作
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    // 更新长度信息
    r->io.buffer.pos += len;
    return 1;
}

/* Returns 1 or 0 for success/failure. */
/* 从rio对象的缓冲区中读取长度为len的内容到buf中，操作成功返回1，否则返回0。*/
static size_t rioBufferRead(rio *r, void *buf, size_t len) {
    // 如果rio对象的缓冲区中内容的长度小于len，读取失败
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* not enough buffer to return len bytes. */
    // 将缓冲区中的内容复制到buf
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    // 更新偏移量
    r->io.buffer.pos += len;
    return 1;
}

/* Returns read/write position in buffer. */
/*  返回rio对象缓冲区的偏移量 */
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
/*  该函数什么事也没有做，直接返回1。*/
static int rioBufferFlush(rio *r) {
    // REDIS_NOTUSED定义在redis.h文件中：#define REDIS_NOTUSED(V) ((void) V)
    REDIS_NOTUSED(r);
    return 1; /* Nothing to do, our write just appends to the buffer. */
}

/* 根据上面的方法定义的流为内存时使用的buffer rio对象 */
static const rio rioBufferIO = {
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    rioBufferFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/* 初始化buffer io对象 */
void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

/* --------------------- Stdio file pointer implementation ------------------- */
/* 文件io实现 */


/* Returns 1 or 0 for success/failure. */
/* 将buf中长度为len的内容写入文件中，返回写入的字节数。*/
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    size_t retval;

    // 调用标准库函数fwrite完成写入操作
    retval = fwrite(buf,len,1,r->io.file.fp);
    // 记录写入的字节数
    r->io.file.buffered += len;

    // 检查写入的字节数，看是否需要执行sync操作
    if (r->io.file.autosync &&
        r->io.file.buffered >= r->io.file.autosync)
    {
        // 冲洗流中的数据
        fflush(r->io.file.fp);
        // 执行一次fsync操作
        aof_fsync(fileno(r->io.file.fp));
        // buffered表示最后一个fsync后写入的字节数，重新置0
        r->io.file.buffered = 0;
    }
    return retval;
}

/* Returns 1 or 0 for success/failure. */
/* 从文件中读取长度为len的内容到buf中，返回实际读到的字节数。*/
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* Returns read/write position in file. */
/* 返回文件当前偏移量 */
static off_t rioFileTell(rio *r) {
    return ftello(r->io.file.fp);
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
/* 冲洗流中信息，如果操作成功返回1，否则返回0。*/
static int rioFileFlush(rio *r) {
    return (fflush(r->io.file.fp) == 0) ? 1 : 0;
}

/* 根据上面的方法定义的流为文件时使用的file rio对象 */
static const rio rioFileIO = {
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    rioFileFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/* 初始化file rio对象 */
void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
}

/* ------------------- File descriptors set implementation ------------------- */
/* 文件描述符集合实现，用于控制多个文件描述符 */


/* Returns 1 or 0 for success/failure.
 * The function returns success as long as we are able to correctly write
 * to at least one file descriptor.
 *
 * When buf is NULL adn len is 0, the function performs a flush operation
 * if there is some pending buffer, so this function is also used in order
 * to implement rioFdsetFlush(). */
/*  该函数执行成功时返回1，失败时返回0。
    该函数只有在成功往至少一个文件描述符中写入数据后才返回1。
    如果参数buf为NULL且len为0，该函数相当于flush操作。*/
static size_t rioFdsetWrite(rio *r, const void *buf, size_t len) {
    ssize_t retval;
    int j;
    unsigned char *p = (unsigned char*) buf;
    // 如果参数buf为NULL且len为0，该函数相当于flush操作
    int doflush = (buf == NULL && len == 0);

    /* To start we always append to our buffer. If it gets larger than
     * a given size, we actually write to the sockets. */
    // 将buf中的内容追加到r->io.fdset.buf缓冲区中。
    if (len) {
        r->io.fdset.buf = sdscatlen(r->io.fdset.buf,buf,len);
        len = 0; /* Prevent entering the while belove if we don't flush. */
        if (sdslen(r->io.fdset.buf) > REDIS_IOBUF_LEN) doflush = 1;
    }

    if (doflush) {
        p = (unsigned char*) r->io.fdset.buf;
        len = sdslen(r->io.fdset.buf);
    }

    /* Write in little chunchs so that when there are big writes we
     * parallelize while the kernel is sending data in background to
     * the TCP socket. */
    while(len) {
        size_t count = len < 1024 ? len : 1024;
        int broken = 0;
        for (j = 0; j < r->io.fdset.numfds; j++) {
            // 跳过出错的fd
            if (r->io.fdset.state[j] != 0) {
                /* Skip FDs alraedy in error. */
                // 记录出错的fd数量
                broken++;
                continue;
            }

            /* Make sure to write 'count' bytes to the socket regardless
             * of short writes. */
            // 记录写入的字节数
            size_t nwritten = 0;
            // 往当前fd中写入count字节的数据
            while(nwritten != count) {
                // 真正执行写入操作
                retval = write(r->io.fdset.fds[j],p+nwritten,count-nwritten);
                if (retval <= 0) {
                    /* With blocking sockets, which is the sole user of this
                     * rio target, EWOULDBLOCK is returned only because of
                     * the SO_SNDTIMEO socket option, so we translate the error
                     * into one more recognizable by the user. */
                    if (retval == -1 && errno == EWOULDBLOCK) errno = ETIMEDOUT;
                    break;
                }
                nwritten += retval;
            }

            if (nwritten != count) {
                /* Mark this FD as broken. */
                r->io.fdset.state[j] = errno;
                if (r->io.fdset.state[j] == 0) r->io.fdset.state[j] = EIO;
            }
        }
        // 如果所有的fd都出错，返回
        if (broken == r->io.fdset.numfds) return 0; /* All the FDs in error. */
        // 更新偏移量和剩余字节数
        p += count;
        len -= count;
        r->io.fdset.pos += count;
    }

    if (doflush) sdsclear(r->io.fdset.buf);
    return 1;
}

/* Returns 1 or 0 for success/failure. */
/* fd set对象不支持读操作，直接报错 */
static size_t rioFdsetRead(rio *r, void *buf, size_t len) {
    REDIS_NOTUSED(r);
    REDIS_NOTUSED(buf);
    REDIS_NOTUSED(len);
    return 0; /* Error, this target does not support reading. */
}

/* Returns read/write position in file. */
/* 获取偏移量 */
static off_t rioFdsetTell(rio *r) {
    return r->io.fdset.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
/* flush操作 */
static int rioFdsetFlush(rio *r) {
    /* Our flush is implemented by the write method, that recognizes a
     * buffer set to NULL with a count of zero as a flush request. */
    /*  这里的flush操作通过rioFdsetWrite方法实现 */
    return rioFdsetWrite(r,NULL,0);
}

/* 根据上面的方法定义的流为socket fd时使用的fd set rio对象 */
static const rio rioFdsetIO = {
    rioFdsetRead,
    rioFdsetWrite,
    rioFdsetTell,
    rioFdsetFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/* 初始化fd set rio对象 */
void rioInitWithFdset(rio *r, int *fds, int numfds) {
    int j;

    *r = rioFdsetIO;
    r->io.fdset.fds = zmalloc(sizeof(int)*numfds);
    r->io.fdset.state = zmalloc(sizeof(int)*numfds);
    memcpy(r->io.fdset.fds,fds,sizeof(int)*numfds);
    for (j = 0; j < numfds; j++) r->io.fdset.state[j] = 0;
    r->io.fdset.numfds = numfds;
    r->io.fdset.pos = 0;
    r->io.fdset.buf = sdsempty();
}

/* 释放fd set rio对象 */
void rioFreeFdset(rio *r) {
    zfree(r->io.fdset.fds);
    zfree(r->io.fdset.state);
    sdsfree(r->io.fdset.buf);
}

/* ---------------------------- Generic functions ---------------------------- */

/* This function can be installed both in memory and file streams when checksum
 * computation is needed. */
/* 计算文件校验和 */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* Set the file-based rio object to auto-fsync every 'bytes' file written.
 * By default this is set to zero that means no automatic file sync is
 * performed.
 *
 * This feature is useful in a few contexts since when we rely on OS write
 * buffers sometimes the OS buffers way too much, resulting in too many
 * disk I/O concentrated in very little time. When we fsync in an explicit
 * way instead the I/O pressure is more distributed across time. */
/*  该函数用来设置文件rio对象的autosync字段，每次写入bytes参数指定字节的数据后执行一次fsync操作。
    默认情况下，bytes被设置为0，意味着不执行fsync操作。
    这个功能在某些情景下很有用。比如，如果我们一次性地使用write函数写入过多的内容会导致瞬时间磁盘IO次数过多。
    通过显式、间隔性地调用fsync可以将IO操作的压力分担到多次的fsync调用中。*/
void rioSetAutoSync(rio *r, off_t bytes) {
    redisAssert(r->read == rioFileIO.read);
    r->io.file.autosync = bytes;
}

/* --------------------------- Higher level interface --------------------------
 *  高层接口
 * The following higher level functions use lower level rio.c functions to help
 * generating the Redis protocol for the Append Only File. 
 *
 *  下面这些高层函数使用前面的底层rio.c函数来辅助生成AOF文件协议。
 */

/* Write multi bulk count in the format: "*<count>\r\n". */
/*  以"*<count>\r\n"的形式将count以字符串的格式写入rio对象中，返回写入的字节数。*/
size_t rioWriteBulkCount(rio *r, char prefix, int count) {
    char cbuf[128];
    int clen;

    // 构造"*<count>\r\n"格式的字符串
    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';
    // 真正执行写入操作 
    if (rioWrite(r,cbuf,clen) == 0) return 0;
    // 返回成功写入字节数
    return clen;
}

/* Write binary-safe string in the format: "$<count>\r\n<payload>\r\n". */
/*  以"$<count>\r\n<payload>\r\n"格式往rio对象中写入二进制安全字符串。*/
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    // 写入“$<count>\r\n”
    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;
    // 写入“<payload>”
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;
    // 写入“\r\n"
    if (rioWrite(r,"\r\n",2) == 0) return 0;
    // 返回成功写入的字节数
    return nwritten+len+2;
}

/* Write a long long value in format: "$<count>\r\n<payload>\r\n". */
/*  以"$<count>\r\n<payload>\r\n"的格式往rio对象中写入long long类型的值。*/
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    llen = ll2string(lbuf,sizeof(lbuf),l);
    // 最终写入"$llen\r\nlbuf\r\n"
    return rioWriteBulkString(r,lbuf,llen);
}

/* Write a double value in the format: "$<count>\r\n<payload>\r\n" */
/*  以"$<count>\r\n<payload>\r\n"的格式往rio对象中写入double类型的值。*/
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;

    // 最终写入"$dlen\r\ndbuf\r\n"
    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    return rioWriteBulkString(r,dbuf,dlen);
}
