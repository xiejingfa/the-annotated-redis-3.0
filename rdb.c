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
#include "lzf.h"    /* LZF compression library */
#include "zipmap.h"
#include "endianconv.h"

#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>

/* ！！！！ 下面使用的rio类型定义在rio.h文件中 ！！！！ */

/* 将p中长度为len的内容写入rdb中，写入成功返回写入的字节数，否则返回-1。 */
static int rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

/*  将一个字节的type类型写入rdb中 */
int rdbSaveType(rio *rdb, unsigned char type) {
    return rdbWriteRaw(rdb,&type,1);
}

/* Load a "type" in RDB format, that is a one byte unsigned integer.
 * This function is not only used to load object types, but also special
 * "types" like the end-of-file type, the EXPIRE type, and so forth. */
/*  从rdb中加载type类型，该字段是一个字节的无符号整型。
    该函数既可以用来加载对象类型，也可以用来加载特殊的类型标识。
    函数的返回值为加载的type值。*/
int rdbLoadType(rio *rdb) {
    unsigned char type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}

/*  从rdb中加载以秒为单位的过期时间，用time_t类型表示。*/
time_t rdbLoadTime(rio *rdb) {
    int32_t t32;
    if (rioRead(rdb,&t32,4) == 0) return -1;
    return (time_t)t32;
}

/*  将long long类型的、以毫秒为单位的过期时间写入rdb中。写入成功返回写入的字节数，否则返回-1。*/
int rdbSaveMillisecondTime(rio *rdb, long long t) {
    int64_t t64 = (int64_t) t;
    return rdbWriteRaw(rdb,&t64,8);
}

/*  从rdb中加载以毫秒为单位的过期时间，用long long类型表示。*/
long long rdbLoadMillisecondTime(rio *rdb) {
    int64_t t64;
    if (rioRead(rdb,&t64,8) == 0) return -1;
    return (long long)t64;
}

/* Saves an encoded length. The first two bits in the first byte are used to
 * hold the encoding type. See the REDIS_RDB_* definitions for more information
 * on the types of encoding. */
/*  对长度len进行编码后写入rdb中。第一个字节的前两个bit用来保存编码类型，关于编码方式可以参看
    rdb.h文件中REDIS_RDB_*了解。写入成功后返回写入的字节数。*/
int rdbSaveLen(rio *rdb, uint32_t len) {
    unsigned char buf[2];
    size_t nwritten;

    // REDIS_RDB_6BITLEN编码
    if (len < (1<<6)) {
        /* Save a 6 bit len */
        buf[0] = (len&0xFF)|(REDIS_RDB_6BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        nwritten = 1;
    } 
    // REDIS_RDB_14BITLEN编码
    else if (len < (1<<14)) {
        /* Save a 14 bit len */
        buf[0] = ((len>>8)&0xFF)|(REDIS_RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;
    } 
    // REDIS_RDB_32BITLEN编码
    else {
        /* Save a 32 bit len */
        buf[0] = (REDIS_RDB_32BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonl(len);
        if (rdbWriteRaw(rdb,&len,4) == -1) return -1;
        nwritten = 1+4;
    }
    return nwritten;
}

/* Load an encoded length. The "isencoded" argument is set to 1 if the length
 * is not actually a length but an "encoding type". See the REDIS_RDB_ENC_*
 * definitions in rdb.h for more information. */
/*  从rdb中读入一个被编码的长度信息。
    如果该长度信息并不是一个整型值，而是一个编码类型，则参数isencoded被设为1。
    可以查看rdb.h文件中的REDIS_RDB_ENC_*了解更多信息。*/
uint32_t rdbLoadLen(rio *rdb, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;
    // 从rdb中读入长度信息，这个值可能被编码也可能没有被编码，由前两个bit内容决定
    if (rioRead(rdb,buf,1) == 0) return REDIS_RDB_LENERR;
    // 取出前两个bit
    type = (buf[0]&0xC0)>>6;
    if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) *isencoded = 1;
        return buf[0]&0x3F;
    } 
    // REDIS_RDB_6BITLEN编码
    else if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len. */
        return buf[0]&0x3F;
    } 
    // REDIS_RDB_14BITLEN编码
    else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (rioRead(rdb,buf+1,1) == 0) return REDIS_RDB_LENERR;
        return ((buf[0]&0x3F)<<8)|buf[1];
    } 
    // REDIS_RDB_32BITLEN编码
    else {
        /* Read a 32 bit len. */
        if (rioRead(rdb,&len,4) == 0) return REDIS_RDB_LENERR;
        return ntohl(len);
    }
}

/* Encodes the "value" argument as integer when it fits in the supported ranges
 * for encoded types. If the function successfully encodes the integer, the
 * representation is stored in the buffer pointer to by "enc" and the string
 * length is returned. Otherwise 0 is returned. */
/*  如果参数value在编码支持的范围内（编码方式见rdb.h头文件），尝试对其进行特殊的整型编码。
    如果编码成功，将编码后的值保存在参数enc指定的缓冲区中并返回其长度。
    如果编码失败则返回0。 */
int rdbEncodeInteger(long long value, unsigned char *enc) {
    //  REDIS_RDB_ENC_INT8编码
    if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } 
    //  REDIS_RDB_ENC_INT16编码
    else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } 
    // REDIS_RDB_ENC_INT32编码
    else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

/* Loads an integer-encoded object with the specified encoding type "enctype".
 * If the "encode" argument is set the function may return an integer-encoded
 * string object, otherwise it always returns a raw string object. */
/*  从RDB文件中载入参数enctype指定编码方式的整型对象。
    如果参数encode被设置，函数可能返回一个整型编码的字符串对象，否则函数总是返回未编码的字符串对象。*/
robj *rdbLoadIntegerObject(rio *rdb, int enctype, int encode) {
    unsigned char enc[4];
    long long val;

    //  REDIS_RDB_ENC_INT8编码
    if (enctype == REDIS_RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return NULL;
        val = (signed char)enc[0];
    } 
    // REDIS_RDB_ENC_INT16编码
    else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } 
    // REDIS_RDB_ENC_INT32编码
    else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        redisPanic("Unknown RDB integer encoding type");
    }
    if (encode)
        // 整型编码的字符串
        return createStringObjectFromLongLong(val);
    else
        // 未编码的字符串
        return createObject(REDIS_STRING,sdsfromlonglong(val));
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space */
/*  类似“2391”、“-100”这种形式的字符串对象可以编码为8位、16位、32位的带符号整型数以节省空间。
    下面这个函数就是尝试将字符串对象编码为整型数，如果编码成功则返回保存整型数值需要的字节数，否则返回0。*/
int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc) {
    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    // 判断是否可以将字符串对象s转换为整型数值
    value = strtoll(s, &endptr, 10);
    // 转换失败，返回0
    if (endptr[0] != '\0') return 0;
    // 将转换后的整型转换为字符串对象
    ll2string(buf,32,value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    // 如果装换后的整数值不能还远回原来的字符串，则转换失败，返回0
    if (strlen(buf) != len || memcmp(buf,s,len)) return 0;

    // 经过上面的检查后发现可以转换，则对转换后得到的整型数值进行编码
    return rdbEncodeInteger(value,enc);
}

/*  使用lzf算法对参数s表示的字符串进行压缩后再写入RDB文件中。
    该函数在操作成功时返回写入RDB文件中的字节数，如果内存不足或压缩失败返回0，如果写入失败返回-1。*/
int rdbSaveLzfStringObject(rio *rdb, unsigned char *s, size_t len) {
    size_t comprlen, outlen;
    unsigned char byte;
    int n, nwritten = 0;
    void *out;

    /* We require at least four bytes compression for this to be worth it */
    // 字符串s至少超过4个字节才值得压缩
    if (len <= 4) return 0;
    outlen = len-4;
    // 内存不足，返回0
    if ((out = zmalloc(outlen+1)) == NULL) return 0;
    // 使用lzf算法进行字符串压缩
    comprlen = lzf_compress(s, len, out, outlen);
    // 压缩失败，释放空间后返回0
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }
    /* Data compressed! Let's save it on disk */
    /*  经过上面的操作得到压缩后的字符串，现在讲其保存在RDB文件中。*/

    // 写入类型信息，指明这是一个使用lzf压缩后得到的字符串
    byte = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_LZF;
    if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) goto writeerr;
    // 记录写入的字节数
    nwritten += n;

    // 写入压缩后的字符串长度
    if ((n = rdbSaveLen(rdb,comprlen)) == -1) goto writeerr;
    // 记录写入的字节数
    nwritten += n;

    // 写入字符串压缩前的原始长度
    if ((n = rdbSaveLen(rdb,len)) == -1) goto writeerr;
    // 记录写入的字节数
    nwritten += n;

    // 写入压缩后的字符串
    if ((n = rdbWriteRaw(rdb,out,comprlen)) == -1) goto writeerr;
    // 记录写入的字节数
    nwritten += n;

    zfree(out);
    // 返回写入的字节数
    return nwritten;

writeerr:
    zfree(out);
    return -1;
}

/*  从RDB中加载被压缩的字符串，解析返回原始字符串对象。*/
robj *rdbLoadLzfStringObject(rio *rdb) {
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    // 读取压缩后的字符串长度
    if ((clen = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
    // 读物字符串未压缩前的长度
    if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
    // 分配空间保存压缩后的字符串
    if ((c = zmalloc(clen)) == NULL) goto err;
    if ((val = sdsnewlen(NULL,len)) == NULL) goto err;
    // 读取压缩后的字符串信息
    if (rioRead(rdb,c,clen) == 0) goto err;
    // 解压缩得到原始字符串
    if (lzf_decompress(c,clen,val,len) == 0) goto err;
    zfree(c);
    // 创建字符串对象并返回之
    return createObject(REDIS_STRING,val);
err:
    // 操作失败后的清理操作
    zfree(c);
    sdsfree(val);
    return NULL;
}

/* Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form */
/*  以[len][data]的形式将字符串对象写入RDB中。如果该对象是字符串形式表示的整型数，则尝试用特殊的形式保存它。
    操作成功后该函数返回保存字符串所需的字节数。*/
int rdbSaveRawString(rio *rdb, unsigned char *s, size_t len) {
    int enclen;
    int n, nwritten = 0;

    /* Try integer encoding */
    // 尝试进行整型编码
    if (len <= 11) {
        unsigned char buf[5];
        // rdbTryIntegerEncoding函数负责进行整型编码，如果操作成功返回值 > 0
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
            // 整型编码成功，写入RDB中
            if (rdbWriteRaw(rdb,buf,enclen) == -1) return -1;
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    // 如果服务器开启了lzf压缩功能并且待写入字符串长度超过20字节，则先进行lzf压缩后再写入RDB中
    if (server.rdb_compression && len > 20) {
        n = rdbSaveLzfStringObject(rdb,s,len);
        if (n == -1) return -1;
        if (n > 0) return n;
        /* Return value of 0 means data can't be compressed, save the old way */
        //  rdbSaveLzfStringObject的返回值为0表明无法压缩，程序继续往下运行
    }

    /* Store verbatim */
    // 经过上面的尝试，判断出输入字符串s既不能进行整型编码，也不能进行lzf压缩，直接写入RDB中
    // 先写入字符串长度
    if ((n = rdbSaveLen(rdb,len)) == -1) return -1;
    // 记录写入字节数
    nwritten += n;
    // 写入原始字符串
    if (len > 0) {
        if (rdbWriteRaw(rdb,s,len) == -1) return -1;
        // 记录写入字节数
        nwritten += len;
    }
    return nwritten;
}

/* Save a long long value as either an encoded string or a string. */
/*  将long long类型的数值转换为一个编码字符串或一个普通字符串再写入RDB中。
    该函数操作成功后返回写入的字节数，否则返回-1。*/
int rdbSaveLongLongAsStringObject(rio *rdb, long long value) {
    unsigned char buf[32];
    int n, nwritten = 0;
    // 尝试进行整型编码以节省空间
    int enclen = rdbEncodeInteger(value,buf);
    // 整型编码成功，写入RDB中
    if (enclen > 0) {
        return rdbWriteRaw(rdb,buf,enclen);
    } 
    // 整型编码失败，则将该数值转换为普通字符串来保存
    else {
        /* Encode as string */
        // 将参数value转换为普通字符串表示
        enclen = ll2string((char*)buf,32,value);
        redisAssert(enclen < 32);
        // 写入字符串长度
        if ((n = rdbSaveLen(rdb,enclen)) == -1) return -1;
        nwritten += n;
        // 写入字符串本身
        if ((n = rdbWriteRaw(rdb,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

/* Like rdbSaveStringObjectRaw() but handle encoded objects */
/*  将给定的字符串对象obj写入到RDB中。*/
int rdbSaveStringObject(rio *rdb, robj *obj) {
    /* Avoid to decode the object, then encode it again, if the
     * object is already integer encoded. */
    // 如果该对象已经是REDIS_ENCODING_INT编码，直接写入
    if (obj->encoding == REDIS_ENCODING_INT) {
        return rdbSaveLongLongAsStringObject(rdb,(long)obj->ptr);
    } 
    // 处理字符串编码的情况
    else {
        redisAssertWithInfo(NULL,obj,sdsEncodedObject(obj));
        return rdbSaveRawString(rdb,obj->ptr,sdslen(obj->ptr));
    }
}

/*  底层函数：从RDB中读取一个字符串对象并返回。参数encode不为0时指明所使用的编码方式。*/
robj *rdbGenericLoadStringObject(rio *rdb, int encode) {
    int isencoded;
    uint32_t len;
    robj *o;

    // 读取长度信息
    len = rdbLoadLen(rdb,&isencoded);
    if (isencoded) {
        switch(len) {
        //  整型编码
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return rdbLoadIntegerObject(rdb,len,encode);

        // 使用lzf算法压缩后的字符串
        case REDIS_RDB_ENC_LZF:
            return rdbLoadLzfStringObject(rdb);
        default:
            redisPanic("Unknown RDB encoding type");
        }
    }

    // 下面的代码处理非整型编码和非lzf压缩的情况
    if (len == REDIS_RDB_LENERR) return NULL;
    o = encode ? createStringObject(NULL,len) :
                 createRawStringObject(NULL,len);
    // 直接读取原始字符串
    if (len && rioRead(rdb,o->ptr,len) == 0) {
        decrRefCount(o);
        return NULL;
    }
    return o;
}

robj *rdbLoadStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,0);
}

robj *rdbLoadEncodedStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,1);
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
/*  以字符串形式保存一个double数值，该字符串的前缀是一个8bit的无符号整型数，用以指明字符串double的长度信息。
    其中有以下特殊值：
    253：表示输入不是一个数值
    254：表示输入的是正无穷
    255：表示输入的是负无穷  */
int rdbSaveDoubleValue(rio *rdb, double val) {
    unsigned char buf[128];
    int len;

    // not a number 不是一个数
    if (isnan(val)) {
        buf[0] = 253;
        len = 1;
    } 
    // 正无穷 or 负无穷
    else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf)-1,(long long)val);
        else
#endif
            // 转换为字符串表示，写入的起始位置为buf[1]，buf[0]为长度信息
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    // 将字符串写入RDB中
    return rdbWriteRaw(rdb,buf,len);
}

/* For information about double serialization check rdbSaveDoubleValue() */
/*  从RDB中读取字符串表示的double数值并保存在指针val中。*/
int rdbLoadDoubleValue(rio *rdb, double *val) {
    char buf[256];
    unsigned char len;

    // 读取字符串长度
    if (rioRead(rdb,&len,1) == 0) return -1;
    
    switch(len) {

    // 读取特殊值：不是数 or 正无穷 or 负无穷
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;

    // 读取原始字符串
    default:
        if (rioRead(rdb,buf,len) == 0) return -1;
        buf[len] = '\0';
        // 将字符串转换为double数值
        sscanf(buf, "%lg", val);
        return 0;
    }
}

/* Save the object type of object "o". */
/*  将对象o的类型信息写入RDB中，底层调用rdbSaveType函数实现。
    操作成功返回写入的字节数，操作失败返回-1。*/
int rdbSaveObjectType(rio *rdb, robj *o) {
    switch (o->type) {
    case REDIS_STRING:
        // REDIS_RDB_TYPE_STRING编码
        return rdbSaveType(rdb,REDIS_RDB_TYPE_STRING);
    case REDIS_LIST:
        if (o->encoding == REDIS_ENCODING_ZIPLIST)
            // REDIS_ENCODING_ZIPLIST编码的list
            return rdbSaveType(rdb,REDIS_RDB_TYPE_LIST_ZIPLIST);
        else if (o->encoding == REDIS_ENCODING_LINKEDLIST)
            // REDIS_ENCODING_LINKEDLIST编码的list
            return rdbSaveType(rdb,REDIS_RDB_TYPE_LIST);
        else
            redisPanic("Unknown list encoding");
    case REDIS_SET:
        if (o->encoding == REDIS_ENCODING_INTSET)
            //  REDIS_ENCODING_INTSET编码的set
            return rdbSaveType(rdb,REDIS_RDB_TYPE_SET_INTSET);
        else if (o->encoding == REDIS_ENCODING_HT)
            // REDIS_ENCODING_HT编码的set
            return rdbSaveType(rdb,REDIS_RDB_TYPE_SET);
        else
            redisPanic("Unknown set encoding");
    case REDIS_ZSET:
        if (o->encoding == REDIS_ENCODING_ZIPLIST)
            // REDIS_ENCODING_ZIPLIST编码的zset
            return rdbSaveType(rdb,REDIS_RDB_TYPE_ZSET_ZIPLIST);
        else if (o->encoding == REDIS_ENCODING_SKIPLIST)
            //  REDIS_ENCODING_SKIPLIST编码的zset
            return rdbSaveType(rdb,REDIS_RDB_TYPE_ZSET);
        else
            redisPanic("Unknown sorted set encoding");
    case REDIS_HASH:
        if (o->encoding == REDIS_ENCODING_ZIPLIST)
            // REDIS_ENCODING_ZIPLIST编码的hash
            return rdbSaveType(rdb,REDIS_RDB_TYPE_HASH_ZIPLIST);
        else if (o->encoding == REDIS_ENCODING_HT)
            //  REDIS_ENCODING_HT编码的hash
            return rdbSaveType(rdb,REDIS_RDB_TYPE_HASH);
        else
            redisPanic("Unknown hash encoding");
    default:
        redisPanic("Unknown object type");
    }
    return -1; /* avoid warning */
}

/* Use rdbLoadType() to load a TYPE in RDB format, but returns -1 if the
 * type is not specifically a valid Object Type. */
/*  该函数使用rdbLoadType函数从RDB中读取类型信息并返回，如果该类型并不是一个合法的
    Redis对象类型则返回-1。*/
int rdbLoadObjectType(rio *rdb) {
    int type;
    if ((type = rdbLoadType(rdb)) == -1) return -1;
    if (!rdbIsObjectType(type)) return -1;
    return type;
}

/* Save a Redis object. Returns -1 on error, number of bytes written on success. */
/*  将给定的Redis对象o写入RDB中。
    该函数操作成功返回写入的字节数，操作失败返回-1。*/
int rdbSaveObject(rio *rdb, robj *o) {
    int n, nwritten = 0;

    // 处理REDIS_STRING类型对象
    if (o->type == REDIS_STRING) {
        /* Save a string value */
        // 保存字符串值
        if ((n = rdbSaveStringObject(rdb,o)) == -1) return -1;
        // 记录写入字节数
        nwritten += n;
    } 
    // 处理REDIS_LIST类型对象
    else if (o->type == REDIS_LIST) {
        /* Save a list value */
        // 处理REDIS_ENCODING_ZIPLIST编码的list
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            // 获取ziplist占用的空间大小
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            // ziplist本身就是一个字符数组，这里以字符串的形式保存整个ziplist
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } 
        // 处理REDIS_ENCODING_LINKEDLIST编码的list
        else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
            list *list = o->ptr;
            listIter li;
            listNode *ln;

            // 写入长度信息（节点个数）
            if ((n = rdbSaveLen(rdb,listLength(list))) == -1) return -1;
            nwritten += n;

            listRewind(list,&li);
            // 遍历list中的每一项
            while((ln = listNext(&li))) {
                // 获取当前节点中的保存的数据内容
                robj *eleobj = listNodeValue(ln);
                // 以字符串的形式保存当前节点的内容
                if ((n = rdbSaveStringObject(rdb,eleobj)) == -1) return -1;
                nwritten += n;
            }
        } else {
            redisPanic("Unknown list encoding");
        }
    } 
    // 处理REDIS_SET类型对象
    else if (o->type == REDIS_SET) {
        /* Save a set value */
        // 处理REDIS_ENCODING_HT编码的set
        if (o->encoding == REDIS_ENCODING_HT) {
            dict *set = o->ptr;
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            // 写入长度信息
            if ((n = rdbSaveLen(rdb,dictSize(set))) == -1) return -1;
            nwritten += n;

            // 遍历字典dict的每个成员
            while((de = dictNext(di)) != NULL) {
                // 获取当前节点的key值
                robj *eleobj = dictGetKey(de);
                // 以字符串的形式保存当前节点的key
                if ((n = rdbSaveStringObject(rdb,eleobj)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
        } 
        // 处理REDIS_ENCODING_INTSET编码的set
        else if (o->encoding == REDIS_ENCODING_INTSET) {
            // 计算inset所占用空间大小
            size_t l = intsetBlobLen((intset*)o->ptr);

            // inset本身是一个字符数组，这里以字符串的形式保存整个inset
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } else {
            redisPanic("Unknown set encoding");
        }
    } 
    // 处理REDIS_ZSET类型对象
    else if (o->type == REDIS_ZSET) {
        /* Save a sorted set value */
        // 处理REDIS_ENCODING_ZIPLIST编码的zset
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            // 计算ziplist所占用空间大小
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            // ziplist本身是一个字符数组，这里以字符串的形式保存整个ziplist
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } 
        //  处理REDIS_ENCODING_SKIPLIST编码的zset
        else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;

            // 保存字典dict的节点个数
            if ((n = rdbSaveLen(rdb,dictSize(zs->dict))) == -1) return -1;
            nwritten += n;

            // 遍历字典dict的每一个节点
            while((de = dictNext(di)) != NULL) {
                // 获取当前节点（键值对）的key值
                robj *eleobj = dictGetKey(de);
                // 获取分值score值
                double *score = dictGetVal(de);

                // 以字符串的形式保存key值
                if ((n = rdbSaveStringObject(rdb,eleobj)) == -1) return -1;
                nwritten += n;
                // 保存分值score
                if ((n = rdbSaveDoubleValue(rdb,*score)) == -1) return -1;
                nwritten += n;
            }
            // 释放迭代器
            dictReleaseIterator(di);
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } 
    // 处理REDIS_HASH类型对象
    else if (o->type == REDIS_HASH) {
        /* Save a hash value */
        // 处理REDIS_ENCODING_ZIPLIST编码的hash
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            // 计算ziplist所占用空间大小
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            // ziplist本身是一个字符数组，这里以字符串的形式保存整个ziplist
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;

        } 
        // 处理REDIS_ENCODING_HT编码的hash
        else if (o->encoding == REDIS_ENCODING_HT) {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            // 保存字典dict的节点个数
            if ((n = rdbSaveLen(rdb,dictSize((dict*)o->ptr))) == -1) return -1;
            nwritten += n;

            // 遍历字典dict的每一个节点
            while((de = dictNext(di)) != NULL) {
                // 获取当前节点（键值对）的key值和value值
                robj *key = dictGetKey(de);
                robj *val = dictGetVal(de);

                // 以字符串的形式保存key值和value值
                if ((n = rdbSaveStringObject(rdb,key)) == -1) return -1;
                nwritten += n;
                if ((n = rdbSaveStringObject(rdb,val)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);

        } else {
            redisPanic("Unknown hash encoding");
        }

    } else {
        redisPanic("Unknown object type");
    }
    return nwritten;
}

/* Return the length the object will have on disk if saved with
 * the rdbSaveObject() function. Currently we use a trick to get
 * this length with very little changes to the code. In the future
 * we could switch to a faster solution. */
/*  返回对象o保存在RDB中所占用的字节长度。*/
off_t rdbSavedObjectLen(robj *o) {
    int len = rdbSaveObject(NULL,o);
    redisAssertWithInfo(NULL,o,len != -1);
    return len;
}

/* Save a key-value pair, with expire time, type, key, value.
 * On error -1 is returned.
 * On success if the key was actually saved 1 is returned, otherwise 0
 * is returned (the key was already expired). */
/*  将键值对相关的键、值、过期时间、类型信息写入RDB中。
    如果操作失败返回-1。
    如果操作成功则返回1。
    如果该key已经过期则返回0。*/
int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val,
                        long long expiretime, long long now)
{
    /* Save the expire time */
    // 保存key的过期时间
    if (expiretime != -1) {
        /* If this key is already expired skip it */
        // 如果该key已经过期，直接返回
        if (expiretime < now) return 0;
        // 保存类型信息
        if (rdbSaveType(rdb,REDIS_RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        /// 保存过期时间
        if (rdbSaveMillisecondTime(rdb,expiretime) == -1) return -1;
    }

    /* Save type, key, value */
    // 分别保存类型、key、value值信息
    if (rdbSaveObjectType(rdb,val) == -1) return -1;
    if (rdbSaveStringObject(rdb,key) == -1) return -1;
    if (rdbSaveObject(rdb,val) == -1) return -1;
    return 1;
}

/* Produces a dump of the database in RDB format sending it to the specified
 * Redis I/O channel. On success REDIS_OK is returned, otherwise REDIS_ERR
 * is returned and part of the output, or all the output, can be
 * missing because of I/O errors.
 *
 * When the function returns REDIS_ERR and if 'error' is not NULL, the
 * integer pointed by 'error' is set to the value of errno just after the I/O
 * error. */
/*  将Redis数据库中的数据以RDB格式保存，该RDB文件以后将会发送到指定的Redis I/O通道。
    如果保存成功函数返回REDIS_OK，如果保存失败则返回REDIS_ERR。*/
int rdbSaveRio(rio *rdb, int *error) {
    dictIterator *di = NULL;
    dictEntry *de;
    char magic[10];
    int j;
    long long now = mstime();
    uint64_t cksum;

    // 设置校验和函数，rioGenericUpdateChecksum定义在rio.h文件中
    if (server.rdb_checksum)
        rdb->update_cksum = rioGenericUpdateChecksum;
    // 生成RDB文件版本号
    snprintf(magic,sizeof(magic),"REDIS%04d",REDIS_RDB_VERSION);
    // 写入RDB版本号
    if (rdbWriteRaw(rdb,magic,9) == -1) goto werr;

    // 遍历Redis服务器上的所有数据库
    for (j = 0; j < server.dbnum; j++) {
        // 获取当前数据库
        redisDb *db = server.db+j;
        // 获取当前数据库的键空间key space
        dict *d = db->dict;
        // 如果当前数据库为空，跳过
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);
        if (!di) return REDIS_ERR;

        /* Write the SELECT DB opcode */
        // 写入数据库DB的编号，即 j
        if (rdbSaveType(rdb,REDIS_RDB_OPCODE_SELECTDB) == -1) goto werr;
        if (rdbSaveLen(rdb,j) == -1) goto werr;

        /* Iterate this DB writing every entry */
        // 遍历键空间中的每一项，并写入RDB中
        while((de = dictNext(di)) != NULL) {
            // 获取key和value
            sds keystr = dictGetKey(de);
            robj key, *o = dictGetVal(de);
            long long expire;

            // 创建一个key对象
            initStaticStringObject(key,keystr);
            // 获取key的过期信息
            expire = getExpire(db,&key);
            // 保存当前键值对
            if (rdbSaveKeyValuePair(rdb,&key,o,expire,now) == -1) goto werr;
        }
        dictReleaseIterator(di);
    }
    di = NULL; /* So that we don't release it again on error. */

    /* EOF opcode */
    // 写入EOF符
    if (rdbSaveType(rdb,REDIS_RDB_OPCODE_EOF) == -1) goto werr;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. */
    // CRC64校验和，如果Redis校验和功能被关闭则cksum的值为0。在这种情况下当Redis载入RDB时会
    // 跳过该校验和的检查
    cksum = rdb->cksum;
    memrev64ifbe(&cksum);
    // 写入校验和
    if (rioWrite(rdb,&cksum,8) == 0) goto werr;
    return REDIS_OK;

werr:
    if (error) *error = errno;
    if (di) dictReleaseIterator(di);
    return REDIS_ERR;
}

/* This is just a wrapper to rdbSaveRio() that additionally adds a prefix
 * and a suffix to the generated RDB dump. The prefix is:
 *
 * $EOF:<40 bytes unguessable hex string>\r\n
 *
 * While the suffix is the 40 bytes hex string we announced in the prefix.
 * This way processes receiving the payload can understand when it ends
 * without doing any processing of the content. */
/*  该函数是rdbSaveRio()的包装，只是额外地往RDB文件中添加了前缀和后缀。
    前缀为：
    $EOF:<40 bytes unguessable hex string>\r\n

    后缀为前缀中的“<40 bytes unguessable hex string>”部分。
    这种方式可以在不改变RDB原始内容的前提下让接受进程知道结束位置
    */
int rdbSaveRioWithEOFMark(rio *rdb, int *error) {
    char eofmark[REDIS_EOF_MARK_SIZE];

    getRandomHexChars(eofmark,REDIS_EOF_MARK_SIZE);
    if (error) *error = 0;
    if (rioWrite(rdb,"$EOF:",5) == 0) goto werr;
    if (rioWrite(rdb,eofmark,REDIS_EOF_MARK_SIZE) == 0) goto werr;
    if (rioWrite(rdb,"\r\n",2) == 0) goto werr;
    if (rdbSaveRio(rdb,error) == REDIS_ERR) goto werr;
    if (rioWrite(rdb,eofmark,REDIS_EOF_MARK_SIZE) == 0) goto werr;
    return REDIS_OK;

werr: /* Write error. */
    /* Set 'error' only if not already set by rdbSaveRio() call. */
    if (error && *error == 0) *error = errno;
    return REDIS_ERR;
}

/* Save the DB on disk. Return REDIS_ERR on error, REDIS_OK on success. */
/*  save命令的底层函数。将Redis数据库db保存到磁盘中，如果操作成功函数返回REDIS_OK，如果操作失败函数返回REDIS_ERR。*/
int rdbSave(char *filename) {
    char tmpfile[256];
    FILE *fp;
    rio rdb;
    int error;

    // 生成临时文件名称
    snprintf(tmpfile,256,"temp-%d.rdb", (int) getpid());
    // 创建临时文件
    fp = fopen(tmpfile,"w");
    if (!fp) {
        redisLog(REDIS_WARNING, "Failed opening .rdb for saving: %s",
            strerror(errno));
        return REDIS_ERR;
    }

    // 初始化file rio对象
    rioInitWithFile(&rdb,fp);
    // 调用rdbSaveRio将db中的数据写入RDB文件中
    if (rdbSaveRio(&rdb,&error) == REDIS_ERR) {
        errno = error;
        goto werr;
    }

    /* Make sure data will not remain on the OS's output buffers */
    // flush操作，确保所有数据都写入RDB文件中
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    // 重命名临时文件
    if (rename(tmpfile,filename) == -1) {
        // 如果操作失败，删除临时文件
        redisLog(REDIS_WARNING,"Error moving temp DB file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }
    redisLog(REDIS_NOTICE,"DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    server.lastbgsave_status = REDIS_OK;
    return REDIS_OK;

werr:
    // 发生错误清理资源
    redisLog(REDIS_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return REDIS_ERR;
}

/*  bgsave命令的底层函数。rdbSave函数的非阻塞版本，利用子进程来实现生成RDB文件。*/
int rdbSaveBackground(char *filename) {
    pid_t childpid;
    long long start;

    // 如果Redis服务器正在执行bgsave命令，则直接返回REDIS_ERR
    if (server.rdb_child_pid != -1) return REDIS_ERR;

    // 记录bgsave命令执行前数据库被修改的次数
    server.dirty_before_bgsave = server.dirty;
    // 记录最后一次执行bgsave的时间戳
    server.lastbgsave_try = time(NULL);

    // 开始执行时间戳
    start = ustime();
    // fork一个子进程
    if ((childpid = fork()) == 0) {
        /*  下面是子进程执行的代码 */

        int retval;

        /* Child */
        // 关闭网络监听
        closeListeningSockets(0);
        // 设置进程标识
        redisSetProcTitle("redis-rdb-bgsave");
        // 通过rdbSave函数实现保存操作
        retval = rdbSave(filename);
        if (retval == REDIS_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                redisLog(REDIS_NOTICE,
                    "RDB: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }
        }
        // 向父进程发送信号
        exitFromChild((retval == REDIS_OK) ? 0 : 1);
    } else {
        /*  下面是父进程执行的代码 */
        /* Parent */
        // 计算fork函数执行的时间
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        // fork出错，返回
        if (childpid == -1) {
            server.lastbgsave_status = REDIS_ERR;
            redisLog(REDIS_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }
        // 打印bgsave开始的提示信息
        redisLog(REDIS_NOTICE,"Background saving started by pid %d",childpid);
        server.rdb_save_time_start = time(NULL);
        server.rdb_child_pid = childpid;
        server.rdb_child_type = REDIS_RDB_CHILD_TYPE_DISK;
        updateDictResizePolicy();
        return REDIS_OK;
    }
    return REDIS_OK; /* unreached */
}

/*  删除bgsave命令所产生的临时文件 */
void rdbRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,sizeof(tmpfile),"temp-%d.rdb", (int) childpid);
    unlink(tmpfile);
}

/* Load a Redis object of the specified type from the specified file.
 * On success a newly allocated object is returned, otherwise NULL. */
/*  从RDB文件中载入指定类型的Redis对象，操作成功返回一个新创建的对象，否则返回NULL。*/
robj *rdbLoadObject(int rdbtype, rio *rdb) {
    robj *o, *ele, *dec;
    size_t len;
    unsigned int i;

    // string类型对象
    if (rdbtype == REDIS_RDB_TYPE_STRING) {
        /* Read string value */
        if ((o = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        o = tryObjectEncoding(o);
    } 
    // list类型对象
    else if (rdbtype == REDIS_RDB_TYPE_LIST) {
        /* Read list value */
        // 读取list的长度，即包含的节点个数
        if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;

        /* Use a real list when there are too many entries */
        // 根据节点个数，创建linked list编码或ziplist编码的list对象
        if (len > server.list_max_ziplist_entries) {
            o = createListObject();
        } else {
            o = createZiplistObject();
        }

        /* Load every single element of the list */
        // 载入所有的list节点
        while(len--) {
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;

            /* If we are using a ziplist and the value is too big, convert
             * the object to a real list. */
            // 如果当前的list是linked list编码，并且当前节点的字符串对象长度超过了
            // list_max_ziplist_value则将list转换为linke list编码
            if (o->encoding == REDIS_ENCODING_ZIPLIST &&
                sdsEncodedObject(ele) &&
                sdslen(ele->ptr) > server.list_max_ziplist_value)
                    listTypeConvert(o,REDIS_ENCODING_LINKEDLIST);

            // 往ziplist中添加一个元素
            if (o->encoding == REDIS_ENCODING_ZIPLIST) {
                dec = getDecodedObject(ele);
                o->ptr = ziplistPush(o->ptr,dec->ptr,sdslen(dec->ptr),REDIS_TAIL);
                decrRefCount(dec);
                decrRefCount(ele);
            } 
            // 往linked list中添加一个元素
            else {
                ele = tryObjectEncoding(ele);
                listAddNodeTail(o->ptr,ele);
            }
        }
    } 
    // set类型对象
    else if (rdbtype == REDIS_RDB_TYPE_SET) {
        /* Read list/set value */
        // 读取set中保存的元素个数
        if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;

        /* Use a regular set when there are too many entries. */
        // 根据元素个数的多少，创建intset编码或dict编码的set对象
        if (len > server.set_max_intset_entries) {
            o = createSetObject();
            /* It's faster to expand the dict to the right size asap in order
             * to avoid rehashing */
            // 扩充空间，避免rehash操作
            if (len > DICT_HT_INITIAL_SIZE)
                dictExpand(o->ptr,len);
        } else {
            o = createIntsetObject();
        }

        /* Load every single element of the list/set */
        // 分别载入set中的每一个元素
        for (i = 0; i < len; i++) {
            long long llval;
            // 载入当前元素
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            ele = tryObjectEncoding(ele);

            // 将当前元素添加到inset编码的set集合中
            if (o->encoding == REDIS_ENCODING_INTSET) {
                /* Fetch integer value from element */
                // 如果当前元素不能用整型编码表示，需要将set对象装换为dict编码
                if (isObjectRepresentableAsLongLong(ele,&llval) == REDIS_OK) {
                    o->ptr = intsetAdd(o->ptr,llval,NULL);
                } else {
                    setTypeConvert(o,REDIS_ENCODING_HT);
                    dictExpand(o->ptr,len);
                }
            }

            /* This will also be called when the set was just converted
             * to a regular hash table encoded set */
            // 将当前元素添加到dict编码的set集合中
            if (o->encoding == REDIS_ENCODING_HT) {
                dictAdd((dict*)o->ptr,ele,NULL);
            } else {
                decrRefCount(ele);
            }
        }
    } 
    // zset类型对象
    else if (rdbtype == REDIS_RDB_TYPE_ZSET) {
        /* Read list/set value */
        size_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;

        // 读取zset中的元素个数
        if ((zsetlen = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
        // 创建zset对象
        o = createZsetObject();
        zs = o->ptr;

        /* Load every single element of the list/set */
        // 载入zset有序集合中的每一个元素
        while(zsetlen--) {
            robj *ele;
            double score;
            zskiplistNode *znode;

            // 载入元素值value
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            ele = tryObjectEncoding(ele);
            // 载入分值score
            if (rdbLoadDoubleValue(rdb,&score) == -1) return NULL;

            /* Don't care about integer-encoded strings. */
            // 记录zset中元素的最大长度，用于后面的编码转换判断
            if (sdsEncodedObject(ele) && sdslen(ele->ptr) > maxelelen)
                maxelelen = sdslen(ele->ptr);

            // 将元素及其分值添加到跳跃表中并关联到字典dict中
            znode = zslInsert(zs->zsl,score,ele);
            dictAdd(zs->dict,ele,&znode->score);
            incrRefCount(ele); /* added to skiplist */
        }

        /* Convert *after* loading, since sorted sets are not stored ordered. */
        //  如果有序集合zset中的元素个数并未超过zset_max_ziplist_entries且最大的元素长度也未超过
        //  zset_max_ziplist_value，则使用ziplist编码以节省空间
        if (zsetLength(o) <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
                zsetConvert(o,REDIS_ENCODING_ZIPLIST);
    } 
    // hash类型对象
    else if (rdbtype == REDIS_RDB_TYPE_HASH) {
        size_t len;
        int ret;

        // 读取hash对象中保存的键值对数量
        len = rdbLoadLen(rdb, NULL);
        if (len == REDIS_RDB_LENERR) return NULL;

        // 常见hash对象
        o = createHashObject();

        /* Too many entries? Use a hash table. */
        // 如果节点数量超过hash_max_ziplist_entries，转换为dict编码
        if (len > server.hash_max_ziplist_entries)
            hashTypeConvert(o, REDIS_ENCODING_HT);

        /* Load every field and value into the ziplist */
        //  如果当前hash对象为ziplist编码，载入所有的key值和value值并添加到ziplist中
        while (o->encoding == REDIS_ENCODING_ZIPLIST && len > 0) {
            robj *field, *value;

            len--;
            /* Load raw strings */
            // 载入key值
            field = rdbLoadStringObject(rdb);
            if (field == NULL) return NULL;
            redisAssert(sdsEncodedObject(field));
            // 载入value值
            value = rdbLoadStringObject(rdb);
            if (value == NULL) return NULL;
            redisAssert(sdsEncodedObject(value));

            /* Add pair to ziplist */
            // key值和value值组成一个键值对，添加到ziplisst中
            o->ptr = ziplistPush(o->ptr, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
            o->ptr = ziplistPush(o->ptr, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
            /* Convert to hash table if size threshold is exceeded */
            // 如果key或value的长度超过hash_max_ziplist_value，则转换为dict编码
            if (sdslen(field->ptr) > server.hash_max_ziplist_value ||
                sdslen(value->ptr) > server.hash_max_ziplist_value)
            {
                decrRefCount(field);
                decrRefCount(value);
                hashTypeConvert(o, REDIS_ENCODING_HT);
                break;
            }
            decrRefCount(field);
            decrRefCount(value);
        }

        /* Load remaining fields and values into the hash table */
        //  如果当前hash对象为dict编码，载入所有的key值和value值并添加到dict中
        while (o->encoding == REDIS_ENCODING_HT && len > 0) {
            robj *field, *value;

            len--;
            /* Load encoded strings */
            // 载入key值
            field = rdbLoadEncodedStringObject(rdb);
            if (field == NULL) return NULL;
            // 载入value值
            value = rdbLoadEncodedStringObject(rdb);
            if (value == NULL) return NULL;

            // 尝试对其进行编码以节省空间
            field = tryObjectEncoding(field);
            value = tryObjectEncoding(value);

            /* Add pair to hash table */
            // 添加到dict中
            ret = dictAdd((dict*)o->ptr, field, value);
            redisAssert(ret == DICT_OK);
        }

        /* All pairs should be read by now */
        redisAssert(len == 0);

    } 
    // 以下这些类型都是用字符数组来保存数据，可以直接读取然后恢复为原对象
    else if (rdbtype == REDIS_RDB_TYPE_HASH_ZIPMAP  ||
               rdbtype == REDIS_RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == REDIS_RDB_TYPE_SET_INTSET   ||
               rdbtype == REDIS_RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == REDIS_RDB_TYPE_HASH_ZIPLIST)
    {
        // 载入字符串对象
        robj *aux = rdbLoadStringObject(rdb);

        if (aux == NULL) return NULL;
        // 创建对象并分配空间
        o = createObject(REDIS_STRING,NULL); /* string is just placeholder */
        o->ptr = zmalloc(sdslen(aux->ptr));
        // 数据拷贝
        memcpy(o->ptr,aux->ptr,sdslen(aux->ptr));
        decrRefCount(aux);

        /* Fix the object encoding, and make sure to convert the encoded
         * data type into the base type if accordingly to the current
         * configuration there are too many elements in the encoded data
         * type. Note that we only check the length and not max element
         * size as this is an O(N) scan. Eventually everything will get
         * converted. */
        // 检查对象中保存的元素个数，如果其个数超过指定值则转换编码方式
        switch(rdbtype) {
            // zipmap编码的哈希表已经被废弃，需要统一转换为ziplist编码
            case REDIS_RDB_TYPE_HASH_ZIPMAP:
                /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
                {
                    // 创建ziplist
                    unsigned char *zl = ziplistNew();
                    unsigned char *zi = zipmapRewind(o->ptr);
                    unsigned char *fstr, *vstr;
                    unsigned int flen, vlen;
                    unsigned int maxlen = 0;

                    // 遍历zipmap，从中取出key值和value值并逐一添加到ziplist中
                    while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                        // 记录元素的最大长度，用以后面判断是否需要转换编码方式
                        if (flen > maxlen) maxlen = flen;
                        if (vlen > maxlen) maxlen = vlen;
                        // 将key值和value顺序加入ziplist中
                        zl = ziplistPush(zl, fstr, flen, ZIPLIST_TAIL);
                        zl = ziplistPush(zl, vstr, vlen, ZIPLIST_TAIL);
                    }

                    // 释放原zipmap对象空间，然后设置类型、编码信息
                    zfree(o->ptr);
                    o->ptr = zl;
                    o->type = REDIS_HASH;
                    o->encoding = REDIS_ENCODING_ZIPLIST;

                    // 检查是否需要进行编码方式的转换
                    if (hashTypeLength(o) > server.hash_max_ziplist_entries ||
                        maxlen > server.hash_max_ziplist_value)
                    {
                        hashTypeConvert(o, REDIS_ENCODING_HT);
                    }
                }
                break;

            // ziplist编码的list
            case REDIS_RDB_TYPE_LIST_ZIPLIST:
                o->type = REDIS_LIST;
                o->encoding = REDIS_ENCODING_ZIPLIST;
                // 检查是否需要进行编码方式的转换
                if (ziplistLen(o->ptr) > server.list_max_ziplist_entries)
                    listTypeConvert(o,REDIS_ENCODING_LINKEDLIST);
                break;

            // inset编码的set类型对象
            case REDIS_RDB_TYPE_SET_INTSET:
                o->type = REDIS_SET;
                o->encoding = REDIS_ENCODING_INTSET;
                // 检查是否需要进行编码方式的转换
                if (intsetLen(o->ptr) > server.set_max_intset_entries)
                    setTypeConvert(o,REDIS_ENCODING_HT);
                break;

            // ziplist编码的zset
            case REDIS_RDB_TYPE_ZSET_ZIPLIST:
                o->type = REDIS_ZSET;
                o->encoding = REDIS_ENCODING_ZIPLIST;
                // 检查是否需要进行编码方式的转换
                if (zsetLength(o) > server.zset_max_ziplist_entries)
                    zsetConvert(o,REDIS_ENCODING_SKIPLIST);
                break;

            // ziplist编码的hash对象
            case REDIS_RDB_TYPE_HASH_ZIPLIST:
                o->type = REDIS_HASH;
                o->encoding = REDIS_ENCODING_ZIPLIST;
                // 检查是否需要进行编码方式的转换
                if (hashTypeLength(o) > server.hash_max_ziplist_entries)
                    hashTypeConvert(o, REDIS_ENCODING_HT);
                break;
            default:
                redisPanic("Unknown encoding");
                break;
        }
    } else {
        redisPanic("Unknown object type");
    }
    return o;
}

/* Mark that we are loading in the global state and setup the fields
 * needed to provide loading stats. */
/*  设置相关的全局状态位（表示当前正在载入RDB文件）*/
void startLoading(FILE *fp) {
    struct stat sb;

    /* Load the DB */
    // 设置载入标识
    server.loading = 1;
    // 开始进行载入的时间戳
    server.loading_start_time = time(NULL);
    // RDB文件大小
    server.loading_loaded_bytes = 0;
    if (fstat(fileno(fp), &sb) == -1) {
        server.loading_total_bytes = 0;
    } else {
        server.loading_total_bytes = sb.st_size;
    }
}

/* Refresh the loading progress info */
/*  刷新载入进度信息。*/
void loadingProgress(off_t pos) {
    server.loading_loaded_bytes = pos;
    if (server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

/* Loading finished */
/*  设置载入完成标识。*/
void stopLoading(void) {
    server.loading = 0;
}

/* Track loading progress in order to serve client's from time to time
   and if needed calculate rdb checksum  */
/*  记录载入进度供客户端查询用，如果有需要也会计算RDB校验和。*/
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len) {
    // 计算校验和
    if (server.rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    if (server.loading_process_events_interval_bytes &&
        (r->processed_bytes + len)/server.loading_process_events_interval_bytes > r->processed_bytes/server.loading_process_events_interval_bytes)
    {
        /* The DB can take some non trivial amount of time to load. Update
         * our cached time since it is used to create and update the last
         * interaction time with clients and for other important things. */
        updateCachedTime();
        if (server.masterhost && server.repl_state == REDIS_REPL_TRANSFER)
            replicationSendNewlineToMaster();
        loadingProgress(r->processed_bytes);
        processEventsWhileBlocked();
    }
}

/*  将RDB文件中的数据载入数据库中。*/
int rdbLoad(char *filename) {
    uint32_t dbid;
    int type, rdbver;
    redisDb *db = server.db+0;
    char buf[1024];
    long long expiretime, now = mstime();
    FILE *fp;
    rio rdb;

    // 打开RDB文件
    if ((fp = fopen(filename,"r")) == NULL) return REDIS_ERR;

    // 初始化file rio对象
    rioInitWithFile(&rdb,fp);
    // rdbLoadProgressCallback中既有校验和检验功能，又有更新载入进度功能
    rdb.update_cksum = rdbLoadProgressCallback;
    rdb.max_processing_chunk = server.loading_process_events_interval_bytes;
    // 读取RDB版本
    if (rioRead(&rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    // 检查RDB版本号
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Wrong signature trying to load DB from file");
        errno = EINVAL;
        return REDIS_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > REDIS_RDB_VERSION) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return REDIS_ERR;
    }

    // 设置开始载入标识
    startLoading(fp);
    while(1) {
        robj *key, *val;
        expiretime = -1;

        /* Read type. */
        // 读取类型信息，类型信息决定如何处理后面的数据
        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        // 读入过期时间（以秒为单位）
        if (type == REDIS_RDB_OPCODE_EXPIRETIME) {
            if ((expiretime = rdbLoadTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            // 过期时间后面紧接着是一个键值对
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliseconds. */
            // 将过期时间统一转换为以毫秒为单位
            expiretime *= 1000;
        } 
        // 读入过期时间（以毫秒为单位）
        else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS) {
            /* Milliseconds precision expire times introduced with RDB
             * version 3. */
            // 读入以毫秒为单位的过期时间
            if ((expiretime = rdbLoadMillisecondTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
             // 过期时间后面紧接着是一个键值对
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        }

        // 读入EOF标识
        if (type == REDIS_RDB_OPCODE_EOF)
            break;

        /* Handle SELECT DB opcode as a special case */
        // 读入数据库编号
        if (type == REDIS_RDB_OPCODE_SELECTDB) {
            // 得到数据库编号
            if ((dbid = rdbLoadLen(&rdb,NULL)) == REDIS_RDB_LENERR)
                goto eoferr;
            // 检查数据库编号是否合法
            if (dbid >= (unsigned)server.dbnum) {
                redisLog(REDIS_WARNING,"FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum);
                exit(1);
            }
            // 切换数据库
            db = server.db+dbid;
            continue;
        }
        /* Read key */
        // 读入key
        if ((key = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
        /* Read value */
        // 读入对应的object
        if ((val = rdbLoadObject(type,&rdb)) == NULL) goto eoferr;
        /* Check if the key already expired. This function is used when loading
         * an RDB file from disk, either at startup, or when an RDB was
         * received from the master. In the latter case, the master is
         * responsible for key expiry. If we would expire keys here, the
         * snapshot taken by the master may not be reflected on the slave. */
        // 检查当前key是否过期。如果当前Redis服务器为master，对于过期key不在加入数据库DB中。
        if (server.masterhost == NULL && expiretime != -1 && expiretime < now) {
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }
        /* Add the new object in the hash table */
        // 将key和相应的object关联到数据库中
        dbAdd(db,key,val);

        /* Set the expire time if needed */
        // 设置当前key的过期时间
        if (expiretime != -1) setExpire(db,key,expiretime);

        decrRefCount(key);
    }
    /* Verify the checksum if RDB version is >= 5 */
    // 如果RDB的版本 >= 5且服务器开启了校验功能，计算校验和
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;

        // 读入校验和
        if (rioRead(&rdb,&cksum,8) == 0) goto eoferr;
        memrev64ifbe(&cksum);
        // 对比校验和是否相同
        if (cksum == 0) {
            redisLog(REDIS_WARNING,"RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            redisLog(REDIS_WARNING,"Wrong RDB checksum. Aborting now.");
            exit(1);
        }
    }

    // 关闭RDB文件
    fclose(fp);
    // 设置载入完成标识
    stopLoading();
    return REDIS_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    // 出错处理
    redisLog(REDIS_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
    exit(1);
    return REDIS_ERR; /* Just to avoid warning */
}

/* A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of actual BGSAVEs. */
/*  当bgsave命令fork的子进程完成RDB文件的写入后向父进程发送信号，该函数用来处理bgsave发出的信号。*/
void backgroundSaveDoneHandlerDisk(int exitcode, int bysignal) {
    // bgsave命令执行成功
    if (!bysignal && exitcode == 0) {
        redisLog(REDIS_NOTICE,
            "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.lastbgsave_status = REDIS_OK;
    } 
    // bgsave命令执行失败
    else if (!bysignal && exitcode != 0) {
        redisLog(REDIS_WARNING, "Background saving error");
        server.lastbgsave_status = REDIS_ERR;
    } 
    // bgsave命令被中断
    else {
        mstime_t latency;

        redisLog(REDIS_WARNING,
            "Background saving terminated by signal %d", bysignal);
        latencyStartMonitor(latency);
        // 删除临时文件
        rdbRemoveTempFile(server.rdb_child_pid);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("rdb-unlink-temp-file",latency);
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * tirggering an error conditon. */
        if (bysignal != SIGUSR1)
            server.lastbgsave_status = REDIS_ERR;
    }
    // 更新当前Redis服务器信息
    server.rdb_child_pid = -1;
    server.rdb_child_type = REDIS_RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_last = time(NULL)-server.rdb_save_time_start;
    server.rdb_save_time_start = -1;
    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    // 可能有一些slave节点在等待bgsave命令完成，下面的函数用户处理这种节点
    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? REDIS_OK : REDIS_ERR, REDIS_RDB_CHILD_TYPE_DISK);
}

/* A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of RDB -> Salves socket transfers for
 * diskless replication. */
/*  该函数用来处理使用无硬盘复制的方式将RDB通过socket发送给从节点这一操作完成后的情况。*/
void backgroundSaveDoneHandlerSocket(int exitcode, int bysignal) {
    uint64_t *ok_slaves;

    // 根据操作结果记录日志信息
    if (!bysignal && exitcode == 0) {
        redisLog(REDIS_NOTICE,
            "Background RDB transfer terminated with success");
    } else if (!bysignal && exitcode != 0) {
        redisLog(REDIS_WARNING, "Background transfer error");
    } else {
        redisLog(REDIS_WARNING,
            "Background transfer terminated by signal %d", bysignal);
    }
    // 重置RDB相关的状态
    server.rdb_child_pid = -1;
    server.rdb_child_type = REDIS_RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_start = -1;

    /* If the child returns an OK exit code, read the set of slave client
     * IDs and the associated status code. We'll terminate all the slaves
     * in error state.
     *
     * If the process returned an error, consider the list of slaves that
     * can continue to be emtpy, so that it's just a special case of the
     * normal code path. */
    //  当子进程的退出码为OK时（即成功退出），读出各个从节点客户端的ID和相关的状态码，然后终结处于错误状态的从节点
    ok_slaves = zmalloc(sizeof(uint64_t)); /* Make space for the count. */
    ok_slaves[0] = 0;
    if (!bysignal && exitcode == 0) {
        int readlen = sizeof(uint64_t);

        /* 下面if分支中的代码通过匿名管道读出各个从节点客户端的ID和相关的状态码，其返回消息的格式为<len> <slave[0].id> <slave[0].error> ... */

        // 读出<len>字段
        if (read(server.rdb_pipe_read_result_from_child, ok_slaves, readlen) ==
                 readlen)
        {
            readlen = ok_slaves[0]*sizeof(uint64_t)*2;

            /* Make space for enough elements as specified by the first
             * uint64_t element in the array. */
            // 开辟足够的空间来存放回复消息
            ok_slaves = zrealloc(ok_slaves,sizeof(uint64_t)+readlen);
            // 读出完整的回复信息
            if (readlen &&
                read(server.rdb_pipe_read_result_from_child, ok_slaves+1,
                     readlen) != readlen)
            {
                ok_slaves[0] = 0;
            }
        }
    }

    // 关闭匿名管道fd
    close(server.rdb_pipe_read_result_from_child);
    close(server.rdb_pipe_write_result_to_parent);

    /* We can continue the replication process with all the slaves that
     * correctly received the full payload. Others are terminated. */
    // 如果所有的从节点都正确接收到了完成的RDB数据，我们可以继续复制过程，否则终结之
    listNode *ln;
    listIter li;

    // 遍历从节点列表
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        // 得到当前从节点客户端指针
        redisClient *slave = ln->value;

        // REDIS_REPL_WAIT_BGSAVE_END表示该从节点等待主节点后台RDB数据转储的完成
        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) {
            uint64_t j;
            int errorcode = 0;

            /* Search for the slave ID in the reply. In order for a slave to
             * continue the replication process, we need to find it in the list,
             * and it must have an error code set to 0 (which means success). */
            // 在ok_slaves数组中查找当前从节点对应的状态信息
            for (j = 0; j < ok_slaves[0]; j++) {
                if (slave->id == ok_slaves[2*j+1]) {
                    errorcode = ok_slaves[2*j+2];
                    break; /* Found in slaves list. */
                }
            }
            // 如果该从节点未能成功接收RDB数据，终结之
            if (j == ok_slaves[0] || errorcode != 0) {
                redisLog(REDIS_WARNING,
                "Closing slave %s: child->slave RDB transfer failed: %s",
                    replicationGetSlaveName(slave),
                    (errorcode == 0) ? "RDB transfer child aborted"
                                     : strerror(errorcode));
                freeClient(slave);
            } 
            // 如果该从节点成功接收RDB数据，回复之
            else {
                redisLog(REDIS_WARNING,
                "Slave %s correctly received the streamed RDB file.",
                    replicationGetSlaveName(slave));
                /* Restore the socket as non-blocking. */
                anetNonBlock(NULL,slave->fd);
                anetSendTimeout(NULL,slave->fd,0);
            }
        }
    }
    zfree(ok_slaves);

    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? REDIS_OK : REDIS_ERR, REDIS_RDB_CHILD_TYPE_SOCKET);
}

/* When a background RDB saving/transfer terminates, call the right handler. */
/*  当一个后台RDB存储操作结束后调用该函数进行处理    */
void backgroundSaveDoneHandler(int exitcode, int bysignal) {
    switch(server.rdb_child_type) {
    case REDIS_RDB_CHILD_TYPE_DISK:
        backgroundSaveDoneHandlerDisk(exitcode,bysignal);
        break;
    case REDIS_RDB_CHILD_TYPE_SOCKET:
        backgroundSaveDoneHandlerSocket(exitcode,bysignal);
        break;
    default:
        redisPanic("Unknown RDB child type.");
        break;
    }
}

/* Spawn an RDB child that writes the RDB to the sockets of the slaves
 * that are currently in REDIS_REPL_WAIT_BGSAVE_START state. */
/*  创建一个子进程用于将RDB文件发送给处在REDIS_REPL_WAIT_BGSAVE_START状态的slave节点。*/
int rdbSaveToSlavesSockets(void) {
    int *fds;
    uint64_t *clientids;
    int numfds;
    listNode *ln;
    listIter li;
    pid_t childpid;
    long long start;
    int pipefds[2];

    // 如果Redis服务器正在执行bgsave命令，则直接返回REDIS_ERR
    if (server.rdb_child_pid != -1) return REDIS_ERR;

    /* Before to fork, create a pipe that will be used in order to
     * send back to the parent the IDs of the slaves that successfully
     * received all the writes. */
    // 在执行fork调用之前，创建匿名管道用于向父进程发送成功接收RDB文件的slave节点ID
    if (pipe(pipefds) == -1) return REDIS_ERR;
    // 记录相关句柄
    server.rdb_pipe_read_result_from_child = pipefds[0];
    server.rdb_pipe_write_result_to_parent = pipefds[1];

    /* Collect the file descriptors of the slaves we want to transfer
     * the RDB to, which are i WAIT_BGSAVE_START state. */
    // 分配数组空间，用于保存slave节点的socket文件描述符
    fds = zmalloc(sizeof(int)*listLength(server.slaves));
    /* We also allocate an array of corresponding client IDs. This will
     * be useful for the child process in order to build the report
     * (sent via unix pipe) that will be sent to the parent. */
    // 分配数组空间，用于记录相应的slave节点ID
    clientids = zmalloc(sizeof(uint64_t)*listLength(server.slaves));
    numfds = 0;

    // 遍历slave节点列表
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        // 如果当前从节点正处于REDIS_REPL_WAIT_BGSAVE_START状态，加入fds数组和clientids数组中
        // REDIS_REPL_WAIT_BGSAVE_START状态表示该从节点等待主节点后台RDB数据转储的开始；
        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) {
            clientids[numfds] = slave->id;
            fds[numfds++] = slave->fd;
            replicationSetupSlaveForFullResync(slave,getPsyncInitialOffset());
            /* Put the socket in non-blocking mode to simplify RDB transfer.
             * We'll restore it when the children returns (since duped socket
             * will share the O_NONBLOCK attribute with the parent). */
            // 设置socket为非阻塞状态以简化RDB传输
            anetBlock(NULL,slave->fd);
            anetSendTimeout(NULL,slave->fd,server.repl_timeout*1000);
        }
    }

    /* Create the child process. */
    // 创建子进程
    start = ustime();
    if ((childpid = fork()) == 0) {
        /*  下面是子进程运行的代码 */

        /* Child */
        int retval;
        rio slave_sockets;

        // 创建fd set rio对象
        rioInitWithFdset(&slave_sockets,fds,numfds);
        zfree(fds);

        closeListeningSockets(0);
        redisSetProcTitle("redis-rdb-to-slaves");

        // 调用rdbSaveRioWithEOFMark函数发送RDB文件内容
        retval = rdbSaveRioWithEOFMark(&slave_sockets,NULL);
        // flush
        if (retval == REDIS_OK && rioFlush(&slave_sockets) == 0)
            retval = REDIS_ERR;

        if (retval == REDIS_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                redisLog(REDIS_NOTICE,
                    "RDB: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }

            /* If we are returning OK, at least one slave was served
             * with the RDB file as expected, so we need to send a report
             * to the parent via the pipe. The format of the message is:
             *
             * <len> <slave[0].id> <slave[0].error> ...
             *
             * len, slave IDs, and slave errors, are all uint64_t integers,
             * so basically the reply is composed of 64 bits for the len field
             * plus 2 additional 64 bit integers for each entry, for a total
             * of 'len' entries.
             *
             * The 'id' represents the slave's client ID, so that the master
             * can match the report with a specific slave, and 'error' is
             * set to 0 if the replication process terminated with a success
             * or the error code if an error occurred. */
            //  如果我们返回REDIS_OK，表明至少有一个slave节点接受到了RDB文件，所以我们需要通过匿名管道通知父进程。
            //  通知消息的格式为：
            //      <len> <slave[0].id> <slave[0].error> ... 
            //  其中len、slave id、slave error都是uint64_t整型。
            //  id字段表示slave节点的id，error字段如果是0，表示复制过程成功执行，否则被置为相应的错误码error code

            // 分配回复消息所需要的内存空间
            void *msg = zmalloc(sizeof(uint64_t)*(1+2*numfds));
            uint64_t *len = msg;
            uint64_t *ids = len+1;
            int j, msglen;

            // 记录各个slave节点的处理结果
            *len = numfds;
            for (j = 0; j < numfds; j++) {
                *ids++ = clientids[j];
                *ids++ = slave_sockets.io.fdset.state[j];
            }

            /* Write the message to the parent. If we have no good slaves or
             * we are unable to transfer the message to the parent, we exit
             * with an error so that the parent will abort the replication
             * process with all the childre that were waiting. */
            // 将回复消息发送给父进程。
            msglen = sizeof(uint64_t)*(1+2*numfds);
            if (*len == 0 ||
                write(server.rdb_pipe_write_result_to_parent,msg,msglen)
                != msglen)
            {
                retval = REDIS_ERR;
            }
            zfree(msg);
        }
        zfree(clientids);
        // 如果我们无法将消息发送给父进程，则错误退出，这样父进程将中断复制过程
        exitFromChild((retval == REDIS_OK) ? 0 : 1);
    } else {
        /*  下面是父进程运行的代码 */
        /* Parent */
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);

        // 创建子进程出错
        if (childpid == -1) {
            redisLog(REDIS_WARNING,"Can't save in background: fork: %s",
                strerror(errno));

            /* Undo the state change. The caller will perform cleanup on
             * all the slaves in BGSAVE_START state, but an early call to
             * replicationSetupSlaveForFullResync() turned it into BGSAVE_END */
            // 撤销状态修改
            listRewind(server.slaves,&li);
            while((ln = listNext(&li))) {
                redisClient *slave = ln->value;
                int j;

                for (j = 0; j < numfds; j++) {
                    if (slave->id == clientids[j]) {
                        slave->replstate = REDIS_REPL_WAIT_BGSAVE_START;
                        break;
                    }
                }
            }
            // 关闭匿名管道
            close(pipefds[0]);
            close(pipefds[1]);
        } else {
            redisLog(REDIS_NOTICE,"Background RDB transfer started by pid %d",
                childpid);
            server.rdb_save_time_start = time(NULL);
            server.rdb_child_pid = childpid;
            server.rdb_child_type = REDIS_RDB_CHILD_TYPE_SOCKET;
            updateDictResizePolicy();
        }
        zfree(clientids);
        zfree(fds);
        return (childpid == -1) ? REDIS_ERR : REDIS_OK;
    }
    return REDIS_OK; /* Unreached. */
}

/*  save命令实现 */
void saveCommand(redisClient *c) {
    // 如果后台正在执行bgsave命令，直接退出
    if (server.rdb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");
        return;
    }
    // 阻塞执行save命令
    if (rdbSave(server.rdb_filename) == REDIS_OK) {
        addReply(c,shared.ok);
    } else {
        addReply(c,shared.err);
    }
}

/*  bgsave命令实现 */
void bgsaveCommand(redisClient *c) {
    // 如果后台正在执行bgsave命令，直接退出
    if (server.rdb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");
    } 
    // 如果后台正在执行bgrewriteaof命令（重写AOF文件），直接退出
    else if (server.aof_child_pid != -1) {
        addReplyError(c,"Can't BGSAVE while AOF log rewriting is in progress");
    } 
    // 执行bgsave命令
    else if (rdbSaveBackground(server.rdb_filename) == REDIS_OK) {
        addReplyStatus(c,"Background saving started");
    } else {
        addReply(c,shared.err);
    }
}
