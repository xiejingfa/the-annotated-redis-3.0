#ifndef CRC64_H
#define CRC64_H

#include <stdint.h>

/* 生成CRC64循环冗余校验码 */
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);

#endif
