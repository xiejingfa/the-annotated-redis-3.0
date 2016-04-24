/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 *	ZSET使用两种结构来有序地保存同一元素从而提供O(log(N))复杂度的插入和删除操作。
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view"). 
 *	
 *	元素被添加到哈希表中，这个哈希表维持着Redis对象到分值score的映射关系。同时该元素也添加到跳跃表中，
 *	跳跃表维持着分值score到Redis对象的映射关系。这样ZSET就（给用户）提供了一种按分值score排序的视图。
 */

/* This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. 
 *
 *	Redis中的跳跃表实现和William Pugh在《Skip Lists: A Probabilistic Alternative to Balanced Trees》
 *	一文中描述的跳跃表基本一致，主要有以下三点改进：
 *	（1）、Redis中的跳跃表允许有重复的分值score。
 *	（2）、节点的比较操作不仅仅比较其分值score，同时还要比较其关联的元素值。
 *	（3）、每个节点还有一个后退指针（相当于双向链表中的prev指针），通个该指针，我们可以从表尾向表头遍历列表。
 *		从这点看，跳跃表的第一层是一个双向链表。
 */

#include "redis.h"
#include <math.h>

static int zslLexValueGteMin(robj *value, zlexrangespec *spec);
static int zslLexValueLteMax(robj *value, zlexrangespec *spec);

/*	创建一个层数为level的跳跃表节点，并设置该节点的分值、元素值。*/
zskiplistNode *zslCreateNode(int level, double score, robj *obj) {
	// zskiplistNode中的level数组并不是固定大小的，而是可变大小的，每次创建节点需要动态计算
	//	level*sizeof(struct zskiplistLevel)为level[]所占用空间
    zskiplistNode *zn = zmalloc(sizeof(*zn)+level*sizeof(struct zskiplistLevel));
    // 设置分值、节点数据
    zn->score = score;
    zn->obj = obj;
    // 返回节点指针
    return zn;
}

/* 创建一个跳跃表 */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    // 分配空间
    zsl = zmalloc(sizeof(*zsl));
    // 当前最大层数为1，节点数量为0
    zsl->level = 1;
    zsl->length = 0;
    // 列表的初始化需要初始化头部，并使头部每层（根据事先定义的ZSKIPLIST_MAXLEVEL）指向末尾（NULL）
    // ZSKIPLIST_MAXLEVEL的默认值为32
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    // 对于以个空跳跃表，backward指针也为空
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}

/* 释放指定的跳跃表节点 */
void zslFreeNode(zskiplistNode *node) {
    decrRefCount(node->obj);
    zfree(node);
}

/* 释放跳跃表 */
void zslFree(zskiplist *zsl) {
    zskiplistNode *node = zsl->header->level[0].forward, *next;

    // 释放表头
    zfree(zsl->header);
    // 逐一释放每个节点
    while(node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }
    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
/* 	返回一个随机值作为跳跃表新节点的层数。 该随机值的数值范围是[1, ZSKIPLIST_MAXLEVEL]。
	该函数使用了powerlaw-alike的方法，能够保证越大的值生成的概率越小。 

	跳表是一种随机化数据结构，其随机化体现在插入元素的时候元素所占有的层数完全是随机的，层数是通过随机算法产生的
*/
int zslRandomLevel(void) {
    int level = 1;
    while ((random()&0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* 往跳跃表中插入一个新节点，新节点的分值为score、保存的元素值为obj。*/
zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj) {
	// update数组用来保存降层节点指针
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    // 记录沿途跨越的节点数，用来计算新节点的span值
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    redisAssert(!isnan(score));
    x = zsl->header;
    // 从高到底在各层中查找插入位置
    for (i = zsl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];
        // 在当前层找到一个下降节点，并将其指针保存在update数组中
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                compareStringObjects(x->level[i].forward->obj,obj) < 0))) {
        	// 记录跨越的节点数
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
       	// update[i]就是要和新节点直接相连的节点
        update[i] = x;
    }
    /* we assume the key is not already inside, since we allow duplicated
     * scores, and the re-insertion of score and redis object should never
     * happen since the caller of zslInsert() should test in the hash table
     * if the element is already inside or not. */
    //	由调用者保证不会插入相同分值和相同成员的元素，所以这里不需要检查
    level = zslRandomLevel();
    // 如果新节点的层数比目前其它节点的层数都要大，那么表头节点header就会存在一些未初始化的层，
    // 这里将他们记录在update数组中，方便后面处理
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }
        // 更新当前最大层数
        zsl->level = level;
    }
    // 创建新节点
    x = zslCreateNode(level,score,obj);
    // 从低往高逐层更新节点指针，类似于链表的插入
    for (i = 0; i < level; i++) {
    	// 设置新节点的forward指针，指向原节点的下一个节点
        x->level[i].forward = update[i]->level[i].forward;
        // 让原节点的forward指针指向新节点
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        // 更新新节点跨越的节点数量
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    // 更新新节点的backward指针
    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    // 更新新节点下一个节点的backward指针
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zsl->tail = x;
    // 增加了一个新节点
    zsl->length++;
    return x;
}

/* Internal function used by zslDelete, zslDeleteByScore and zslDeleteByRank */
/* 删除节点函数，供zslDelete、zslDeleteByScore和zslDeleteByRank函数调用 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;
    // 从低往高，逐层删除x节点，只要简单地修改指针指向即可，和链表删除节点类似
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }

    // 更新被删除节点x的backward指针
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }

    // 如果该节点的level是最大的，则需要更新跳表的level
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;
    // 跳跃表节点个数减1
    zsl->length--;
}

/* Delete an element with matching score/object from the skiplist. */
/* 从从跳跃表中删除一个分值score、元素值为obj的节点。 */
int zslDelete(zskiplist *zsl, double score, robj *obj) {
	/* 下面这个过程和zslInsert类似，*/

	// update数组用来保存降层节点指针
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    // 从高往低逐层查找目标节点，并把降层节点指针保存在update中
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
            	// 既比较分值score又比较节点对象
                (x->level[i].forward->score == score &&
                compareStringObjects(x->level[i].forward->obj,obj) < 0)))
            x = x->level[i].forward;
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    // 跳跃表中可能存在重复的分值，只有在分值score和元素值obj都相等的节点才是正在要删除的节点
    x = x->level[0].forward;
    if (x && score == x->score && equalStringObjects(x->obj,obj)) {
    	// 删除节点
        zslDeleteNode(zsl, x, update);
        // 释放空间
        zslFreeNode(x);
        return 1;
    }
    return 0; /* not found */
}

/*  判断一个给定值value是否大于或者大于等于spec中的min值。
	zrangespec定义在redis.h文件中，表示一个开区间/闭区间。 */
static int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

/* 判断一个给定值value是否小于或小于等于spec中的max值。
	zrangespec定义在redis.h文件中，表示一个开区间/闭区间。 */
static int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range. */
/* 如果range给定的数值范围包含在跳跃表的分值范围则返回1，否则返回0。*/
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    // 验证range指定的范围是否为空
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    // 检查跳跃表中的最大分值score
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;

    // 检查跳跃表中的最小分值score
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/* 返回跳跃表中第一个分值score在range指定范围的节点，如果没有一个节点符合要求则返回NULL。*/
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 如果给定range不包含在跳跃表的分值范围内，马上返回
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    // 从前往后遍历跳跃表，查找第一个分值落在range指定范围的节点
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    // 找到第一层中的目标节点
    x = x->level[0].forward;
    redisAssert(x != NULL);

    /* Check if score <= max. */
    // 最后还要检查当前节点的分值是否超出range的右边界，即score <= max
    if (!zslValueLteMax(x->score,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/* 返回跳跃表中最后一个分值score在range指定范围的节点，如果没有一个节点符合要求则返回NULL。*/
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 同样，如果给定range不包含在跳跃表的分值范围内，马上返回
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    // 从前往后遍历遍历跳跃表，查找最后一个分值小于或小于等于range.max的节点
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslValueLteMax(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    redisAssert(x != NULL);

    /* Check if score >= min. */
    // 反过来，又要检查该节点的分值是否超出range的左边界
    if (!zslValueGteMin(x->score,range)) return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
/* 	在跳跃表中删除所有分值在给定范围range内的节点。
	注意range中指定的min和max包含在范围之内（即闭区间），所以边界情况score >= min或者score <= max的节点也会被删除。
	另外，不仅跳跃表zsl中的满足要求的节点会被删除，在字典dict中相应的节点也会被删除（以维持zset的一致性）。*/
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
	// update数组用来记录降层节点
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;
    // 从前往后遍历，记录降层节点，方面以后修改指针
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (range->minex ?
            x->level[i].forward->score <= range->min :
            x->level[i].forward->score < range->min))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    // 定位到第一次中待删除的第一个节点
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    // 删除range指定范围内的所有节点
    while (x &&
           (range->maxex ? x->score < range->max : x->score <= range->max))
    {
    	// 记录下一个节点的位置
        zskiplistNode *next = x->level[0].forward;
        // 删除节点
        zslDeleteNode(zsl,x,update);
        // 删除dict中相应的元素
        dictDelete(dict,x->obj);
        zslFreeNode(x);
        // 记录删除节点个数
        removed++;
        // 指向下一个节点
        x = next;
    }
    return removed;
}

/* 删除元素值在指定字典序范围的元素，同样也会在dict中删除相应元素。*/
unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict) {
	// update数组用来记录降层节点
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    x = zsl->header;
    // 从前往后遍历，记录降层节点
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->obj,range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    // 移动到第一个待删除节点的位置
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    // 删除指定范围内的元素
    while (x && zslLexValueLteMax(x->obj,range)) {
    	// 记录下一个节点位置
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->obj);
        zslFreeNode(x);
        removed++;
        // 继续处理下一个节点
        x = next;
    }
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
/*	在跳跃表中删除给定排序范围的节点，注意两点：一是参数start和参数end是包含在内的，二是参数start和参数end都是
	从1开始计数的。该函数最后返回被删除节点数量。*/
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    // update数组用来记录降层节点
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;
    // 从前往后遍历跳跃表，定位到指定排位的起始位置的前置节点，并记录降层节点
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    // 当前排位
    traversed++;
    // 指向排位起始的第一个节点
    x = x->level[0].forward;
    // 删除落在指定排序范围的所有节点
    while (x && traversed <= end) {
    	// 记录下一个节点
        zskiplistNode *next = x->level[0].forward;
        // 从跳跃表中删除节点
        zslDeleteNode(zsl,x,update);
        // 从字典dict中删除节点
        dictDelete(dict,x->obj);
        zslFreeNode(x);
        // 被删除元素个数加1
        removed++;
        // 排位计数加1
        traversed++;
        // 指向下一个节点
        x = next;
    }
    return removed;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
 /* 返回指定元素在跳跃表中的排位，该元素由分值score和元素值对象o指定。
 	如果该元素不在跳跃表中返回0，否则返回相应排位。
 	注意到由于跳跃表的头结点header也包含在内并作为第1个元素（下标为0），排位rank是从1开始计算的。*/
unsigned long zslGetRank(zskiplist *zsl, double score, robj *o) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    // 从前往后遍历跳远表，逐一对当前节点和目标节点进行比对。既要比对分值score也要比对节点的元素值
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                compareStringObjects(x->level[i].forward->obj,o) <= 0))) {
        	// 更新排位信息
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        // x可能指向头结点，需要再次测试
        if (x->obj && equalStringObjects(x->obj,o)) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
/* 返回指定排位上的节点，参数rank是从1开始计算的。*/
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    // 记录当前的排位信息
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    // 从前往后开始遍历，简答的查找过程，可以与链表查找第i个节点的操作进行类比
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* Populate the rangespec according to the objects min and max. */
/* 从min何max对象中解析出一个min-max区间，并存入spec中。*/
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;
    // 默认为闭区间
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    /*
		从命令行输入中解析min-max区间。如果一个输入参数以‘（’字符作为前缀，则认为是开区间的。
		比如ZRANGEBYSCORE zset (1.5 (2.5，解析为区间（1.5， 2.5）
		比如ZRANGEBYSCORE zset 1.5 2.5，解析为区间[1.5, 2.5]
    */
	// 确定spec->min数值和开闭情况
    if (min->encoding == REDIS_ENCODING_INT) {
    	// 如果是整型编码，直接确定边界
        spec->min = (long)min->ptr;
    } else {
    	// min对象为字符串编码，根据第一个字符确定开区间 or 闭区间
        if (((char*)min->ptr)[0] == '(') {
        	// strtod将字符串解析为浮点数
            spec->min = strtod((char*)min->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod((char*)min->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
        }
    }

    // 确定spec->max数值和开闭情况
    if (max->encoding == REDIS_ENCODING_INT) {
    	// 如果是整型编码，直接确定边界
        spec->max = (long)max->ptr;
    } else {
    	// max对象为字符串编码，根据第一个字符确定开区间 or 闭区间
        if (((char*)max->ptr)[0] == '(') {
            spec->max = strtod((char*)max->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod((char*)max->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
        }
    }

    return REDIS_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */
/* ------------------------ 字典序范围 ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparision, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. REDIS_OK will be
  * returned.
  *
  * If the string is not a valid range REDIS_ERR is returned, and the value
  * of *dest and *ex is undefined. */
/* 该函数主要用于解析ZRANGEBYLEX命令的min参数或max参数。
    (foo 表示开区间
    [foo 表示闭区间
    - 表示最小的字符串
    + 表示最大的字符串

    如果输入字符串合法则将解析后的结构存放在dest中。
    如果解析后的边界是闭区间，则ex被设置为0，如果是开区间，则ex被设置为1。
    返回值为REDIS_OK表示解析成功

    如果输入字符串不合法，则返回REDIS_ERR，此时dest和ex的值的undefined的。

    ！！！关于这个函数的作用大家可以联系ZRANGEBYLEX命令来理解！！！
*/
int zslParseLexRangeItem(robj *item, robj **dest, int *ex) {
    char *c = item->ptr;

    // 根据第一个字符即可做出判断
    switch(c[0]) {
    case '+':
        if (c[1] != '\0') return REDIS_ERR;
        *ex = 0;
        // 边界值为shared.maxstring
        *dest = shared.maxstring;
        incrRefCount(shared.maxstring);
        return REDIS_OK;
    case '-':
        if (c[1] != '\0') return REDIS_ERR;
        *ex = 0;
        // 边界值为shared.minstring
        *dest = shared.minstring;
        incrRefCount(shared.minstring);
        return REDIS_OK;
    case '(':
        // 设置为开区间
        *ex = 1;
        // 获取边界值
        *dest = createStringObject(c+1,sdslen(c)-1);
        return REDIS_OK;
    case '[':
        // 设置为闭区间
        *ex = 0;
        // 获取边界值
        *dest = createStringObject(c+1,sdslen(c)-1);
        return REDIS_OK;
    default:
        return REDIS_ERR;
    }
}

/* Populate the rangespec according to the objects min and max.
 *
 * Return REDIS_OK on success. On error REDIS_ERR is returned.
 * When OK is returned the structure must be freed with zslFreeLexRange(),
 * otherwise no release is needed. */
/*  将min和max指定的字符串进行解析，填入字典区间zlexrangespec中，操作成功返回REDIS_OK，否则返回REDIS_ERR。
    注意，如果操作成功，则以后需要使用zslFreeLexRange来释放spec结构，否则并不一定需要释放资源。

    该函数是zslParseLexRangeItem的封装，后者每次只能解析一个参数。
*/
static int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec) {
    /* The range can't be valid if objects are integer encoded.
     * Every item must start with ( or [. */
    // 由于每个参数只能是字符串编码的，如果是整型编码则无法解析
    if (min->encoding == REDIS_ENCODING_INT ||
        max->encoding == REDIS_ENCODING_INT) return REDIS_ERR;

    spec->min = spec->max = NULL;
    // 分别调用zslParseLexRangeItem来解析min参数和max参数
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) == REDIS_ERR ||
        zslParseLexRangeItem(max, &spec->max, &spec->maxex) == REDIS_ERR) {
        if (spec->min) decrRefCount(spec->min);
        if (spec->max) decrRefCount(spec->max);
        return REDIS_ERR;
    } else {
        return REDIS_OK;
    }
}

/* Free a lex range structure, must be called only after zelParseLexRange()
 * populated the structure with success (REDIS_OK returned). */
/* 释放一个字典序区间结构，因为zlexrangespec存在两个robj对象。
    zslParseLexRange调用成功后必须调用该函数来说释放资源。*/
void zslFreeLexRange(zlexrangespec *spec) {
    decrRefCount(spec->min);
    decrRefCount(spec->max);
}

/* This is just a wrapper to compareStringObjects() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of
 * -inf and +inf for strings */
/* 字典序字符串比较函数，实际上是在compareStringObjects函数封装的基础上增加了对
    shared.minstring和shared.maxstring的处理。*/
int compareStringObjectsForLexRange(robj *a, robj *b) {
    if (a == b) return 0; /* This makes sure that we handle inf,inf and
                             -inf,-inf ASAP. One special case less. */
    if (a == shared.minstring || b == shared.maxstring) return -1;
    if (a == shared.maxstring || b == shared.minstring) return 1;
    return compareStringObjects(a,b);
}

/* 以字典序方式判断给定值value是否 > 或者 >= spec.min。 */
static int zslLexValueGteMin(robj *value, zlexrangespec *spec) {
    return spec->minex ?
        (compareStringObjectsForLexRange(value,spec->min) > 0) :
        (compareStringObjectsForLexRange(value,spec->min) >= 0);
}

/* 以字典序方式判断给定值value是否 < 或者 <= spec.max。 */
static int zslLexValueLteMax(robj *value, zlexrangespec *spec) {
    return spec->maxex ?
        (compareStringObjectsForLexRange(value,spec->max) < 0) :
        (compareStringObjectsForLexRange(value,spec->max) <= 0);
}

/* Returns if there is a part of the zset is in the lex range. */
/*  判断跳跃表中是否有一部分节点落在字典序区间内。
    注意，比较的不是分值score，而是obj。 */
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    // 判断字典序区间是否合法
    if (compareStringObjectsForLexRange(range->min,range->max) > 1 ||
            (compareStringObjects(range->min,range->max) == 0 &&
            (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslLexValueGteMin(x->obj,range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslLexValueLteMax(x->obj,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
/* 查找跳跃表中落在字典序区间范围的第一个节点，如果不存在这样的节点则返回NULL。*/
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 如果跳跃表中没有节点落在指定字典序区间，则直接退出
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    // 从前往后遍历跳跃表，查找满足要求的第一个节点
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->obj,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    // 移动到目标节点
    x = x->level[0].forward;
    redisAssert(x != NULL);

    /* Check if score <= max. */
    // 最后再次验证是否超出右边界
    if (!zslLexValueLteMax(x->obj,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/* 查找跳跃表中落在字典序区间范围的最后一个节点，如果不存在这样的节点则返回NULL。*/
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 如果跳跃表中没有节点落在指定字典序区间，则直接退出
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    // 从前往后遍历跳跃表，查找满足要求的最后一个节点
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslLexValueLteMax(x->level[i].forward->obj,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    redisAssert(x != NULL);

    /* Check if score >= min. */
    // 最后验证是否超出左边界
    if (!zslLexValueGteMin(x->obj,range)) return NULL;
    return x;
}

/*-----------------------------------------------------------------------------
 * Ziplist-backed sorted set API 以ziplis为底层结构的zset操作
 *----------------------------------------------------------------------------*/

/****************************************************************************************
	如果zset使用ziplist作为底层结构，则每个有序集元素以两个相邻的ziplist节点表示， 第一个节点保存元素值，
    接下来的第二个元素保存元素的分值score。为了方便描述，我们分别将这两个节点称之为“元素值节点”和“分值节点”
*****************************************************************************************/

/* 获取sptr指针所指节点的分值score，实际上就是获取有序集合中某个元素的分值score域。 */
double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    char buf[128];
    double score;

    redisAssert(sptr != NULL);
    // 调用ziplistGet获取节点的值，如果该节点保存的是字符串，则存放在vstr中，如果该节点保存的是整数，则
    // 保存在vlong中
    redisAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));

    if (vstr) {
        memcpy(buf,vstr,vlen);
        buf[vlen] = '\0';
        // 将字符串转换为double
        score = strtod(buf,NULL);
    } else {
        score = vlong;
    }

    return score;
}

/* Return a ziplist element as a Redis string object.
 * This simple abstraction can be used to simplifies some code at the
 * cost of some performance. */
/* 	获取sptr指针所指节点的存放的数据，并以Redis对象robj的类型返回。
	实际上就是获取有序集合中某个元素的元素值value域。*/
robj *ziplistGetObject(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    redisAssert(sptr != NULL);
    // 调用ziplistGet获取节点的值，如果该节点保存的是字符串，则存放在vstr中，如果该节点保存的是整数，则
    // 保存在vlong中
    redisAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));

    // 更具不同类型创建Redis对象
    if (vstr) {
        return createStringObject((char*)vstr,vlen);
    } else {
        return createStringObjectFromLongLong(vlong);
    }
}

/* Compare element in sorted set with given element. */
/* 将ziplist中eptr所指节点的元素与cstr字符串进行比较。既然是字符串比较，那比较结果可能是：
	（1）、如果两个相同，返回0
	（2）、如果eptr中的字符串 > cstr字符串，则返回一个正数
	（3）、如果eptr中的字符串 < cstr字符串，则返回一个负数
*/
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    // 调用ziplistGet获取节点的值，如果该节点保存的是字符串，则存放在vstr中，如果该节点保存的是整数，则
    // 保存在vlong中
    redisAssert(ziplistGet(eptr,&vstr,&vlen,&vlong));
    if (vstr == NULL) {
        /* Store string representation of long long in buf. */
        // 如果eptr节点保存的是一个整型，则转换为字符串
        vlen = ll2string((char*)vbuf,sizeof(vbuf),vlong);
        vstr = vbuf;
    }

    minlen = (vlen < clen) ? vlen : clen;
    // 使用memcmp进行字符串比较
    cmp = memcmp(vstr,cstr,minlen);
    if (cmp == 0) return vlen-clen;
    return cmp;
}

/* 返回ziplist编码的有序集合中保存的元素个数，ziplist每两个节点看作有序集合中的一个元素。*/
unsigned int zzlLength(unsigned char *zl) {
    return ziplistLen(zl)/2;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry. */
/*	ziplist编码的有序集合的迭代函数，eptr和sptr是相邻的两个节点，被看做是有序集合中的一个元素，其中
	eptr指向有序集合当前元素的元素值节点，sptr指向相应的分值节点。移动eptr和sptr分别获得下一个元素的元素值节点和分值节点。
	如果后面已经没有元素则返回NULL。*/
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    redisAssert(*eptr != NULL && *sptr != NULL);

    // 获得sptr的下一个节点，即有序集合下一个元素的元素值节点
    _eptr = ziplistNext(zl,*sptr);
    if (_eptr != NULL) {
    	// 获得有序集合下一个元素的分值节点
        _sptr = ziplistNext(zl,_eptr);
        redisAssert(_sptr != NULL);
    } else {
        /* No next entry. */
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no next entry. */
/*	ziplist编码的有序集合的迭代函数，eptr和sptr是相邻的两个节点，被看做是有序集合中的一个元素，其中
	eptr指向有序集合当前元素的元素值节点，sptr指向相应的分值节点。移动eptr和sptr分别获得前一个元素的元素值节点和分值节点。
	如果前面已经没有元素则返回NULL。*/
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    redisAssert(*eptr != NULL && *sptr != NULL);

    // 获得eptr的前一个节点，即分值节点
    _sptr = ziplistPrev(zl,*eptr);
    if (_sptr != NULL) {
    	// 获得_sptr的前一个节点，即元素值节点
        _eptr = ziplistPrev(zl,_sptr);
        redisAssert(_eptr != NULL);
    } else {
        /* No previous entry. */
        _eptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
/*  判断ziplist编码的有序集合是不是有一部分分值score落在range指定的范围内，如果有则返回1，否则返回0。
	注意这是一个内部函数，供zzlFirstInRange和zzlLastInRange函数调用。*/
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* Test for ranges that will always be empty. */
    // 检查range指定的区间是否为空区间
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // ziplist中的分值是有序排列的，最后一个为最大的分值
    p = ziplistIndex(zl,-1); /* Last score. */
    if (p == NULL) return 0; /* Empty sorted set */
   	// 从节点中获取分值score
    score = zzlGetScore(p);
    if (!zslValueGteMin(score,range))
        return 0;

    // 获取最小的一个分值节点
    p = ziplistIndex(zl,1); /* First score. */
    redisAssert(p != NULL);
    // 从节点中获取分值score
    score = zzlGetScore(p);
    if (!zslValueLteMax(score,range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/*  返回ziplist编码的有序集合中第一个分值落在range范围内的元素（即元素值节点指针），
	如果不存在这样的节点则返回NULL。*/
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
	// eptr指向第一个元素（ziplist每两个相连的节点作为有序集合中的一个元素）
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double score;

    /* If everything is out of range, return early. */
    // 先检查有序集合中是否有分值在range指定的区间中，如果没有则直接返回
    if (!zzlIsInRange(zl,range)) return NULL;

    // 从表头到表尾遍历ziplist，两两一组找到第一个符合要求的节点
    while (eptr != NULL) {
    	// 找到当前元素的分值节点
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        // 比较分值
        score = zzlGetScore(sptr);
        if (zslValueGteMin(score,range)) {
            /* Check if score <= max. */
            if (zslValueLteMax(score,range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        // 处理下一个元素
        eptr = ziplistNext(zl,sptr);
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/*  返回ziplist编码的有序集合中最后一个分值落在range范围内的元素（即元素值节点指针），
	如果不存在这样的节点则返回NULL。*/
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range) {
    // eptr指向最后一个元素（ziplist每两个相连的节点作为有序集合中的一个元素）
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;
    double score;

    /* If everything is out of range, return early. */
    // 先检查有序集合中是否有分值在range指定的区间中，如果没有则直接返回
    if (!zzlIsInRange(zl,range)) return NULL;

    // 从后往前遍历，两两一组查找第一个满足要求的节点
    while (eptr != NULL) {
    	// 找到当前元素的分值节点
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        // 比较分值
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* Check if score >= min. */
            if (zslValueGteMin(score,range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        // 获取前一个元素的分值节点，如果ziplistPrev返回NULL说明前面已经没有元素了
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            redisAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/*  以字典序方式判断给ziplist中p节点是否 > 或者 >= spec.min。 
    实际是对zslLexValueGteMin的封装，因为需要先从ziplist节点中提取数据。*/
static int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec) {
    // 从p节点中获取其保存的数据对象
    robj *value = ziplistGetObject(p);
    // 调用zslLexValueGteMin进行比较
    int res = zslLexValueGteMin(value,spec);
    decrRefCount(value);
    return res;
}

/*  以字典序方式判断给ziplist中p节点是否 < 或者 <= spec.max。 
    实际是对zslLexValueLteMax的封装，因为需要先从ziplist节点中提取数据。*/
static int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec) {
    // 从p节点中获取其保存的数据对象
    robj *value = ziplistGetObject(p);
    // 调用zslLexValueLteMax进行比较
    int res = zslLexValueLteMax(value,spec);
    decrRefCount(value);
    return res;
}

/*  当插入到有序集合中的所有元素的分值score都相同时，存储有序集合中的元素值是按字典序排序（Lexicographical ordering）的。*/

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
/*  按字典序判断ziplist编码的有序集合中是否有元素落在指定字典序区间内。
    该函数主要作为内部函数供zzlFirstInRange和zzlLastInRange函数使用。*/
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *p;

    /* Test for ranges that will always be empty. */
    // 检查字典序区间是否合法
    if (compareStringObjectsForLexRange(range->min,range->max) > 1 ||
            (compareStringObjects(range->min,range->max) == 0 &&
            (range->minex || range->maxex)))
        return 0;

    // 比较最后一个元素
    p = ziplistIndex(zl,-2); /* Last element. */
    if (p == NULL) return 0;
    if (!zzlLexValueGteMin(p,range))
        return 0;

    // 比较第一个元素
    p = ziplistIndex(zl,0); /* First element. */
    redisAssert(p != NULL);
    if (!zzlLexValueLteMax(p,range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
/*  查找ziplist编码的有序集合中落在指定字典序区间内的第一个元素。
    如果不存在这样的节点则返回NULL。*/
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range) {
    // 获取ziplist中的第一个节点
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    /* If everything is out of range, return early. */
    // 如果没有元素在指定的字典序区间，马上返回
    if (!zzlIsInLexRange(zl,range)) return NULL;

    // 从前往后，两两一组遍历ziplist查找满足要求的第一个节点
    while (eptr != NULL) {
        if (zzlLexValueGteMin(eptr,range)) {
            /* Check if score <= max. */
            if (zzlLexValueLteMax(eptr,range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        // 处理下一个元素（ziplist中相邻的两个节点看做是有序集合中的一个元素），eptr指向元素值节点，sptr指向分值节点
        sptr = ziplistNext(zl,eptr); /* This element score. Skip it. */
        redisAssert(sptr != NULL);
        eptr = ziplistNext(zl,sptr); /* Next element. */
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
/*  查找ziplist编码的有序集合中落在指定字典序区间内的最后一个元素。
    如果不存在这样的节点则返回NULL。*/
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range) {
    // 获取ziplist中倒数第二个节点（即有序集合中最后一个元素）
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;

    /* If everything is out of range, return early. */
    // 如果没有元素在指定的字典序区间，马上返回
    if (!zzlIsInLexRange(zl,range)) return NULL;

    // 从后往前遍历，两两一组查找满足要求的第一个节点（同时也是正序最后一个满足要求的节点）
    while (eptr != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Check if score >= min. */
            if (zzlLexValueGteMin(eptr,range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        // 处理前一个元素
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            redisAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/*  从ziplis编码的有序集合中查找ele元素，并将其分值保存在score中。
    如果操作成功则返回指向该元素的指针，否则返回NULL。*/
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score) {
    // 获取ziplist中的第一个节点指针
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    // 对参数ele解码
    ele = getDecodedObject(ele);
    // 从前往后遍历ziplist，查找目标元素
    while (eptr != NULL) {
        // 获取分值节点
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,ele,sptr != NULL);

        // 对比
        if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr))) {
            /* Matching element, pull out score. */
            // 匹配成功，取出分值保存在score中
            if (score != NULL) *score = zzlGetScore(sptr);
            decrRefCount(ele);
            return eptr;
        }

        /* Move to next element. */
        // 处理下一个元素
        eptr = ziplistNext(zl,sptr);
    }

    decrRefCount(ele);
    return NULL;
}

/* Delete (element,score) pair from ziplist. Use local copy of eptr because we
 * don't want to modify the one given as argument. */
/*  从ziplist编码的有序集合中删除一个元素，反映在ziplist中就是删除元素值节点和相邻的分值节点。
    删除节点后返回ziplist的首地址（可能会引起空间重新分配）*/
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    unsigned char *p = eptr;

    /* TODO: add function to ziplist API to delete N elements from offset. */
    zl = ziplistDelete(zl,&p);
    zl = ziplistDelete(zl,&p);
    return zl;
}

/*  往eptr节点前面插入一个元素值节点和分值节点，如果eptr为空，则将两个新节点添加到ziplist尾部。
    操作成功后返回ziplist的首地址。*/
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, robj *ele, double score) {
    unsigned char *sptr;
    char scorebuf[128];
    int scorelen;
    size_t offset;

    redisAssertWithInfo(NULL,ele,ele->encoding == REDIS_ENCODING_RAW);
    // 将double类型的分值转换为字符串
    scorelen = d2string(scorebuf,sizeof(scorebuf),score);
    // 如果eptr为空，则将元素值节点和分值节点插入到ziplist尾部
    if (eptr == NULL) {
        // 先添加元素值节点
        zl = ziplistPush(zl,ele->ptr,sdslen(ele->ptr),ZIPLIST_TAIL);
        // 再添加分值节点
        zl = ziplistPush(zl,(unsigned char*)scorebuf,scorelen,ZIPLIST_TAIL);
    } 
    // 插入到某个节点前面，实际上通过ziplist的内置函数实现
    else {
        /* Keep offset relative to zl, as it might be re-allocated. */
        // 插入元素值节点
        offset = eptr-zl;
        zl = ziplistInsert(zl,eptr,ele->ptr,sdslen(ele->ptr));
        eptr = zl+offset;

        /* Insert score after the element. */
        redisAssertWithInfo(NULL,ele,(sptr = ziplistNext(zl,eptr)) != NULL);
        // 插入分值节点
        zl = ziplistInsert(zl,sptr,(unsigned char*)scorebuf,scorelen);
    }

    return zl;
}

/* Insert (element,score) pair in ziplist. This function assumes the element is
 * not yet present in the list. */
/* 将元素值节点和分值节点插入到ziplist中。该函数假设ele对象不在列表中。*/
unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score) {
    // 获取ziplist的第一个节点（即有序集合中的第一个元素）
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double s;

    // 对ele进行解码
    ele = getDecodedObject(ele);
    // ziplist中的元素是按分值排序的，这里需要遍历查找插入位置
    while (eptr != NULL) {
        // 两两一组
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,ele,sptr != NULL);
        s = zzlGetScore(sptr);

        if (s > score) {
            /* First element with score larger than score for element to be
             * inserted. This means we should take its spot in the list to
             * maintain ordering. */
            /* 找到第一个分值比输入分值score大的节点，则需要将新节点插入到该节点前面，以维持有序集合的有序性。*/
            zl = zzlInsertAt(zl,eptr,ele,score);
            // 插入完成后直接返回
            break;
        } else if (s == score) {
            /* Ensure lexicographical ordering for elements. */
            // 分值相同的节点按成员字符串字典序排序
            if (zzlCompareElements(eptr,ele->ptr,sdslen(ele->ptr)) > 0) {
                zl = zzlInsertAt(zl,eptr,ele,score);
                break;
            }
        }

        /* Move to next element. */
        // 处理下一个元素
        eptr = ziplistNext(zl,sptr);
    }

    /* Push on tail of list when it was not yet inserted. */
    // 如果ziplist为空，则直接添加到尾部
    if (eptr == NULL)
        zl = zzlInsertAt(zl,NULL,ele,score);

    decrRefCount(ele);
    return zl;
}

/* 删除ziplist编码的有序集合中分值在指定范围的元素，将删除元素个数保存在deleted参数中。*/
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    // 指向ziplist中分值落在指定范围的第一个节点
    eptr = zzlFirstInRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    // 一直删除节点一直遇到不在range指定范围内的节点为止
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* Delete both the element and the score. */
            zl = ziplistDelete(zl,&eptr);
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* 删除ziplist编码的有序集合中元素值字符串在指定字典序区间的元素，将删除元素个数保存在deleted参数中。*/
unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    // 指向ziplist中元素值字符串落在指定字典序范围的第一个节点
    eptr = zzlFirstInLexRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    // 一直删除节点一直遇到不在range指定范围内的节点为止
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Delete both the element and the score. */
            zl = ziplistDelete(zl,&eptr);
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
/*  删除ziplist编码的有序集合中在指定排位范围内的所有元素。
    其中参数start和end指定的排位都包含在内，而且都是从1开始计算的。
    参数deleted用来保存删除的元素个数。*/
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    unsigned int num = (end-start)+1;
    if (deleted) *deleted = num;
    // ziplist中两两相邻的节点看做是有序集合的一个元素，所以实际删除的元素个数需要乘以2
    // ziplist中的节点从0开始计算，有序集合的排位从1开始计算，所以参数start需要减1
    zl = ziplistDeleteRange(zl,2*(start-1),2*num);
    return zl;
}

/*-----------------------------------------------------------------------------
 * Common sorted set API    zset公共接口
 *----------------------------------------------------------------------------*/

 /* 下面的函数实际是一组统一的对外接口，用来屏蔽ziplist编码和skiplist的差异。*/

/* 获得有序集合的长度（节点个数） */
unsigned int zsetLength(robj *zobj) {
    int length = -1;
    // 处理ziplist编码的情况
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        length = zzlLength(zobj->ptr);
    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        length = ((zset*)zobj->ptr)->zsl->length;
    } else {
        redisPanic("Unknown sorted set encoding");
    }
    return length;
}

/* 将zset对象的编码转换为参数encoding指定的编码方式。 */
void zsetConvert(robj *zobj, int encoding) {
    zset *zs;
    zskiplistNode *node, *next;
    robj *ele;
    double score;

    // 如果zset当前的编码已经是指定的编码，则返回
    if (zobj->encoding == encoding) return;

    // 如果当前编码为ziplist，目标编码只能是skiplist
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        // 如果目标编码不是skiplist则出错
        if (encoding != REDIS_ENCODING_SKIPLIST)
            redisPanic("Unknown target encoding");

        // 创建有序集合结构体
        zs = zmalloc(sizeof(*zs));
        // 创建dict成员
        zs->dict = dictCreate(&zsetDictType,NULL);
        // 创建zskiplist成员
        zs->zsl = zslCreate();

        // ziplist编码的有序集合将ziplist的每两个节点看做一个元素
        // 获取ziplist编码的有序集合的第一个元素，其中eptr之元素值节点，sptr为分值节点
        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(NULL,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,zobj,sptr != NULL);

        // 遍历ziplist的所有节点，两两一组并添加到skiplist编码的新集合中
        while (eptr != NULL) {
            // 获取分值节点
            score = zzlGetScore(sptr);
            // 取出元素值，如果是字符串编码则保存在vstr中，如果是整型编码则保存在vlong中
            redisAssertWithInfo(NULL,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                ele = createStringObjectFromLongLong(vlong);
            else
                ele = createStringObject((char*)vstr,vlen);

            /* Has incremented refcount since it was just created. */
            // 添加到跳跃表中
            node = zslInsert(zs->zsl,score,ele);
            // 添加到字典dict中
            redisAssertWithInfo(NULL,zobj,dictAdd(zs->dict,ele,&node->score) == DICT_OK);
            incrRefCount(ele); /* Added to dictionary. */
            // 处理下一个元素
            zzlNext(zl,&eptr,&sptr);
        }

        // 释放源对象的ziplist成员
        zfree(zobj->ptr);
        // 更新原对象的ptr指针和编码方式
        zobj->ptr = zs;
        zobj->encoding = REDIS_ENCODING_SKIPLIST;
    } 
    // 如果当前编码为skiplist，目标编码只能是ziplist
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        // 创建一个ziplist作为底层结构
        unsigned char *zl = ziplistNew();

        // 如果目标编码不是ziplist，则出错
        if (encoding != REDIS_ENCODING_ZIPLIST)
            redisPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the ziplist. */
        // 获取有序集合
        zs = zobj->ptr;
        // 释放原对象的字典成员，该成员只是为了快速定位元素值对应的分值score。后面的操作不需要用到，先删除
        dictRelease(zs->dict);
        // 获取跳跃表的第一个节点
        node = zs->zsl->header->level[0].forward;
        // 释放跳跃表的头结点
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        // 遍历跳跃表，对每个元素，分解出元素值和分值并添加到ziplist中
        while (node) {
            // 对元素值对象解码
            ele = getDecodedObject(node->obj);
            // 往ziplist添加一个新元素（插入元素值节点和分值节点）
            zl = zzlInsertAt(zl,NULL,ele,node->score);
            decrRefCount(ele);

            // 记录下一个节点
            next = node->level[0].forward;
            // 释放当前节点
            zslFreeNode(node);
            // 处理下一个节点
            node = next;
        }

        // 更新信息
        zfree(zs);
        zobj->ptr = zl;
        zobj->encoding = REDIS_ENCODING_ZIPLIST;
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Sorted set commands  zset命令实现
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY. */
/* ZADD和ZINCRBY的底层实现 */
void zaddGenericCommand(redisClient *c, int incr) {
    // 出错信息
    static char *nanerr = "resulting score is not a number (NaN)";

    // zset对象的key
    robj *key = c->argv[1];
    robj *ele;
    robj *zobj;
    robj *curobj;
    double score = 0, *scores = NULL, curscore = 0.0;
    // 获取需要添加的元素个数
    int j, elements = (c->argc-2)/2;
    int added = 0, updated = 0;

    // 输入的元素值和分数必须成对出现，如果不是则出错
    if (c->argc % 2) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    // 提取出所有的分值score，为了确保一致性，插入操作要么全部执行要么全部不执行，所以先提取出score以判断输入命令是否有语法错误
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if (getDoubleFromObjectOrReply(c,c->argv[2+j*2],&scores[j],NULL)
            != REDIS_OK) goto cleanup;
    }

    /* Lookup the key and create the sorted set if does not exist. */
    // 取出有序集合对象
    zobj = lookupKeyWrite(c->db,key);
    // 如果key指定的有序集合对象不存在则创建一个
    if (zobj == NULL) {
        // server.zset_max_ziplist_entries的默认值为128
        // server.zset_max_ziplist_value的默认值为64
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(c->argv[3]->ptr))
        {
            zobj = createZsetObject();
        } else {
            zobj = createZsetZiplistObject();
        }
        dbAdd(c->db,key,zobj);
    } 
    // 如果key指定的对象存在，还需要进一步检查其类型是否是zset
    else {

        if (zobj->type != REDIS_ZSET) {
            addReply(c,shared.wrongtypeerr);
            goto cleanup;
        }
    }

    // 处理所有的输入value和相应的分值
    for (j = 0; j < elements; j++) {
        score = scores[j];

        // 处理ziplist编码的情况
        if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
            unsigned char *eptr;

            /* Prefer non-encoded element when dealing with ziplists. */
            // 获取输入元素值
            ele = c->argv[3+j*2];
            // 检查该成员是否已经存在
            if ((eptr = zzlFind(zobj->ptr,ele,&curscore)) != NULL) {
                // 对于ZINCRBY命令，如果指定的元素已经存在则对其分值进行增加操作
                if (incr) {
                    score += curscore;
                    if (isnan(score)) {
                        addReplyError(c,nanerr);
                        goto cleanup;
                    }
                }

                /* Remove and re-insert when score changed. */
                // ZADD和ZINCRBY命令如果引起某元素对应的分值发生改变，则该元素的位置也要发生改变以维持集合的有序性，下面的操作就是完成这个功能
                // 对于ZADD命令，如果指定的元素已经存在则更新该元素的分数
                // 对于ZINCRBY命令，分值改变也需要执行下列函数
                if (score != curscore) {
                    // 先把已有元素删除，然后再重新插入新元素，因为分值不同则其在ziplist的位置也不同
                    zobj->ptr = zzlDelete(zobj->ptr,eptr);
                    zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                    server.dirty++;
                    updated++;
                }
            } 
            // 如果待插入元素并不存在在有序集合中，则直接插入
            else {
                /* Optimize: check if the element is too large or the list
                 * becomes too long *before* executing zzlInsert. */
                // 执行插入操作
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                // 检查有序集合中元素个数，如果超过server.zset_max_ziplist_entries则转换为skiplist编码方式
                if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries)
                    zsetConvert(zobj,REDIS_ENCODING_SKIPLIST);
                // 检查新添加元素长度，如果超过server.zset_max_ziplist_value则转换为ziplist编码方式
                if (sdslen(ele->ptr) > server.zset_max_ziplist_value)
                    zsetConvert(zobj,REDIS_ENCODING_SKIPLIST);
                server.dirty++;
                added++;
            }
        } 
        // 处理skiplist编码的情况
        else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplistNode *znode;
            dictEntry *de;

            // 对新元素值进行编码
            ele = c->argv[3+j*2] = tryObjectEncoding(c->argv[3+j*2]);
            // 检查新元素是否已经存在于有序集合中
            de = dictFind(zs->dict,ele);
            // 如果待插入元素已经存在
            if (de != NULL) {
                // 取出元素值
                curobj = dictGetKey(de);
                // 取出分值
                curscore = *(double*)dictGetVal(de);

                // 对于ZINCRBY命令，如果指定的元素已经存在则对其分值进行增加操作
                if (incr) {
                    score += curscore;
                    if (isnan(score)) {
                        addReplyError(c,nanerr);
                        /* Don't need to check if the sorted set is empty
                         * because we know it has at least one element. */
                        goto cleanup;
                    }
                }

                /* Remove and re-insert when score changed. We can safely
                 * delete the key object from the skiplist, since the
                 * dictionary still has a reference to it. */
                // ZADD和ZINCRBY命令如果引起某元素对应的分值发生改变，则该元素的位置也要发生改变以维持集合的有序性，下面的操作就是完成这个功能
                // 对于ZADD命令，如果指定的元素已经存在则更新该元素的分数
                // 对于ZINCRBY命令，分值改变也需要执行下列函数
                if (score != curscore) {
                    // 在skiplist中删除原有成员，但是有序集合中dict中仍然存在
                    redisAssertWithInfo(c,curobj,zslDelete(zs->zsl,curscore,curobj));
                    // 往skiplist中插入新元素
                    znode = zslInsert(zs->zsl,score,curobj);
                    incrRefCount(curobj); /* Re-inserted in skiplist. */
                    // 更新分值
                    dictGetVal(de) = &znode->score; /* Update score ptr. */
                    server.dirty++;
                    updated++;
                }
            } 
            // 如果待插入元素并不存在于有序集合中，直接插入
            else {
                // 将新元素插入跳跃表中
                znode = zslInsert(zs->zsl,score,ele);
                incrRefCount(ele); /* Inserted in skiplist. */
                // 将新元素及其分值更新到字典dict
                redisAssertWithInfo(c,NULL,dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
                incrRefCount(ele); /* Added to dictionary. */
                server.dirty++;
                added++;
            }
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    }
    if (incr) /* ZINCRBY */
        addReplyDouble(c,score);
    else /* ZADD */
        addReplyLongLong(c,added);

// 清理操作
cleanup:
    zfree(scores);
    if (added || updated) {
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,
            incr ? "zincr" : "zadd", key, c->db->id);
    }
}

/* zadd命令实现 */
void zaddCommand(redisClient *c) {
    zaddGenericCommand(c,0);
}

/* zincrby命令实现 */
void zincrbyCommand(redisClient *c) {
    zaddGenericCommand(c,1);
}

/* zrem删除元素命令实现 */
void zremCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    // 取出有序集合对象并检查其类型
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // 处理ziplist编码的情况
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        // 从第三个开始遍历所有的输入元素值，即要删除的元素值
        for (j = 2; j < c->argc; j++) {
            // 检查该元素是否存在于ziplist中
            if ((eptr = zzlFind(zobj->ptr,c->argv[j],NULL)) != NULL) {
                // 记录删除元素个数
                deleted++;
                // 执行真正的删除操作
                zobj->ptr = zzlDelete(zobj->ptr,eptr);
                // 如果ziplist已经为空，则将有序集合从db中删除
                if (zzlLength(zobj->ptr) == 0) {
                    dbDelete(c->db,key);
                    keyremoved = 1;
                    break;
                }
            }
        }
    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de;
        double score;

        // 从第三个开始遍历所有的输入元素值，即要删除的元素值
        for (j = 2; j < c->argc; j++) {
            // 检查该元素是否已经存在于有序集合中
            de = dictFind(zs->dict,c->argv[j]);
            if (de != NULL) {
                // 如果该元素已经存在，记录删除元素个数
                deleted++;

                /* Delete from the skiplist */
                // 从跳跃表中将该元素删除
                score = *(double*)dictGetVal(de);
                redisAssertWithInfo(c,c->argv[j],zslDelete(zs->zsl,score,c->argv[j]));

                /* Delete from the hash table */
                // 从字典中将该元素删除
                dictDelete(zs->dict,c->argv[j]);
                if (htNeedsResize(zs->dict)) dictResize(zs->dict);
                // 如果字典中的已经没有元素，说明有序集合中也没有元素，将有序集合从db中删除
                if (dictSize(zs->dict) == 0) {
                    dbDelete(c->db,key);
                    keyremoved = 1;
                    break;
                }
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    // 如果有元素被删除，发送通知
    if (deleted) {
        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,"zrem",key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);
        signalModifiedKey(c->db,key);
        server.dirty += deleted;
    }
    // 回复被删除元素个数
    addReplyLongLong(c,deleted);
}

/* Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX commands. */
#define ZRANGE_RANK 0
#define ZRANGE_SCORE 1
#define ZRANGE_LEX 2
/* ZREMRANGEBYRANK、ZREMRANGEBYSCORE和ZREMRANGEBYLEX命令的底层实现，参数rangetype指明具体的命令 */
void zremrangeGenericCommand(redisClient *c, int rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted;
    // 记录分值区间
    zrangespec range;
    // 记录字典序区间
    zlexrangespec lexrange;
    // 记录排位区间（分别存放在start和end中）
    long start, end, llen;

    /* Step 1: Parse the range. */
    // 步骤1：解析出命令相关的范围区间
    if (rangetype == ZRANGE_RANK) {
        // 排位区间
        if ((getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != REDIS_OK) ||
            (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != REDIS_OK))
            return;
    } else if (rangetype == ZRANGE_SCORE) {
        // 分值区间
        if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
            addReplyError(c,"min or max is not a float");
            return;
        }
    } else if (rangetype == ZRANGE_LEX) {
        // 字典序区间
        if (zslParseLexRange(c->argv[2],c->argv[3],&lexrange) != REDIS_OK) {
            addReplyError(c,"min or max not valid string range item");
            return;
        }
    }

    /* Step 2: Lookup & range  checks if needed. */
    // 步骤3：获取有序集合对象 & 验证区间范围

    // 获取有序集合对象并检查其类型
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) goto cleanup;

    if (rangetype == ZRANGE_RANK) {
        // 将负数索引转换为整数索引
        /* Sanitize indexes. */
        llen = zsetLength(zobj);
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        // 验证、调整排位区间的合法性
        if (start > end || start >= llen) {
            addReply(c,shared.czero);
            goto cleanup;
        }
        if (end >= llen) end = llen-1;
    }

    /* Step 3: Perform the range deletion operation. */
    // 步骤3：调用相关函数处理删除操作，这些函数我们都在前面介绍过

    // 处理ziplist编码的形式
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        switch(rangetype) {
        case ZRANGE_RANK:
            zobj->ptr = zzlDeleteRangeByRank(zobj->ptr,start+1,end+1,&deleted);
            break;
        case ZRANGE_SCORE:
            zobj->ptr = zzlDeleteRangeByScore(zobj->ptr,&range,&deleted);
            break;
        case ZRANGE_LEX:
            zobj->ptr = zzlDeleteRangeByLex(zobj->ptr,&lexrange,&deleted);
            break;
        }

        // 经过前面的操作后，如果有序集合中没有元素则将有序集合从db中删除
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        switch(rangetype) {
        case ZRANGE_RANK:
            deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
            break;
        case ZRANGE_SCORE:
            deleted = zslDeleteRangeByScore(zs->zsl,&range,zs->dict);
            break;
        case ZRANGE_LEX:
            deleted = zslDeleteRangeByLex(zs->zsl,&lexrange,zs->dict);
            break;
        }
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        // 经过前面的操作后，如果有序集合中没有元素则将有序集合从db中删除
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    /* Step 4: Notifications and reply. */
    // 步骤4：通知和回复
    if (deleted) {
        char *event[3] = {"zremrangebyrank","zremrangebyscore","zremrangebylex"};
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,event[rangetype],key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);
    }
    server.dirty += deleted;
    addReplyLongLong(c,deleted);

cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
}

/* zremrangebyrank命令的实现 */
void zremrangebyrankCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_RANK);
}

/* zremrangebyscore命令实现 */
void zremrangebyscoreCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_SCORE);
}

/* zremrangebylex命令实现 */
void zremrangebylexCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_LEX);
}

/* 集合迭代器结构体，用来迭代集合set和有序集合zset*/
typedef struct {
    // 被迭代的对象
    robj *subject;
    // 迭代对象类型
    int type; /* Set, sorted set */
    // 迭代对象的编码方式
    int encoding;
    double weight;

    // 注意下面的是union联合体，节省空间
    union {
        /* Set iterators. */
        // 集合set迭代器（里面的ii和ht只能取其一）
        union _iterset {
            // 对应intset编码方式
            struct {
                intset *is;
                int ii;
            } is;
            // 对应dict编码方式
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        // 有序集合zset迭代器
        union _iterzset {
            // 对应ziplist编码方式
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            // 对应skiplist编码方式
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;


/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. The dirty flag for the long long value is
 * special, since long long values don't need cleanup. Instead, it means that
 * we already checked that "ell" holds a long long, or tried to convert another
 * representation into a long long value. When this was successful,
 * OPVAL_VALID_LL is set as well. */
#define OPVAL_DIRTY_ROBJ 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* Private buffer. */
    robj *ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    double score;
} zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;

void zuiInitIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == REDIS_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == REDIS_ENCODING_INTSET) {
            it->is.is = op->subject->ptr;
            it->is.ii = 0;
        } else if (op->encoding == REDIS_ENCODING_HT) {
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = ziplistIndex(it->zl.zl,0);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = ziplistNext(it->zl.zl,it->zl.eptr);
                redisAssert(it->zl.sptr != NULL);
            }
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->header->level[0].forward;
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

void zuiClearIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == REDIS_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == REDIS_ENCODING_INTSET) {
            REDIS_NOTUSED(it); /* skip */
        } else if (op->encoding == REDIS_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            REDIS_NOTUSED(it); /* skip */
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            REDIS_NOTUSED(it); /* skip */
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

int zuiLength(zsetopsrc *op) {
    if (op->subject == NULL)
        return 0;

    if (op->type == REDIS_SET) {
        if (op->encoding == REDIS_ENCODING_INTSET) {
            return intsetLen(op->subject->ptr);
        } else if (op->encoding == REDIS_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            return dictSize(ht);
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            return zzlLength(op->subject->ptr);
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            return zs->zsl->length;
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort. */
int zuiNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL)
        return 0;

    if (val->flags & OPVAL_DIRTY_ROBJ)
        decrRefCount(val->ele);

    memset(val,0,sizeof(zsetopval));

    if (op->type == REDIS_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == REDIS_ENCODING_INTSET) {
            int64_t ell;

            if (!intsetGet(it->is.is,it->is.ii,&ell))
                return 0;
            val->ell = ell;
            val->score = 1.0;

            /* Move to next element. */
            it->is.ii++;
        } else if (op->encoding == REDIS_ENCODING_HT) {
            if (it->ht.de == NULL)
                return 0;
            val->ele = dictGetKey(it->ht.de);
            val->score = 1.0;

            /* Move to next element. */
            it->ht.de = dictNext(it->ht.di);
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            /* No need to check both, but better be explicit. */
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;
            redisAssert(ziplistGet(it->zl.eptr,&val->estr,&val->elen,&val->ell));
            val->score = zzlGetScore(it->zl.sptr);

            /* Move to next element. */
            zzlNext(it->zl.zl,&it->zl.eptr,&it->zl.sptr);
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            if (it->sl.node == NULL)
                return 0;
            val->ele = it->sl.node->obj;
            val->score = it->sl.node->score;

            /* Move to next element. */
            it->sl.node = it->sl.node->level[0].forward;
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
    return 1;
}

int zuiLongLongFromValue(zsetopval *val) {
    if (!(val->flags & OPVAL_DIRTY_LL)) {
        val->flags |= OPVAL_DIRTY_LL;

        if (val->ele != NULL) {
            if (val->ele->encoding == REDIS_ENCODING_INT) {
                val->ell = (long)val->ele->ptr;
                val->flags |= OPVAL_VALID_LL;
            } else if (val->ele->encoding == REDIS_ENCODING_RAW) {
                if (string2ll(val->ele->ptr,sdslen(val->ele->ptr),&val->ell))
                    val->flags |= OPVAL_VALID_LL;
            } else {
                redisPanic("Unsupported element encoding");
            }
        } else if (val->estr != NULL) {
            if (string2ll((char*)val->estr,val->elen,&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else {
            /* The long long was already set, flag as valid. */
            val->flags |= OPVAL_VALID_LL;
        }
    }
    return val->flags & OPVAL_VALID_LL;
}

robj *zuiObjectFromValue(zsetopval *val) {
    if (val->ele == NULL) {
        if (val->estr != NULL) {
            val->ele = createStringObject((char*)val->estr,val->elen);
        } else {
            val->ele = createStringObjectFromLongLong(val->ell);
        }
        val->flags |= OPVAL_DIRTY_ROBJ;
    }
    return val->ele;
}

int zuiBufferFromValue(zsetopval *val) {
    if (val->estr == NULL) {
        if (val->ele != NULL) {
            if (val->ele->encoding == REDIS_ENCODING_INT) {
                val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),(long)val->ele->ptr);
                val->estr = val->_buf;
            } else if (val->ele->encoding == REDIS_ENCODING_RAW) {
                val->elen = sdslen(val->ele->ptr);
                val->estr = val->ele->ptr;
            } else {
                redisPanic("Unsupported element encoding");
            }
        } else {
            val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),val->ell);
            val->estr = val->_buf;
        }
    }
    return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise. */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {
    if (op->subject == NULL)
        return 0;

    if (op->type == REDIS_SET) {
        if (op->encoding == REDIS_ENCODING_INTSET) {
            if (zuiLongLongFromValue(val) &&
                intsetFind(op->subject->ptr,val->ell))
            {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == REDIS_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            zuiObjectFromValue(val);
            if (dictFind(ht,val->ele) != NULL) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        zuiObjectFromValue(val);

        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            if (zzlFind(op->subject->ptr,val->ele,score) != NULL) {
                /* Score is already set by zzlFind. */
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            dictEntry *de;
            if ((de = dictFind(zs->dict,val->ele)) != NULL) {
                *score = *(double*)dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

int zuiCompareByCardinality(const void *s1, const void *s2) {
    return zuiLength((zsetopsrc*)s1) - zuiLength((zsetopsrc*)s2);
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

inline static void zunionInterAggregate(double *target, double val, int aggregate) {
    if (aggregate == REDIS_AGGR_SUM) {
        *target = *target + val;
        /* The result of adding two doubles is NaN when one variable
         * is +inf and the other is -inf. When these numbers are added,
         * we maintain the convention of the result being 0.0. */
        if (isnan(*target)) *target = 0.0;
    } else if (aggregate == REDIS_AGGR_MIN) {
        *target = val < *target ? val : *target;
    } else if (aggregate == REDIS_AGGR_MAX) {
        *target = val > *target ? val : *target;
    } else {
        /* safety net */
        redisPanic("Unknown ZUNION/INTER aggregate type");
    }
}

void zunionInterGenericCommand(redisClient *c, robj *dstkey, int op) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    robj *tmp;
    unsigned int maxelelen = 0;
    robj *dstobj;
    zset *dstzset;
    zskiplistNode *znode;
    int touched = 0;

    /* expect setnum input keys to be given */
    if ((getLongFromObjectOrReply(c, c->argv[2], &setnum, NULL) != REDIS_OK))
        return;

    if (setnum < 1) {
        addReplyError(c,
            "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
        return;
    }

    /* test if the expected number of keys would overflow */
    if (setnum > c->argc-3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* read keys to be used for input */
    src = zcalloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = 3; i < setnum; i++, j++) {
        robj *obj = lookupKeyWrite(c->db,c->argv[j]);
        if (obj != NULL) {
            if (obj->type != REDIS_ZSET && obj->type != REDIS_SET) {
                zfree(src);
                addReply(c,shared.wrongtypeerr);
                return;
            }

            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;
        } else {
            src[i].subject = NULL;
        }

        /* Default all weights to 1. */
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    if (j < c->argc) {
        int remaining = c->argc - j;

        while (remaining) {
            if (remaining >= (setnum + 1) && !strcasecmp(c->argv[j]->ptr,"weights")) {
                j++; remaining--;
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (getDoubleFromObjectOrReply(c,c->argv[j],&src[i].weight,
                            "weight value is not a float") != REDIS_OK)
                    {
                        zfree(src);
                        return;
                    }
                }
            } else if (remaining >= 2 && !strcasecmp(c->argv[j]->ptr,"aggregate")) {
                j++; remaining--;
                if (!strcasecmp(c->argv[j]->ptr,"sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr,"min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr,"max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReply(c,shared.syntaxerr);
                    return;
                }
                j++; remaining--;
            } else {
                zfree(src);
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    qsort(src,setnum,sizeof(zsetopsrc),zuiCompareByCardinality);

    dstobj = createZsetObject();
    dstzset = dstobj->ptr;
    memset(&zval, 0, sizeof(zval));

    if (op == REDIS_OP_INTER) {
        /* Skip everything if the smallest input is empty. */
        if (zuiLength(&src[0]) > 0) {
            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */
            zuiInitIterator(&src[0]);
            while (zuiNext(&src[0],&zval)) {
                double score, value;

                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    if (src[j].subject == src[0].subject) {
                        value = zval.score*src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                if (j == setnum) {
                    tmp = zuiObjectFromValue(&zval);
                    znode = zslInsert(dstzset->zsl,score,tmp);
                    incrRefCount(tmp); /* added to skiplist */
                    dictAdd(dstzset->dict,tmp,&znode->score);
                    incrRefCount(tmp); /* added to dictionary */

                    if (tmp->encoding == REDIS_ENCODING_RAW)
                        if (sdslen(tmp->ptr) > maxelelen)
                            maxelelen = sdslen(tmp->ptr);
                }
            }
            zuiClearIterator(&src[0]);
        }
    } else if (op == REDIS_OP_UNION) {
        dict *accumulator = dictCreate(&setDictType,NULL);
        dictIterator *di;
        dictEntry *de;
        double score;

        if (setnum) {
            /* Our union is at least as large as the largest set.
             * Resize the dictionary ASAP to avoid useless rehashing. */
            dictExpand(accumulator,zuiLength(&src[setnum-1]));
        }

        /* Step 1: Create a dictionary of elements -> aggregated-scores
         * by iterating one sorted set after the other. */
        for (i = 0; i < setnum; i++) {
            if (zuiLength(&src[i]) == 0) continue;

            zuiInitIterator(&src[i]);
            while (zuiNext(&src[i],&zval)) {
                /* Initialize value */
                score = src[i].weight * zval.score;
                if (isnan(score)) score = 0;

                /* Search for this element in the accumulating dictionary. */
                de = dictFind(accumulator,zuiObjectFromValue(&zval));
                /* If we don't have it, we need to create a new entry. */
                if (de == NULL) {
                    tmp = zuiObjectFromValue(&zval);
                    /* Remember the longest single element encountered,
                     * to understand if it's possible to convert to ziplist
                     * at the end. */
                    if (tmp->encoding == REDIS_ENCODING_RAW) {
                        if (sdslen(tmp->ptr) > maxelelen)
                            maxelelen = sdslen(tmp->ptr);
                    }
                    /* Add the element with its initial score. */
                    de = dictAddRaw(accumulator,tmp);
                    incrRefCount(tmp);
                    dictSetDoubleVal(de,score);
                } else {
                    /* Update the score with the score of the new instance
                     * of the element found in the current sorted set.
                     *
                     * Here we access directly the dictEntry double
                     * value inside the union as it is a big speedup
                     * compared to using the getDouble/setDouble API. */
                    zunionInterAggregate(&de->v.d,score,aggregate);
                }
            }
            zuiClearIterator(&src[i]);
        }

        /* Step 2: convert the dictionary into the final sorted set. */
        di = dictGetIterator(accumulator);

        /* We now are aware of the final size of the resulting sorted set,
         * let's resize the dictionary embedded inside the sorted set to the
         * right size, in order to save rehashing time. */
        dictExpand(dstzset->dict,dictSize(accumulator));

        while((de = dictNext(di)) != NULL) {
            robj *ele = dictGetKey(de);
            score = dictGetDoubleVal(de);
            znode = zslInsert(dstzset->zsl,score,ele);
            incrRefCount(ele); /* added to skiplist */
            dictAdd(dstzset->dict,ele,&znode->score);
            incrRefCount(ele); /* added to dictionary */
        }
        dictReleaseIterator(di);

        /* We can free the accumulator dictionary now. */
        dictRelease(accumulator);
    } else {
        redisPanic("Unknown operator");
    }

    if (dbDelete(c->db,dstkey)) {
        signalModifiedKey(c->db,dstkey);
        touched = 1;
        server.dirty++;
    }
    if (dstzset->zsl->length) {
        /* Convert to ziplist when in limits. */
        if (dstzset->zsl->length <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
                zsetConvert(dstobj,REDIS_ENCODING_ZIPLIST);

        dbAdd(c->db,dstkey,dstobj);
        addReplyLongLong(c,zsetLength(dstobj));
        if (!touched) signalModifiedKey(c->db,dstkey);
        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,
            (op == REDIS_OP_UNION) ? "zunionstore" : "zinterstore",
            dstkey,c->db->id);
        server.dirty++;
    } else {
        decrRefCount(dstobj);
        addReply(c,shared.czero);
        if (touched)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",dstkey,c->db->id);
    }
    zfree(src);
}

void zunionstoreCommand(redisClient *c) {
    zunionInterGenericCommand(c,c->argv[1], REDIS_OP_UNION);
}

void zinterstoreCommand(redisClient *c) {
    zunionInterGenericCommand(c,c->argv[1], REDIS_OP_INTER);
}

/* zrange命令的底层实现，zrange命令的形式为range key start stop [withscore]。参数reverse指明迭代方向（正向/逆向）。*/
void zrangeGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    robj *zobj;
    int withscores = 0;
    long start;
    long end;
    int llen;
    int rangelen;

    // 取出需要返回的元素的下标start和end，即zrange命令的start参数和end参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

    // 判断是否有withscores选项，即是否需要打印相应的分数值
    if (c->argc == 5 && !strcasecmp(c->argv[4]->ptr,"withscores")) {
        withscores = 1;
    } 
    // 出错
    else if (c->argc >= 5) {
        addReply(c,shared.syntaxerr);
        return;
    }

    // 取出有序集合对象并检查其对象类型
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL
         || checkType(c,zobj,REDIS_ZSET)) return;

    /* Sanitize indexes. */
    // 将负数索引转换为正数索引，方便操作
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    // 验证索引
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    // 调整索引
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c, withscores ? (rangelen*2) : rangelen);

    // 处理ziplist编码的情况
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        // 根据迭代方向获取第一个开始的迭代节点
        if (reverse)
            eptr = ziplistIndex(zl,-2-(2*start));
        else
            eptr = ziplistIndex(zl,2*start);

        redisAssertWithInfo(c,zobj,eptr != NULL);
        // 获取分值节点
        sptr = ziplistNext(zl,eptr);

        // 依次遍历，取出在指定排位范围的元素
        while (rangelen--) {
            redisAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            // 取出元素值，如果是字符串编码则保存在vstr中，如果是整数编码则保存在vlong中
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                addReplyBulkLongLong(c,vlong);
            else
                addReplyBulkCBuffer(c,vstr,vlen);

            if (withscores)
                addReplyDouble(c,zzlGetScore(sptr));

            // 根据迭代方向获取前一个元素 or 后一个元素
            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }

    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        robj *ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        // 根据迭代对象取出第一个开始的迭代节点
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        // 依次迭代，取出相应的元素
        while(rangelen--) {
            redisAssertWithInfo(c,zobj,ln != NULL);
            // 取出元素值
            ele = ln->obj;
            addReplyBulk(c,ele);
            if (withscores)
                addReplyDouble(c,ln->score);
            // 根据迭代方向去的前一个节点 or 后一个节点
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

/* zrange命令的实现 */
void zrangeCommand(redisClient *c) {
    zrangeGenericCommand(c,0);
}

/* zrevrange命令实现 */
void zrevrangeCommand(redisClient *c) {
    zrangeGenericCommand(c,1);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
/* ZRANGEBYSCORE和ZREVRANGEBYSCORE命令的底层实现，参数reverse指明迭代方向（正向/逆向） */
void genericZrangebyscoreCommand(redisClient *c, int reverse) {
    zrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    int withscores = 0;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    // 解析命令指定的score区间范围
    if (zslParseRange(c->argv[minidx],c->argv[maxidx],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    // ZRANGEBYSCORE、ZREVRANGEBYSCORE完整的命令如下：
    //      zrangebyscore key min max [withscores] [limit offset N]

    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    // 解析其余可选参数
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            // withscores选项
            if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"withscores")) {
                pos++; remaining--;
                withscores = 1;
            } 
            // limit选项
            else if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != REDIS_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != REDIS_OK)) return;
                pos += 3; remaining -= 3;
            } 
            // 如果c->argc > 4，但又不是上面的withscores和limit选项，则报错
            else {
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    // 获取有序集合对象并检查其类型
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // 处理ziplist编码的情况
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score;

        /* If reversed, get the last node in range as starting point. */
        // 根据迭代方向获取迭代开始的第一个节点
        if (reverse) {
            eptr = zzlLastInRange(zl,&range);
        } else {
            eptr = zzlFirstInRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        // 如果没有元素的分值落在指定范围，则直接返回
        if (eptr == NULL) {
            addReply(c, shared.emptymultibulk);
            return;
        }

        /* Get score pointer for the first element. */
        redisAssertWithInfo(c,zobj,eptr != NULL);
        // 获取分值节点
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 跳过offset参数指定数量的元素
        while (eptr && offset--) {
            // 根据迭代方向跳过当前元素
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        // 返回所有分值在指定范围内的元素（受limit参数影响）
        while (eptr && limit--) {
            // 获取分值
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            // 检查当前元素的分值是否符合要求
            if (reverse) {
                if (!zslValueGteMin(score,&range)) break;
            } else {
                if (!zslValueLteMax(score,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always succeed */
            // 获取元素值
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }

            if (withscores) {
                addReplyDouble(c,score);
            }

            /* Move to next node */
            // 处理下一个元素（由迭代方向决定）
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        // 根据迭代方向获取迭代开始的第一个节点
        if (reverse) {
            ln = zslLastInRange(zsl,&range);
        } else {
            ln = zslFirstInRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        // 如果没有元素的分值落在指定范围，则直接返回
        if (ln == NULL) {
            addReply(c, shared.emptymultibulk);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 跳过offset参数指定数量的元素
        while (ln && offset--) {
            // 根据迭代方向跳过当前元素
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        // 返回所有分值在指定范围内的元素（受limit参数影响）
        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(ln->score,&range)) break;
            } else {
                if (!zslValueLteMax(ln->score,&range)) break;
            }

            rangelen++;
            addReplyBulk(c,ln->obj);

            if (withscores) {
                addReplyDouble(c,ln->score);
            }

            /* Move to next node */
            // 处理下一个节点
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    if (withscores) {
        rangelen *= 2;
    }

    setDeferredMultiBulkLength(c, replylen, rangelen);
}

/* zrangebyscore命令实现 */
void zrangebyscoreCommand(redisClient *c) {
    genericZrangebyscoreCommand(c,0);
}

/* zrevrangebyscore命令实现 */
void zrevrangebyscoreCommand(redisClient *c) {
    genericZrangebyscoreCommand(c,1);
}

/* zcount命令实现，返回分值在指定范围的元素个数 */
void zcountCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    int count = 0;

    /* Parse the range arguments */
    // 获取指定的分值范围
    if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Lookup the sorted set */
    // 获取有序集合对象并检查其类型
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_ZSET)) return;

    // 处理ziplist编码的类型
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        // 获取ziplist在指定分值范围内的第一个节点
        eptr = zzlFirstInRange(zl,&range);

        /* No "first" element */
        // 如果不存在任何一个元素的分值落在指定范围内，直接返回
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        // 取出当前元素的分值节点
        sptr = ziplistNext(zl,eptr);
        // 获取分值
        score = zzlGetScore(sptr);
        // 验证分值是否操作指定范围的右边界
        redisAssertWithInfo(c,zobj,zslValueLteMax(score,&range));

        /* Iterate over elements in range */
        // 开始遍历每一个元素直到其分值不满足要求
        while (eptr) {
            // 获取分值
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            // 遇到一个不满足条件的分值，跳出循环
            if (!zslValueLteMax(score,&range)) {
                break;
            } else {
                // 更新count计数器
                count++;
                // 处理下一个元素
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        // 获取指定分值范围内的第一个节点
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        // 下面根据排位来计算元素数量
        if (zn != NULL) {
            // 确定指定分值范围内的第一个元素的排位
            rank = zslGetRank(zsl, zn->score, zn->obj);
            // 减去落在指定区间范围左边界以外的元素个数
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            // 获取指定分值范围内的最后一个节点
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            // 如果指定分值范围内的最后一个节点存在，计算该节点的排位
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->obj);
                // 减去落在指定区间范围右边界以外的元素个数，得到指定分值范围内的元素个数
                count -= (zsl->length - rank);
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    addReplyLongLong(c, count);
}

/* zlexcount命令实现，获取有序集合中指定字典区间内元素数量 */
void zlexcountCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    int count = 0;

    /* Parse the range arguments */
    // 获取指定的字典序区间
    if (zslParseLexRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Lookup the sorted set */
    // 获取有序集合对象并检查其类型
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_ZSET))
    {
        // 如果出错注意要释放字典序区间结构体
        zslFreeLexRange(&range);
        return;
    }

    // 处理ziplist编码的情况
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        // 返回ziplist编码的有序集合中在指定字典序区间范围内的第一个节点
        eptr = zzlFirstInLexRange(zl,&range);

        /* No "first" element */
        // 如果没有任何一个元素落在指定字典序区间内，直接返回。同样，需要释放字典序区间结构体
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        // 获取分值节点
        sptr = ziplistNext(zl,eptr);
        // 检查指定字典序区间范围内的第一个节点是否操作字典序区间的右边界
        redisAssertWithInfo(c,zobj,zzlLexValueLteMax(eptr,&range));

        /* Iterate over elements in range */
        // 开始遍历，两两一组直到遇到超出字典序区间范围的节点
        while (eptr) {
            /* Abort when the node is no longer in range. */
            if (!zzlLexValueLteMax(eptr,&range)) {
                break;
            } else {
                // 记录元素个数
                count++;
                // 处理下一个元素
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        // 获取skiplist编码的有序集合中在指定字典序区间范围内的第一个节点
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        // 如果指定字典序区间范围内的第一个节点不为NULL，计算其排位
        if (zn != NULL) {
            // 获得指定字典序区间范围内的第一个节点的排位
            rank = zslGetRank(zsl, zn->score, zn->obj);
            // 减去该排位前面的元素个数
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            // 获取skiplist编码的有序集合中在指定字典序区间范围内的最后一个节点
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                // 获得指定字典序区间范围内的最后一个节点的排位
                rank = zslGetRank(zsl, zn->score, zn->obj);
                // 减去该排位后面的节点数量，得到指定字典序区间范围内的元素个数
                count -= (zsl->length - rank);
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/*  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    当插入到有序集合（Sorted set）中的所有元素的分值score都相同时，存储在键key中的有序集合中的元素是按字典序排序
    （Lexicographical ordering）的，使用下面命令返回值在最小值min及最大值max之间的所有元素。
    如果有序集合中的元素存在不同的分值，所返回的元素将不确定。
*/

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
/* ZRANGEBYLEX和ZREVRANGEBYLEX命令的底层实现，参数reverse指明迭代方向 */
void genericZrangebylexCommand(redisClient *c, int reverse) {
    zlexrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    // 获取指定的字典序区间范围
    if (zslParseLexRange(c->argv[minidx],c->argv[maxidx],&range) != REDIS_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    // ZRANGEBYLEX命令的完整形式为：
    // zrangebylex key min max [limit offset count]
    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    // 解析其他选项参数
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            // 获取limit选项
            if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != REDIS_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != REDIS_OK)) return;
                pos += 3; remaining -= 3;
            } else {
                zslFreeLexRange(&range);
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    // 获取有序集合对象并检查其类型
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET))
    {
        // 出错后务必释放字典序区间结构体
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        // 根据迭代方向获取指定字典序区间范围内开始迭代的第一个节点
        if (reverse) {
            eptr = zzlLastInLexRange(zl,&range);
        } else {
            eptr = zzlFirstInLexRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        // 如果没有任何一个元素落在指定的字典序区间范围内，直接返回
        if (eptr == NULL) {
            addReply(c, shared.emptymultibulk);
            zslFreeLexRange(&range);
            return;
        }

        /* Get score pointer for the first element. */
        redisAssertWithInfo(c,zobj,eptr != NULL);
        // 获取分值节点
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 跳过offset参数指定数量的元素
        while (eptr && offset--) {
            // 根据迭代方向跳过当前元素
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        // 依次遍历，获取指定字典序区间范围内的所有元素（受limit影响）
        while (eptr && limit--) {
            /* Abort when the node is no longer in range. */
            // 遇到不满足要求的节点，跳出
            if (reverse) {
                if (!zzlLexValueGteMin(eptr,&range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed. */
            // 从当前节点中获取元素值
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }

            /* Move to next node */
            // 处理下一个元素
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        // 根据迭代方向获取指定字典序区间范围内开始迭代的第一个节点
        if (reverse) {
            ln = zslLastInLexRange(zsl,&range);
        } else {
            ln = zslFirstInLexRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        // 如果没有任何一个元素落在指定的字典序区间范围内，直接返回
        if (ln == NULL) {
            addReply(c, shared.emptymultibulk);
            zslFreeLexRange(&range);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 跳过offset参数指定数量的元素
        while (ln && offset--) {
            // 根据迭代方向跳过当前元素
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        // 依次遍历，获取指定字典序区间范围内的所有元素（受limit影响）
        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslLexValueGteMin(ln->obj,&range)) break;
            } else {
                if (!zslLexValueLteMax(ln->obj,&range)) break;
            }

            rangelen++;
            addReplyBulk(c,ln->obj);

            /* Move to next node */
            // 处理下一个元素
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    // 释放资源
    zslFreeLexRange(&range);
    setDeferredMultiBulkLength(c, replylen, rangelen);
}

/* zrangebylex命令实现 */
void zrangebylexCommand(redisClient *c) {
    genericZrangebylexCommand(c,0);
}

/* zrevrangebylex命令实现 */
void zrevrangebylexCommand(redisClient *c) {
    genericZrangebylexCommand(c,1);
}

/* zcard命令实现，计算集合中元素的数量 */
void zcardCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    addReplyLongLong(c,zsetLength(zobj));
}

/* zscore命令实现，返回元素的分值 */
void zscoreCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    // 获取有序集合对象并检查其类型
    if ((zobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // 处理ziplist编码的情况
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        if (zzlFind(zobj->ptr,c->argv[2],&score) != NULL)
            addReplyDouble(c,score);
        else
            addReply(c,shared.nullbulk);
    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de;

        c->argv[2] = tryObjectEncoding(c->argv[2]);
        de = dictFind(zs->dict,c->argv[2]);
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            addReplyDouble(c,score);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

/* zrank命令的底层实现，返回指定元素的排名，参数reverse指明正序输出还是逆序输出  */
void zrankGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    // 目标元素值
    robj *ele = c->argv[2];
    robj *zobj;
    unsigned long llen;
    unsigned long rank;

    // 获取有序集合对象并检查其类型
    if ((zobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;
    llen = zsetLength(zobj);

    redisAssertWithInfo(c,ele,ele->encoding == REDIS_ENCODING_RAW);
    // 处理ziplist编码的情况
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        // 获取ziplist中第一个节点，对应有序集合中第一个元素
        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(c,zobj,eptr != NULL);
        // 获取分数值
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(c,zobj,sptr != NULL);

        rank = 1;
        // ziplist节点顺序存储元素，只能遍历一遍找到目标元素
        while(eptr != NULL) {
            if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL) {
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            // 未能找到目标节点
            addReply(c,shared.nullbulk);
        }
    } 
    // 处理skiplist编码的情况
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        // 对目标元素值编码
        ele = c->argv[2] = tryObjectEncoding(c->argv[2]);
        // 查找目标元素
        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            // 获取排位
            rank = zslGetRank(zsl,score,ele);
            redisAssertWithInfo(c,ele,rank); /* Existing elements always have a rank. */
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            // 未能找到目标元素
            addReply(c,shared.nullbulk);
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

/* zrank命令实现 */
void zrankCommand(redisClient *c) {
    zrankGenericCommand(c, 0);
}

void zrevrankCommand(redisClient *c) {
    zrankGenericCommand(c, 1);
}

void zscanCommand(redisClient *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,REDIS_ZSET)) return;
    scanGenericCommand(c,o,cursor);
}
