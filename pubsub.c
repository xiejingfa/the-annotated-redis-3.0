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

/*-----------------------------------------------------------------------------
 * Pubsub low level API    发布（Publish）订阅（Subscribe）底层API
 *----------------------------------------------------------------------------*/

/*	Redis中可以订阅频道channel和模式pattern		*/

/*	释放给定的pubsubPattern结构实例 */
void freePubsubPattern(void *p) {
	// pubsubPattern结构体定义在redis.h头文件中，表示一个订阅模式的记录
    pubsubPattern *pat = p;

    decrRefCount(pat->pattern);
    zfree(pat);
}

/*	比较给定的模式a和模式b是否相同，如果相同返回1，否则返回0 */
int listMatchPubsubPattern(void *a, void *b) {
    pubsubPattern *pa = a, *pb = b;

    // 模式订阅的客户端相同，且被订阅的模式也相同
    return (pa->client == pb->client) &&
           (equalStringObjects(pa->pattern,pb->pattern));
}

/* Return the number of channels + patterns a client is subscribed to. */
/*	统计该客户端订阅的频道和模式数量之和	*/
int clientSubscriptionsCount(redisClient *c) {
    return dictSize(c->pubsub_channels)+
           listLength(c->pubsub_patterns);
}

/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. */
/*	频道订阅，即设置客户端订阅频道，如果操作成功返回1，如果该客户端已经订阅了指定频道则返回0 	*/
int pubsubSubscribeChannel(redisClient *c, robj *channel) {
    dictEntry *de;
    list *clients = NULL;
    int retval = 0;

    /* Add the channel to the client -> channels hash table */
    // 将频道channel添加到client -> channels哈希表中
    if (dictAdd(c->pubsub_channels,channel,NULL) == DICT_OK) {
        retval = 1;
        incrRefCount(channel);
        /* Add the client to the channel -> list of clients hash table */
        // server.pubsub_channels中保存了Redis服务器中所有频道和其相关客户端的信息
        // 先判断给定频道是否存在于server.pubsub_channels字典中
        de = dictFind(server.pubsub_channels,channel);
        if (de == NULL) {
        	// 频道不存在，则将其加入server.pubsub_channels中
            clients = listCreate();
            dictAdd(server.pubsub_channels,channel,clients);
            incrRefCount(channel);
        } else {
            clients = dictGetVal(de);
        }
        // 将客户端添加到指定频道对应的客户端链表中
        listAddNodeTail(clients,c);
    }
    /* Notify the client */
    // 回复客户端
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.subscribebulk);
    addReplyBulk(c,channel);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
/*	退订频道，即取消客户端对某频道的订阅。如果操作成功返回1，如果该客户端没有订阅该频道则返回0	*/
int pubsubUnsubscribeChannel(redisClient *c, robj *channel, int notify) {
    dictEntry *de;
    list *clients;
    listNode *ln;
    int retval = 0;

    /* Remove the channel from the client -> channels hash table */
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */
    // 将频道从客户端client -> channels字典中移除，如果移除成功，说明客户端的确订阅了该频道
    if (dictDelete(c->pubsub_channels,channel) == DICT_OK) {
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        /* 将客户端从server.pubsub_channels字典中移除	*/

        // 找到订阅该频道的客户端链表
        de = dictFind(server.pubsub_channels,channel);
        redisAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        // 在链表中查找该客户端
        ln = listSearchKey(clients,c);
        redisAssertWithInfo(c,NULL,ln != NULL);
        // 移除客户端
        listDelNode(clients,ln);
        // 如果订阅该频道的客户端链表为空，则删除之
        if (listLength(clients) == 0) {
            /* Free the list and associated hash entry at all if this was
             * the latest client, so that it will be possible to abuse
             * Redis PUBSUB creating millions of channels. */
            dictDelete(server.pubsub_channels,channel);
        }
    }
    /* Notify the client */
    // 回复客户端
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        // 被退订的客户端
        addReplyBulk(c,channel);
        // 客户端仍在订阅的频道和模式数量
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));

    }
    decrRefCount(channel); /* it is finally safe to release it */
    return retval;
}

/* Subscribe a client to a pattern. Returns 1 if the operation succeeded, or 0 if the client was already subscribed to that pattern. */
/*	模式订阅，即设置客户端订阅某模式。如果订阅成功则返回1，如果客户端已经订阅了该模式则返回0。*/
int pubsubSubscribePattern(redisClient *c, robj *pattern) {
    int retval = 0;

    // 先在客户端的c->pubsub_patterns链表中查找，判断客户端是否已经订阅了该模式
    if (listSearchKey(c->pubsub_patterns,pattern) == NULL) {
    	// 客户端并没有订阅该模式
        retval = 1;
        pubsubPattern *pat;
        // 将制定模式添加到c->pubsub_patterns链表中
        listAddNodeTail(c->pubsub_patterns,pattern);
        incrRefCount(pattern);
        pat = zmalloc(sizeof(*pat));
        pat->pattern = getDecodedObject(pattern);
        pat->client = c;
        // 将pubsubPattern结构添加到server.pubsub_patterns链表中
        listAddNodeTail(server.pubsub_patterns,pat);
    }
    /* Notify the client */
    // 回复客户端
    addReply(c,shared.mbulkhdr[3]);
    // 回复“psubscribe”字符串
    addReply(c,shared.psubscribebulk);
    // 回复被订阅的模式字符串
    addReplyBulk(c,pattern);
    // 回复客户端订阅的频道和模式总数目
    addReplyLongLong(c,clientSubscriptionsCount(c));
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
/*	退订模式，即取消客户端对某模式的订阅。如果取消成功返回1，如果客户端并没有订阅该模式则返回0。*/
int pubsubUnsubscribePattern(redisClient *c, robj *pattern, int notify) {
    listNode *ln;
    pubsubPattern pat;
    int retval = 0;

    incrRefCount(pattern); /* Protect the object. May be the same we remove */
    // 遍历c->pubsub_patterns链表，判断该客户端是否订阅了该模式
    if ((ln = listSearchKey(c->pubsub_patterns,pattern)) != NULL) {
    	// 客户端订阅了该模式
        retval = 1;
        // 从c->pubsub_patterns链表中删除该模式
        listDelNode(c->pubsub_patterns,ln);
        pat.client = c;
        pat.pattern = pattern;
        // 从server.pubsub_patterns链表中删除该模式
        ln = listSearchKey(server.pubsub_patterns,&pat);
        listDelNode(server.pubsub_patterns,ln);
    }
    /* Notify the client */
    // 回复客户端
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.punsubscribebulk);
        addReplyBulk(c,pattern);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));
    }
    decrRefCount(pattern);
    return retval;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed to. */
/*	取消客户端订阅的所有频道，最后返回退订的频道数量	 */
int pubsubUnsubscribeAllChannels(redisClient *c, int notify) {
	// 获取迭代器
    dictIterator *di = dictGetSafeIterator(c->pubsub_channels);
    dictEntry *de;
    int count = 0;

    // 遍历c->pubsub_channels字典，逐一退订所订阅的频道
    while((de = dictNext(di)) != NULL) {
        robj *channel = dictGetKey(de);

        // 统计退订的频道数量
        count += pubsubUnsubscribeChannel(c,channel,notify);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    // 如果count == 0，说明客户端没有订阅任何频道，回复客户端
    if (notify && count == 0) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReply(c,shared.nullbulk);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));
    }
    dictReleaseIterator(di);
    //	最后返回退订的频道数量
    return count;
}

/* Unsubscribe from all the patterns. Return the number of patterns the
 * client was subscribed from. */
/*	取消客户端所订阅的所有模式，最后返回退订的模式数量	*/
int pubsubUnsubscribeAllPatterns(redisClient *c, int notify) {
    listNode *ln;
    listIter li;
    int count = 0;

    // 获取c->pubsub_patterns链表迭代器
    listRewind(c->pubsub_patterns,&li);
    // 遍历客户端订阅的模式链表，逐一退订
    while ((ln = listNext(&li)) != NULL) {
        robj *pattern = ln->value;
        // 统计退订的模式数量
        count += pubsubUnsubscribePattern(c,pattern,notify);
    }

    // 如果count == 0，说明客户端没有订阅任何模式，回复客户端
    if (notify && count == 0) {
        /* We were subscribed to nothing? Still reply to the client. */
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.punsubscribebulk);
        addReply(c,shared.nullbulk);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));
    }
    return count;
}

/* Publish a message */
/*	发布一则消息，即将消息发送给所有订阅了指定频道channel的所有客户端以及所有订阅了和指定频道匹配的模式的客户端。*/
int pubsubPublishMessage(robj *channel, robj *message) {
    int receivers = 0;
    dictEntry *de;
    listNode *ln;
    listIter li;

    /* Send to clients listening for that channel */
    // 取出订阅指定频道的客户端链表
    de = dictFind(server.pubsub_channels,channel);
    if (de) {
        list *list = dictGetVal(de);
        listNode *ln;
        listIter li;

        listRewind(list,&li);
        // 遍历该客户端链表，并将消息逐一发送给这些客户端
        while ((ln = listNext(&li)) != NULL) {
            redisClient *c = ln->value;

            // 发送消息
            addReply(c,shared.mbulkhdr[3]);
            // 回复“message”字符串
            addReply(c,shared.messagebulk);
            // 回复消息的来源频道
            addReplyBulk(c,channel);
            // 回复消息内容
            addReplyBulk(c,message);
            // 统计接收客户端的数量
            receivers++;
        }
    }
    /* Send to clients listening to matching channels */
    // 将消息发送个订阅了和指定频道匹配的模式的客户端
    if (listLength(server.pubsub_patterns)) {
        listRewind(server.pubsub_patterns,&li);
        channel = getDecodedObject(channel);
        // 遍历server.pubsub_patterns
        while ((ln = listNext(&li)) != NULL) {
        	// 取出当前模式
            pubsubPattern *pat = ln->value;

            // 判断当前模式是否和给定频道相匹配
            if (stringmatchlen((char*)pat->pattern->ptr,
                                sdslen(pat->pattern->ptr),
                                (char*)channel->ptr,
                                sdslen(channel->ptr),0)) {
            	// 发送消息
                addReply(pat->client,shared.mbulkhdr[4]);
                addReply(pat->client,shared.pmessagebulk);
                addReplyBulk(pat->client,pat->pattern);
                addReplyBulk(pat->client,channel);
                addReplyBulk(pat->client,message);
                receivers++;
            }
        }
        decrRefCount(channel);
    }
    return receivers;
}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation	发布订阅命令实现
 *----------------------------------------------------------------------------*/

/*	subscribe订阅频道命令*/
void subscribeCommand(redisClient *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c,c->argv[j]);
    c->flags |= REDIS_PUBSUB;
}

/*	unsbuscribe退订频道命令	*/
void unsubscribeCommand(redisClient *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~REDIS_PUBSUB;
}

/*	psubscribe订阅模式命令 */
void psubscribeCommand(redisClient *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribePattern(c,c->argv[j]);
    c->flags |= REDIS_PUBSUB;
}

/*	punsubscribe退订模式命令 */
void punsubscribeCommand(redisClient *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllPatterns(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribePattern(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~REDIS_PUBSUB;
}

/* publish发布命令*/
void publishCommand(redisClient *c) {
    int receivers = pubsubPublishMessage(c->argv[1],c->argv[2]);
    if (server.cluster_enabled)
        clusterPropagatePublish(c->argv[1],c->argv[2]);
    else
        forceCommandPropagation(c,REDIS_PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* PUBSUB command for Pub/Sub introspection. */
void pubsubCommand(redisClient *c) {

	// 处理PUBSUB CHANNELS [pattern]命令
    if (!strcasecmp(c->argv[1]->ptr,"channels") &&
        (c->argc == 2 || c->argc ==3))
    {
        /* PUBSUB CHANNELS [<pattern>] */
        // 获取pattern参数，如果没有则为NULL
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        dictIterator *di = dictGetIterator(server.pubsub_channels);
        dictEntry *de;
        long mblen = 0;
        void *replylen;

        replylen = addDeferredMultiBulkLength(c);
        // 遍历server.pubsub_channels字典
        while((de = dictNext(di)) != NULL) {
        	// 取出当前频道channel
            robj *cobj = dictGetKey(de);
            sds channel = cobj->ptr;

            // 如果没有给定pattern参数，则打印出所有频道
            // 如果给定pattern参数，则打印出与pattern参数相匹配的频道
            if (!pat || stringmatchlen(pat, sdslen(pat),
                                       channel, sdslen(channel),0))
            {
                addReplyBulk(c,cobj);
                mblen++;
            }
        }
        dictReleaseIterator(di);
        setDeferredMultiBulkLength(c,replylen,mblen);
    } 
    // 处理PUBSUB NUMSUB [Channel_1 ... Channel_N]命令
    else if (!strcasecmp(c->argv[1]->ptr,"numsub") && c->argc >= 2) {
        /* PUBSUB NUMSUB [Channel_1 ... Channel_N] */
        int j;

        addReplyMultiBulkLen(c,(c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            list *l = dictFetchValue(server.pubsub_channels,c->argv[j]);

            addReplyBulk(c,c->argv[j]);
            addReplyLongLong(c,l ? listLength(l) : 0);
        }
    } 
    // 处理PUBSUB NUMPA命令
    else if (!strcasecmp(c->argv[1]->ptr,"numpat") && c->argc == 2) {
        /* PUBSUB NUMPAT */
        addReplyLongLong(c,listLength(server.pubsub_patterns));
    } else {
        addReplyErrorFormat(c,
            "Unknown PUBSUB subcommand or wrong number of arguments for '%s'",
            (char*)c->argv[1]->ptr);
    }
}
