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

/* ================================ MULTI/EXEC ============================== */
/* multi / exec 命令相关操作 */

/* Client state initialization for MULTI/EXEC */
/*  初始化redisClient中与nulti / exec命令相关的成员的状态。 */
void initClientMultiState(redisClient *c) {
    // 命令队列
    c->mstate.commands = NULL;
    // 命令计算
    c->mstate.count = 0;
}

/* Release all the resources associated with MULTI/EXEC state */
/*  释放所有与multi/exec命令状态相关的资源
    multiState中包含multiCmd *command指针，multiCmd中包含robj **argv指针，这些资源都需要释放。
    具体可以看redis.h中multiState、multiCmd等结构体的定义。 */
void freeClientMultiState(redisClient *c) {
    int j;

    // 遍历命令队列
    for (j = 0; j < c->mstate.count; j++) {
        int i;
        // 获得当前命令
        multiCmd *mc = c->mstate.commands+j;

        // 释放当前命令的所有参数
        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);
        // 释放参数数组
        zfree(mc->argv);
    }
    // 释放任务队列
    zfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue */
/* 将一个新命令添加到multi命令队列中 */
void queueMultiCommand(redisClient *c) {
    multiCmd *mc;
    int j;

    // 在原commands后面配置空间以存放新命令
    c->mstate.commands = zrealloc(c->mstate.commands,
            sizeof(multiCmd)*(c->mstate.count+1));
    // 执行新配置的空间
    mc = c->mstate.commands+c->mstate.count;
    // 设置各个属性（命令、命令参数个数以及具体的命令参数）
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    // 分配空间以存放命令参数
    mc->argv = zmalloc(sizeof(robj*)*c->argc);
    memcpy(mc->argv,c->argv,sizeof(robj*)*c->argc);
    for (j = 0; j < c->argc; j++)
        incrRefCount(mc->argv[j]);
    // 命令队列中保存的命令个数加1
    c->mstate.count++;
}

/* 取消事务 */
void discardTransaction(redisClient *c) {
    // 释放资源
    freeClientMultiState(c);
    // 重置相关状态值
    initClientMultiState(c);
    c->flags &= ~(REDIS_MULTI|REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC);
    // 取消对所有key的监控
    unwatchAllKeys(c);
}

/* Flag the transacation as DIRTY_EXEC so that EXEC will fail.
 * Should be called every time there is an error while queueing a command. */
/* 将事务标识为DIRTY_EXEC，这样随后的EXEC命令则会失效。该函数需要在每次往命令队列添加命令出错时调用。*/
void flagTransaction(redisClient *c) {
    if (c->flags & REDIS_MULTI)
        c->flags |= REDIS_DIRTY_EXEC;
}

/* 执行MULTI命令 */
void multiCommand(redisClient *c) {
    // 不支持嵌套事务，否则直接报错
    if (c->flags & REDIS_MULTI) {
        addReplyError(c,"MULTI calls can not be nested");
        return;
    }
    // 设置事务标识
    c->flags |= REDIS_MULTI;
    addReply(c,shared.ok);
}

/* 执行discard命令 */
void discardCommand(redisClient *c) {
    // 如果当前客户端不处于事务状态，该命令无效
    if (!(c->flags & REDIS_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }
    // 取消事务
    discardTransaction(c);
    addReply(c,shared.ok);
}

/* Send a MULTI command to all the slaves and AOF file. Check the execCommand
 * implementation for more information. */
/* 向所有的slave节点和AOF文件发送MULTI命令 。*/
void execCommandPropagateMulti(redisClient *c) {
    // 生成MULTI命令字符串对象
    robj *multistring = createStringObject("MULTI",5);

    // 传播MULTI命令
    propagate(server.multiCommand,c->db->id,&multistring,1,
              REDIS_PROPAGATE_AOF|REDIS_PROPAGATE_REPL);
    decrRefCount(multistring);
}

/* 执行exec命令 */
void execCommand(redisClient *c) {
    int j;
    robj **orig_argv;
    int orig_argc;
    struct redisCommand *orig_cmd;
    // 是否需要将MULTI/EXEC命令传播到slave节点/AOF
    int must_propagate = 0; /* Need to propagate MULTI/EXEC to AOF / slaves? */

    // 如果客户端当前不处于事务状态，直接返回
    if (!(c->flags & REDIS_MULTI)) {
        addReplyError(c,"EXEC without MULTI");
        return;
    }

    /* Check if we need to abort the EXEC because:
     * 1) Some WATCHed key was touched.
     * 2) There was a previous error while queueing commands.
     * A failed EXEC in the first case returns a multi bulk nil object
     * (technically it is not an error but a special behavior), while
     * in the second an EXECABORT error is returned. */
    // 检查是否需要中断事务执行，因为：
    // （1）、有被监控的key被修改
    // （2）、命令入队的时候发生错误
    //  对于第一种情况，Redis返回多个nil空对象（准确地说这种情况并不是错误，应视为一种特殊的行为）
    //  对于第二种情况则返回一个EXECABORT错误
    if (c->flags & (REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC)) {
        addReply(c, c->flags & REDIS_DIRTY_EXEC ? shared.execaborterr :
                                                  shared.nullmultibulk);
        // 取消事务
        discardTransaction(c);
        goto handle_monitor;
    }

    /* Exec all the queued commands */
    // 现在可以执行该事务的所有命令了

    // 取消对所有key的监控，否则会浪费CPU资源
    unwatchAllKeys(c); /* Unwatch ASAP otherwise we'll waste CPU cycles */
    // 先备份一次命令队列中的命令
    orig_argv = c->argv;
    orig_argc = c->argc;
    orig_cmd = c->cmd;
    addReplyMultiBulkLen(c,c->mstate.count);
    // 逐一将事务中的命令交给客户端redisClient执行
    for (j = 0; j < c->mstate.count; j++) {
        // 将事务命令队列中的命令设置给客户端
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->cmd = c->mstate.commands[j].cmd;

        /* Propagate a MULTI request once we encounter the first write op.
         * This way we'll deliver the MULTI/..../EXEC block as a whole and
         * both the AOF and the replication link will have the same consistency
         * and atomicity guarantees. */
        //  当我们第一次遇到写命令时，传播MULTI命令。如果是读命令则无需传播
        //  这里我们MULTI/..../EXEC当做一个整体传输，保证服务器和AOF以及附属节点的一致性
        if (!must_propagate && !(c->cmd->flags & REDIS_CMD_READONLY)) {
            execCommandPropagateMulti(c);
            // 只需要传播一次MULTI命令即可
            must_propagate = 1;
        }

        // 真正执行命令
        call(c,REDIS_CALL_FULL);

        /* Commands may alter argc/argv, restore mstate. */
        // 命令执行后可能会被修改，需要更新操作
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].cmd = c->cmd;
    }
    // 恢复原命令
    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;
    // 清除事务状态
    discardTransaction(c);
    /* Make sure the EXEC command will be propagated as well if MULTI
     * was already propagated. */
    if (must_propagate) server.dirty++;

handle_monitor:
    /* Send EXEC to clients waiting data from MONITOR. We do it here
     * since the natural order of commands execution is actually:
     * MUTLI, EXEC, ... commands inside transaction ...
     * Instead EXEC is flagged as REDIS_CMD_SKIP_MONITOR in the command
     * table, and we do it here with correct ordering. */
    if (listLength(server.monitors) && !server.loading)
        replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
}

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *              WATCH命令
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 *  该实现在每个redisDB数据库使用一个哈希表来维护key和所有监控该key的客户端列表的映射关系。
 *  这样当一个key被修改后，我们就可以对所有监控该key的客户端设置dirty标识。
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called. 
 *
 *  另外，每个客户端redisClient也维护着一个保存所有被监控的key的列表，这样就可以方便地对key取消监控
 */

/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB */
 /* 在client->watched_keys中我们需要使用watchedKey结构来标识一个Redis中的key，在watchedKey中
    我们不仅需要保存被监控的key，还需要记录该key所在的数据库。*/
typedef struct watchedKey {
    // 被监控的key
    robj *key;
    // key所在的数据库
    redisDb *db;
} watchedKey;

/* Watch for the specified key */
/* 对一个给定的key进行监控。*/
void watchForKey(redisClient *c, robj *key) {
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    /* Check if we are already watching for this key */
    // 检查该key是否已经保存在client->watched_keys列表中

    // listRewind获取list的迭代器
    listRewind(c->watched_keys,&li);
    // 遍历查找，如果发现给定key已经存在直接返回
    while((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key,wk->key))
            return; /* Key already watched */
    }

    /* This key is not already watched in this DB. Let's add it */
    // 检查redisDB->watched_keys是否保存了该key和客户端的映射关系，如果没有则添加之
    // 获取监控给定key的客户端列表
    clients = dictFetchValue(c->db->watched_keys,key);
    // 如果该列表为空，则创建一个
    if (!clients) {
        clients = listCreate();
        dictAdd(c->db->watched_keys,key,clients);
        incrRefCount(key);
    }
    // 并加入当前客户端
    listAddNodeTail(clients,c);

    /* Add the new key to the list of keys watched by this client */
    // 将一个新的watchedKey结构添加到client->watched_keys列表中
    wk = zmalloc(sizeof(*wk));
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    listAddNodeTail(c->watched_keys,wk);
}

/* Unwatch all the keys watched by this client. To clean the EXEC dirty
 * flag is up to the caller. */
/* 取消客户端对所有key的监视，该操作由调用者执行。*/
void unwatchAllKeys(redisClient *c) {
    listIter li;
    listNode *ln;

    // 如果没有key被监控，直接返回
    if (listLength(c->watched_keys) == 0) return;
    // 获得c->watched_keys列表的迭代器
    listRewind(c->watched_keys,&li);
    // 遍历c->watched_keys列表，逐一删除被该客户端监视的key
    while((ln = listNext(&li))) {
        list *clients;
        watchedKey *wk;

        /* Lookup the watched key -> clients list and remove the client
         * from the list */
        wk = listNodeValue(ln);
        // 将当前客户端从db->watched_keys中删除
        clients = dictFetchValue(wk->db->watched_keys, wk->key);
        redisAssertWithInfo(c,NULL,clients != NULL);
        listDelNode(clients,listSearchKey(clients,c));

        /* Kill the entry at all if this was the only client */
        // 如果没有任何客户端监控该key，则将该key从db->watched_keys中删除
        if (listLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);

        /* Remove this watched key from the client->watched list */
        // 将c->watched_keys删除该key
        listDelNode(c->watched_keys,ln);

        // 释放资源
        decrRefCount(wk->key);
        zfree(wk);
    }
}

/* "Touch" a key, so that if this key is being WATCHed by some client the
 * next EXEC will fail. */
/* 如果某个被监控的key被修改（触碰touch），则设置REDIS_DIRTY_CAS标识，随着这些客户端client在执行EXEC命令时将失败返回。*/
void touchWatchedKey(redisDb *db, robj *key) {
    list *clients;
    listIter li;
    listNode *ln;

    // 如果没有任何键被监控，直接返回
    if (dictSize(db->watched_keys) == 0) return;
    // 找到监控该key的客户端列表
    clients = dictFetchValue(db->watched_keys, key);
    if (!clients) return;

    /* Mark all the clients watching this key as REDIS_DIRTY_CAS */
    /* Check if we are already watching for this key */
    // 遍历监控该key的所有客户端列表，设置其flags标识为REDIS_DIRTY_CAS
    listRewind(clients,&li);
    while((ln = listNext(&li))) {
        redisClient *c = listNodeValue(ln);

        c->flags |= REDIS_DIRTY_CAS;
    }
}

/* On FLUSHDB or FLUSHALL all the watched keys that are present before the
 * flush but will be deleted as effect of the flushing operation should
 * be touched. "dbid" is the DB that's getting the flush. -1 if it is
 * a FLUSHALL operation (all the DBs flushed). */
/*  当执行FLUSHDB或FLUSHALL命令时候，该数据库内的所有被监控的key都被touch，也就是认为这些
    key已经被修改。

    参数dbid是flush操作的目标数据库，如果dbid为-1，则表示所有的数据库都要被flush。
*/
void touchWatchedKeysOnFlush(int dbid) {
    listIter li1, li2;
    listNode *ln;

    // 这里的做法是遍历所有的客户端client，对于每一个客户端，遍历其监视的所有key，设置相应客户端的flag标识

    /* For every client, check all the waited keys */
    // 遍历所有的客户端client
    listRewind(server.clients,&li1);
    while((ln = listNext(&li1))) {
        redisClient *c = listNodeValue(ln);

        // 遍历该客户端监控的所有key，保存在c->watched_keys中，每一个节点为watchedKey结构
        listRewind(c->watched_keys,&li2);
        while((ln = listNext(&li2))) {
            watchedKey *wk = listNodeValue(ln);

            /* For every watched key matching the specified DB, if the
             * key exists, mark the client as dirty, as the key will be
             * removed. */
            // 设置flags标识为REDIS_DIRTY_CAS
            if (dbid == -1 || wk->db->id == dbid) {
                if (dictFind(wk->db->dict, wk->key->ptr) != NULL)
                    c->flags |= REDIS_DIRTY_CAS;
            }
        }
    }
}

/* watch命令 */
void watchCommand(redisClient *c) {
    int j;

    // watch命令不能在事务状态时执行
    if (c->flags & REDIS_MULTI) {
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    }

    // 监控输入的所有key
    for (j = 1; j < c->argc; j++)
        watchForKey(c,c->argv[j]);
    addReply(c,shared.ok);
}

/* unwatch命令 */
void unwatchCommand(redisClient *c) {
    unwatchAllKeys(c);
    c->flags &= (~REDIS_DIRTY_CAS);
    addReply(c,shared.ok);
}
