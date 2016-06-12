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
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

void aofUpdateCurrentSize(void);
void aofClosePipes(void);

/* ----------------------------------------------------------------------------
 * AOF rewrite buffer implementation.
 *	AOF重写缓存的实现
 *
 * The following code implement a simple buffer used in order to accumulate
 * changes while the background process is rewriting the AOF file.
 *	下面的代码实现了一个简单的缓存功能，用来在后台执行AOF文件重写（即BGREWRITEAOF命令）时
 *	积攒所有修改数据库的操作。
 *
 * We only need to append, but can't just use realloc with a large block
 * because 'huge' reallocs are not always handled as one could expect
 * (via remapping of pages at OS level) but may involve copying data.
 *	对于这个缓存，我们只需要append操作，但是我们无法分配一个非常大的空间。
 *	因为并不总是能成功分配一个非常大的空间
 *
 * For this reason we use a list of blocks, every block is
 * AOF_RW_BUF_BLOCK_SIZE bytes.
 *	因此我们使用多个大小为AOF_RW_BUF_BLOCK_SIZE字节的空间来实现缓存功能。
 *
 *	！！！！！！！！！这里约定：把每个大小为AOF_RW_BUF_BLOCK_SIZE字节的空间称作一个“缓冲区” ！！！！！！！！！！！！！
 * ------------------------------------------------------------------------- */

/*	定义每个缓冲区的大小为10M	*/
#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */

/*	AOF重写缓存结构体 */
typedef struct aofrwblock {
	// 缓冲区中已经使用的字节数和可用字节数
    unsigned long used, free;
    // 缓冲区
    char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;

/* This function free the old AOF rewrite buffer if needed, and initialize
 * a fresh new one. It tests for server.aof_rewrite_buf_blocks equal to NULL
 * so can be used for the first initialization as well. */
/*	这个函数用来释放旧的AOF重写缓存并初始化一个新的AOF重写缓存。
	这个函数会检测server.aof_rewrite_buf_blocks是否为NULL，因此也可以用于AOF重写缓存的初始化。*/
void aofRewriteBufferReset(void) {
    // 释放旧的AOF重写缓存，实际就是释放链表
    if (server.aof_rewrite_buf_blocks)
        listRelease(server.aof_rewrite_buf_blocks);

    // 初始化新的AOF缓存，实际就是创建一个链表
    server.aof_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.aof_rewrite_buf_blocks,zfree);
}

/* Return the current size of the AOF rewrite buffer. */
/*	返回AOF重写缓存的当前已使用的空间字节数	*/
unsigned long aofRewriteBufferSize(void) {
    listNode *ln;
    listIter li;
    unsigned long size = 0;

    // 初始化链表迭代器
    listRewind(server.aof_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
    	// 获取描述当前缓冲区的aofrwblock结构体
        aofrwblock *block = listNodeValue(ln);
        // 更新已使用空间大小
        size += block->used;
    }
    return size;
}

/* Event handler used to send data to the child process doing the AOF
 * rewrite. We send pieces of our AOF differences buffer so that the final
 * write when the child finishes the rewrite will be small. */
/*	事件处理器，用于将AOF重写缓存中的数据发送给AOF重写子进程。*/
void aofChildWriteDiffData(aeEventLoop *el, int fd, void *privdata, int mask) {
    listNode *ln;
    aofrwblock *block;
    ssize_t nwritten;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(fd);
    REDIS_NOTUSED(privdata);
    REDIS_NOTUSED(mask);

    while(1) {
        ln = listFirst(server.aof_rewrite_buf_blocks);
        block = ln ? ln->value : NULL;
        if (server.aof_stop_sending_diff || !block) {
            aeDeleteFileEvent(server.el,server.aof_pipe_write_data_to_child,
                              AE_WRITABLE);
            return;
        }
        if (block->used > 0) {
            nwritten = write(server.aof_pipe_write_data_to_child,
                             block->buf,block->used);
            if (nwritten <= 0) return;
            memmove(block->buf,block->buf+nwritten,block->used-nwritten);
            block->used -= nwritten;
        }
        if (block->used == 0) listDelNode(server.aof_rewrite_buf_blocks,ln);
    }
}

/* Append data to the AOF rewrite buffer, allocating new blocks if needed. */
/*	将s中的数据追加到AOF重写缓存中，如果有需要的话就分配一个新的缓冲区。*/
void aofRewriteBufferAppend(unsigned char *s, unsigned long len) {
	// 找到链表中的最后一个缓冲区
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    // 取出该节点的值，即aofrwblock结构
    aofrwblock *block = ln ? ln->value : NULL;

    while(len) {
        /* If we already got at least an allocated block, try appending
         * at least some piece into it. */
    	// 如果至少有一个缓冲区，我们就尝试将数据追加到该缓冲区中
        if (block) {
            unsigned long thislen = (block->free < len) ? block->free : len;
            // 如果当前缓冲区还有剩余空间能够容纳s中的数据，则不需要分配新的缓冲区
            if (thislen) {  /* The current block is not already full. */
                memcpy(block->buf+block->used, s, thislen);
                block->used += thislen;
                block->free -= thislen;
                s += thislen;
                len -= thislen;
            }
        }

        // 链表为空（不存在缓冲区） 或 s中还有剩余数据，这两种情况下需要分配新的缓冲区
        if (len) { /* First block to allocate, or need another block. */
            int numblocks;

            // 分配一个新的缓冲区
            block = zmalloc(sizeof(*block));
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;
            // 将该缓冲区追加到链表尾部
            listAddNodeTail(server.aof_rewrite_buf_blocks,block);

            /* Log every time we cross more 10 or 100 blocks, respectively
             * as a notice or warning. */
            // 每创建10个或100个缓冲区，就打印一个“提醒”或“警告”日志
            numblocks = listLength(server.aof_rewrite_buf_blocks);
            if (((numblocks+1) % 10) == 0) {
                int level = ((numblocks+1) % 100) == 0 ? REDIS_WARNING :
                                                         REDIS_NOTICE;
                redisLog(level,"Background AOF buffer size: %lu MB",
                    aofRewriteBufferSize()/(1024*1024));
            }
        }
    }

    /* Install a file event to send data to the rewrite child if there is
     * not one already. */
    if (aeGetFileEvents(server.el,server.aof_pipe_write_data_to_child) == 0) {
        aeCreateFileEvent(server.el, server.aof_pipe_write_data_to_child,
            AE_WRITABLE, aofChildWriteDiffData, NULL);
    }
}

/* Write the buffer (possibly composed of multiple blocks) into the specified
 * fd. If a short write or any other error happens -1 is returned,
 * otherwise the number of bytes written is returned. */
/*	将AOF缓存中的内容（可能由多个缓冲区组成）写入指定的fd。
	如果发生错误返回-1，否则返回写入的字节数。*/
ssize_t aofRewriteBufferWrite(int fd) {
    listNode *ln;
    listIter li;
    ssize_t count = 0;

    // 通过迭代器遍历AOF重写缓存中的所有缓冲区
    listRewind(server.aof_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
    	// 得到当前缓冲区
        aofrwblock *block = listNodeValue(ln);
        ssize_t nwritten;

        if (block->used) {
        	// 如果当前缓冲区中有数据，则将所有数据写入fd中
            nwritten = write(fd,block->buf,block->used);
            // 发生错误，返回-1
            if (nwritten != (ssize_t)block->used) {
                if (nwritten == 0) errno = EIO;
                return -1;
            }
            // 记录写入的字节数
            count += nwritten;
        }
    }
    return count;
}

/* ----------------------------------------------------------------------------
 * AOF file implementation
 *	AOF文件的实现
 * ------------------------------------------------------------------------- */

/* Starts a background task that performs fsync() against the specified
 * file descriptor (the one of the AOF file) in another thread. */
/*	在另一个线程中，对参数fd指向AOF文件执行符执行fsync（）操作	*/
void aof_background_fsync(int fd) {
    bioCreateBackgroundJob(REDIS_BIO_AOF_FSYNC,(void*)(long)fd,NULL,NULL);
}

/* Called when the user switches from "appendonly yes" to "appendonly no"
 * at runtime using the CONFIG command. */
/*	在Redis运行时，如果用户通过CONFIG命令关闭了AOF功能时调用该函数。	*/
void stopAppendOnly(void) {
	// 只有在Redis起用AOF命令时候才能执行关闭操作，从而触发该函数
    redisAssert(server.aof_state != REDIS_AOF_OFF);
    // flush操作，将AOF缓存中的内容写入AOF文件中
    flushAppendOnlyFile(1);
    aof_fsync(server.aof_fd);
    // 关闭AOF文件
    close(server.aof_fd);

    // 重置redisServer实例中与AOF相关的状态
    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = REDIS_AOF_OFF;
    /* rewrite operation in progress? kill it, wait child exit */
    // 如果正在执行AOF重写操作，则杀死相关子进程并等待其退出
    if (server.aof_child_pid != -1) {
        int statloc;

        redisLog(REDIS_NOTICE,"Killing running AOF rewrite child: %ld",
            (long) server.aof_child_pid);
        // kill AOF子进程
        if (kill(server.aof_child_pid,SIGUSR1) != -1)
        	// 等待子进程退出
            wait3(&statloc,0,NULL);
        /* reset the buffer accumulating changes while the child saves */
        // 清理缓存和临时文件
        aofRewriteBufferReset();
        aofRemoveTempFile(server.aof_child_pid);
        // 重置相关的状态
        server.aof_child_pid = -1;
        server.aof_rewrite_time_start = -1;
        /* close pipes used for IPC between the two processes. */
        // 关闭管道文件
        aofClosePipes();
    }
}

/* Called when the user switches from "appendonly no" to "appendonly yes"
 * at runtime using the CONFIG command. */
/*	在Redis运行时，如果用户通过CONFIG命令开启了AOF功能时调用该函数。	*/
int startAppendOnly(void) {
	// 将当前时间设为AOF最后一次同步fsync操作时间
    server.aof_last_fsync = server.unixtime;
    // 打开AOF文件
    server.aof_fd = open(server.aof_filename,O_WRONLY|O_APPEND|O_CREAT,0644);
    redisAssert(server.aof_state == REDIS_AOF_OFF);
    // 如果打开失败则返回
    if (server.aof_fd == -1) {
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't open the append only file: %s",strerror(errno));
        return REDIS_ERR;
    }
    // 执行AOF重写操作
    if (rewriteAppendOnlyFileBackground() == REDIS_ERR) {
        close(server.aof_fd);
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
        return REDIS_ERR;
    }
    /* We correctly switched on AOF, now wait for the rewrite to be complete
     * in order to append data on disk. */
    // 设置server.aof_state状态。到这里我们成功开启了AOF功能，等待AOF重写完成
    server.aof_state = REDIS_AOF_WAIT_REWRITE;
    return REDIS_OK;
}

/* Write the append only file buffer on disk.
 *	将AOF缓存中的内容写入到磁盘文件中
 *
 * Since we are required to write the AOF before replying to the client,
 * and the only way the client socket can get a write is entering when the
 * the event loop, we accumulate all the AOF writes in a memory
 * buffer and write it on disk using this function just before entering
 * the event loop again.
 *	因为Redis要求在回复客户端之前对AOF文件执行写操作，而客户端能执行写操作的唯一途径是进入事件循环之前。
 *	所以我们将所有AOF写的内容存放在缓存中，当Redis重新进入事件循环之前调用下面这个函数将缓存中的内容写入
 *	文件中。
 *
 * About the 'force' argument:
 *	关于参数force，介绍如下：
 *
 * When the fsync policy is set to 'everysec' we may delay the flush if there
 * is still an fsync() going on in the background thread, since for instance
 * on Linux write(2) will be blocked by the background fsync anyway.
 * When this happens we remember that there is some aof buffer to be
 * flushed ASAP, and will try to do that in the serverCron() function.
 *	当fsync同步策略被设置为everysec（每秒保存一次），如果后台线程正在执行fsync（）操作
 *	我们就延迟执行flush操作，因为Linux上的write（2）会被后台的fsync操作阻塞。
 *	当这种情况发生时，我们需要记录“存在一些AOF缓存需要执行flush操作”的信息，Redis将会尝试在serverCron()
 *	函数中执行该操作。
 *
 * However if force is set to 1 we'll write regardless of the background
 * fsync. 
 *	但是，如果参数force被设置为1，则不管后台是否正在执行fsync操作都会直接将AOF缓存写入文件中。
 *
 *	AOF支持三种fsync同步策略：always、everysec、no，默认是everysec。
 */
#define AOF_WRITE_LOG_ERROR_RATE 30 /* Seconds between errors logging. */
void flushAppendOnlyFile(int force) {
    ssize_t nwritten;
    int sync_in_progress = 0;
    mstime_t latency;

    // 缓冲区中没有没有任何内容，直接返回
    if (sdslen(server.aof_buf) == 0) return;

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
    	// 判断后台是否有fsync在执行
        sync_in_progress = bioPendingJobsOfType(REDIS_BIO_AOF_FSYNC) != 0;

    // 如果fsync同步策略被设置everysec，且不强制写入
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {
        /* With this append fsync policy we do background fsyncing.
         * If the fsync is still in progress we can try to delay
         * the write for a couple of seconds. */
        // 当fsync同步策略被设置everysec时，我们会在后台执行fsync操作
        // 如果当前后台仍在执行fsync操作，我们将尝试延迟几秒再执行写操作

        // sync_in_progress不为0，说明后台有fsync操作在执行
        if (sync_in_progress) {
            if (server.aof_flush_postponed_start == 0) {
                /* No previous write postponing, remember that we are
                 * postponing the flush and return. */
            	// 前面没有推迟的写操作，记录下延迟写操作的时间然后退出
                server.aof_flush_postponed_start = server.unixtime;
                return;
            } else if (server.unixtime - server.aof_flush_postponed_start < 2) {
                /* We were already waiting for fsync to finish, but for less
                 * than two seconds this is still ok. Postpone again. */
            	// 如果前面已经有写操作因为fsync而被推迟，且推迟的时间不超过2秒，直接返回，不执行剩余操作
                return;
            }
            /* Otherwise fall trough, and go write since we can't wait
             * over two seconds. */
            // 如果后台有fsync操作正在执行，且写操作已经被推迟多于2秒，那么执行写操作（但是该操作会被阻塞）
            server.aof_delayed_fsync++;
            redisLog(REDIS_NOTICE,"Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }
    /* We want to perform a single write. This should be guaranteed atomic
     * at least if the filesystem we are writing is a real physical one.
     * While this will save us against the server being killed I don't think
     * there is much to do about the whole server stopping for power problems
     * or alike */
    /*	这里我们要执行单次写操作，如果我们写入的文件系统是物理设备的话需要保证这个操作是原子的。 */

    latencyStartMonitor(latency);
    // 将server.aof_buf缓冲区中的数据写入文件中
    nwritten = write(server.aof_fd,server.aof_buf,sdslen(server.aof_buf));
    latencyEndMonitor(latency);
    /* We want to capture different events for delayed writes:
     * when the delay happens with a pending fsync, or with a saving child
     * active, and when the above two conditions are missing.
     * We also use an additional event name to save all samples which is
     * useful for graphing / monitoring purposes. */
    if (sync_in_progress) {
        latencyAddSampleIfNeeded("aof-write-pending-fsync",latency);
    } else if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) {
        latencyAddSampleIfNeeded("aof-write-active-child",latency);
    } else {
        latencyAddSampleIfNeeded("aof-write-alone",latency);
    }
    latencyAddSampleIfNeeded("aof-write",latency);

    /* We performed the write so reset the postponed flush sentinel to zero. */
    // 重置延迟写操作的时间记录
    server.aof_flush_postponed_start = 0;

    // 如果写入的字节数与server.aof_buf缓冲区中数据的字节数不一致，说明写操作出错
    if (nwritten != (signed)sdslen(server.aof_buf)) {
        static time_t last_write_error_log = 0;
        int can_log = 0;

        /* Limit logging rate to 1 line per AOF_WRITE_LOG_ERROR_RATE seconds. */
        // 限制日志的记录频率为每行AOF_WRITE_LOG_ERROR_RATE秒
        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            last_write_error_log = server.unixtime;
        }

        /* Log the AOF write error and record the error code. */
        // 	记录AOF写入错误日志和相关的错误码
        if (nwritten == -1) {
            if (can_log) {
                redisLog(REDIS_WARNING,"Error writing to the AOF file: %s",
                    strerror(errno));
                server.aof_last_write_errno = errno;
            }
        } 
        //	如果nwritten不为-1，说明可能写入部分内容（未写完就出错了）
        else {
            if (can_log) {
                redisLog(REDIS_WARNING,"Short write while writing to "
                                       "the AOF file: (nwritten=%lld, "
                                       "expected=%lld)",
                                       (long long)nwritten,
                                       (long long)sdslen(server.aof_buf));
            }

            // 截断操作，尝试移除之前写入的不完整内容
            if (ftruncate(server.aof_fd, server.aof_current_size) == -1) {
                if (can_log) {
                    redisLog(REDIS_WARNING, "Could not remove short write "
                             "from the append-only file.  Redis may refuse "
                             "to load the AOF the next time it starts.  "
                             "ftruncate: %s", strerror(errno));
                }
            } else {
                /* If the ftruncate() succeeded we can set nwritten to
                 * -1 since there is no longer partial data into the AOF. */
            	// 如果ftruncate（）函数调用成功，则将nwritten设置为-1
                nwritten = -1;
            }
            server.aof_last_write_errno = ENOSPC;
        }

        /* Handle the AOF write error. */
        // 处理写AOF文件时出现的错误
        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            /* We can't recover when the fsync policy is ALWAYS since the
             * reply for the client is already in the output buffers, and we
             * have the contract with the user that on acknowledged write data
             * is synced on disk. */
            // 我们无法处理fsync策略为ALWAYS的情况，因为在该策略下客户端回复信息已经在输出缓存中了。
            redisLog(REDIS_WARNING,"Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            /* Recover from failed write leaving data into the buffer. However
             * set an error to stop accepting writes as long as the error
             * condition is not cleared. */
            server.aof_last_write_status = REDIS_ERR;

            /* Trim the sds buffer if there was a partial write, and there
             * was no way to undo it with ftruncate(2). */
            // 
            if (nwritten > 0) {
                server.aof_current_size += nwritten;
                sdsrange(server.aof_buf,nwritten,-1);
            }
            return; /* We'll try again on the next call... */
        }
    } 
    // 下面处理写入成功的情况
    else {
        /* Successful write(2). If AOF was in error state, restore the
         * OK state and log the event. */
    	// 写入成功，如果当前AOF状态为REDIS_ERR，则更新最后的写入状态并记录日志
        if (server.aof_last_write_status == REDIS_ERR) {
            redisLog(REDIS_WARNING,
                "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = REDIS_OK;
        }
    }
    // 更新当前AOF文件大小
    server.aof_current_size += nwritten;

    /* Re-use AOF buffer when it is small enough. The maximum comes from the
     * arena size of 4k minus some overhead (but is otherwise arbitrary). */
    // 如果AOF缓冲区足够小就重用该缓冲区，如果超过4000bytes则释放该缓冲区
    if ((sdslen(server.aof_buf)+sdsavail(server.aof_buf)) < 4000) {
        // 清空缓冲区中的内容，继续重用该缓冲区
        sdsclear(server.aof_buf);
    } else {
    	// 释放该缓冲区
        sdsfree(server.aof_buf);
       	// 新建一个缓冲区
        server.aof_buf = sdsempty();
    }

    /* Don't fsync if no-appendfsync-on-rewrite is set to yes and there are
     * children doing I/O in the background. */
    // 如果Redis的no-appendfsync-on-rewrite选项被开启，且后台有子进程正在执行IO操作，则不执行fsync操作，直接返回
    if (server.aof_no_fsync_on_rewrite &&
        (server.aof_child_pid != -1 || server.rdb_child_pid != -1))
            return;

    /* Perform the fsync if needed. */
    // 如果有需要，执行fsync操作
    
    //	当前的fsync策略为AOF_FSYNC_ALWAYS
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        /* aof_fsync is defined as fdatasync() for Linux in order to avoid
         * flushing metadata. */
        latencyStartMonitor(latency);
        aof_fsync(server.aof_fd); /* Let's try to get this data on the disk */
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-fsync-always",latency);
        server.aof_last_fsync = server.unixtime;
    } 
    // 如果当前的fsync策略为AOF_FSYNC_EVERYSEC，且距离上次fsync操作已经过去1秒
    else if ((server.aof_fsync == AOF_FSYNC_EVERYSEC &&
                server.unixtime > server.aof_last_fsync)) {
    	// 在后台执行fsync操作
        if (!sync_in_progress) aof_background_fsync(server.aof_fd);
        server.aof_last_fsync = server.unixtime;
    }
}

/*	根据传入命令和该命令的参数将其构造成符合AOF文件格式的字符串形式 	*/
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    // 构建格式为“*<count>\r\n"格式的字符串，count为命令参数个数
    buf[0] = '*';
    len = 1+ll2string(buf+1,sizeof(buf)-1,argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst,buf,len);

    // 重建命令，每个item的格式为“$<len>\r\n<content>\r\n”，其中<len>指明<content>的字符长度，<content>为参数内容
    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j]);
        buf[0] = '$';
        len = 1+ll2string(buf+1,sizeof(buf)-1,sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst,buf,len);
        dst = sdscatlen(dst,o->ptr,sdslen(o->ptr));
        dst = sdscatlen(dst,"\r\n",2);
        decrRefCount(o);
    }
    // 返回重建后的命令内容
    return dst;
}

/* Create the sds representation of an PEXPIREAT command, using
 * 'seconds' as time to live and 'cmd' to understand what command
 * we are translating into a PEXPIREAT.
 *	创建PEXPIREAT命令的字符串表示，其中参数seconds表示剩余的生存时间，参数cmd指明原命令的类型。
 *
 * This command is used in order to translate EXPIRE and PEXPIRE commands
 * into PEXPIREAT command so that we retain precision in the append only
 * file, and the time is always absolute and not relative. 
 *	这个函数会将EXPIRE、PEXPIRE命令转换为PEXPIREAT命令，这样就能在确保精确度一直的情况下将过期时间值转换为
 *	绝对（时间戳）值而不是相对值。*/
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
    long long when;
    robj *argv[3];

    /* Make sure we can use strtoll */
    // 从参数seconds中取出过期时间戳并转换为long long类型
    seconds = getDecodedObject(seconds);
    when = strtoll(seconds->ptr,NULL,10);
    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT */
    //	将过期时间统一转换为毫秒
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand)
    {
        when *= 1000;
    }
    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX */
    // 将过期时间由相对值转换为绝对值
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand)
    {
        when += mstime();
    }
    decrRefCount(seconds);

    // 构建PEXPIREAT命令
    argv[0] = createStringObject("PEXPIREAT",9);
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when);
    // 追加到buf指向的缓存中
    buf = catAppendOnlyGenericCommand(buf, 3, argv);
    decrRefCount(argv[0]);
    decrRefCount(argv[2]);
    return buf;
}

/* 	将命令还原后追加到AOF缓冲区server.aof_buf中，该缓冲区的内容将会在某个时刻被写入磁盘。
	另外，如果后台正在执行AOF文件重写操作，还需要将该命令追加到AOF重写缓存中。 */  
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc) {
    sds buf = sdsempty();
    robj *tmpargv[3];

    /* The DB this command was targeting is not the same as the last command
     * we appended. To issue a SELECT command is needed. */
    // 如果当前命令涉及的数据库与server.aof_selected_db指明的数据库不一致，需要加入SELECT命令显式设置
    if (dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb,sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
        server.aof_selected_db = dictid;
    }

    // 处理EXPIRE, SETEX, EXPIREAT命令
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == expireatCommand) {
        /* Translate EXPIRE/PEXPIRE/EXPIREAT into PEXPIREAT */
        // 将EXPIRE/PEXPIRE/EXPIREAT命令都转换为PEXPIREAT命令
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);
    } 
    // 处理SETEX、PSETEX命令
    else if (cmd->proc == setexCommand || cmd->proc == psetexCommand) {
        /* Translate SETEX/PSETEX to SET and PEXPIREAT */
        // 将SETEX/PSETEX命令转换为SET命令和PEXPIREAT命令
        tmpargv[0] = createStringObject("SET",3);
        tmpargv[1] = argv[1];
        tmpargv[2] = argv[3];
        buf = catAppendOnlyGenericCommand(buf,3,tmpargv);
        decrRefCount(tmpargv[0]);
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);
    } 
    // 其它命令使用catAppendOnlyGenericCommand（）函数处理
    else {
        /* All the other commands don't need translation or need the
         * same translation already operated in the command vector
         * for the replication itself. */
        // 所有其它命令并不需要转换操作或者已经完成转换
        buf = catAppendOnlyGenericCommand(buf,argc,argv);
    }

    /* Append to the AOF buffer. This will be flushed on disk just before
     * of re-entering the event loop, so before the client will get a
     * positive reply about the operation performed. */
    // 将重构后的命令字符串追加到AOF缓冲区中。AOF缓冲区中的数据会在重新进入时间循环前写入磁盘中，相应的客户端
    // 也会受到一个关于此次操作的回复消息
    if (server.aof_state == REDIS_AOF_ON)
        server.aof_buf = sdscatlen(server.aof_buf,buf,sdslen(buf));

    /* If a background append only file rewriting is in progress we want to
     * accumulate the differences between the child DB and the current one
     * in a buffer, so that when the child process will do its work we
     * can append the differences to the new append only file. */
    // 如果后台正在执行AOF文件重写操作（即BGREWRITEAOF命令），为了记录当前正在重写的AOF文件和当前数据库的
    // 差异信息，我们还需要将重构后的命令追加到AOF重写缓存中。
    if (server.aof_child_pid != -1)
        aofRewriteBufferAppend((unsigned char*)buf,sdslen(buf));

    sdsfree(buf);
}

/* ----------------------------------------------------------------------------
 * AOF loading    加载AOF文件
 * ------------------------------------------------------------------------- */

/* In Redis commands are always executed in the context of a client, so in
 * order to load the append only file we need to create a fake client. */
/*	在Redis中，命令必须由redisClient实例来执行，所以为了加载AOF文件需要创建一个伪Redis客户端。*/
struct redisClient *createFakeClient(void) {
    struct redisClient *c = zmalloc(sizeof(*c));

    selectDb(c,0);
    c->fd = -1;
    c->name = NULL;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->argc = 0;
    c->argv = NULL;
    c->bufpos = 0;
    c->flags = 0;
    c->btype = REDIS_BLOCKED_NONE;
    /* We set the fake client as a slave waiting for the synchronization
     * so that Redis will not try to send replies to this client. */
    // 将该客户端设置为正在等待同步的从节点，这样Redis就不会向该客户端发送回复了
    c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    c->watched_keys = listCreate();
    c->peerid = NULL;
    listSetFreeMethod(c->reply,decrRefCountVoid);
    listSetDupMethod(c->reply,dupClientReplyValue);
    initClientMultiState(c);
    return c;
}

// 释放伪客户端的命令空间，即argv数组
void freeFakeClientArgv(struct redisClient *c) {
    int j;

    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    zfree(c->argv);
}

/*	释放伪Redis客户端。*/
void freeFakeClient(struct redisClient *c) {
	// 释放查询缓存
    sdsfree(c->querybuf);
    // 释放回复缓存
    listRelease(c->reply);
    // 释放被监视的key列表
    listRelease(c->watched_keys);
    // 释放事务状态
    freeClientMultiState(c);
    // 释放客户端实例
    zfree(c);
}

/* Replay the append log file. On success REDIS_OK is returned. On non fatal
 * error (the append only file is zero-length) REDIS_ERR is returned. On
 * fatal error an error message is logged and the program exists. */
/*	执行AOF文件中的命令。如果操作成功则返回REDIS_OK。
	如果出现一些非致命性错误，比如AOF文件长度为0，则返回REDIS_ERR。
	如果出现致命性错误，则记录错误信息并且退出程序。*/
int loadAppendOnlyFile(char *filename) {
	// 伪客户端
    struct redisClient *fakeClient;
    // 打开AOF文件
    FILE *fp = fopen(filename,"r");
    struct redis_stat sb;
    int old_aof_state = server.aof_state;
    long loops = 0;
    off_t valid_up_to = 0; /* Offset of the latest well-formed command loaded. */

    // 检查AOF文件
    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        fclose(fp);
        return REDIS_ERR;
    }

    // 检查文件句柄，即判断AOF文件是否打开
    if (fp == NULL) {
        redisLog(REDIS_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    /* Temporarily disable AOF, to prevent EXEC from feeding a MULTI
     * to the same file we're about to read. */
    // 暂时关闭AOF功能，防止在执行MULTI命令时，EXEC命令被传播到当前被打开的AOF文件中
    server.aof_state = REDIS_AOF_OFF;

    // 创建伪客户端
    fakeClient = createFakeClient();
    //	设置相关的全局状态位，startLoading函数定义在rdb.c中
    startLoading(fp);

    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        /* Serve the clients from time to time */
        // 间隔性处理客户端请求
        if (!(loops++ % 1000)) {
            loadingProgress(ftello(fp));
            processEventsWhileBlocked();
        }

        // 将文件中的内容读出到buf缓冲区中，fgets只读取一行内容
        if (fgets(buf,sizeof(buf),fp) == NULL) {
        	// 如果文件内容已经读取完毕，跳出while循环
            if (feof(fp))
                break;
            else
                goto readerr;
        }
        // 检查命令格式
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        // 取出命令参数个数
        argc = atoi(buf+1);
        // 检查命令参数个数是否合法
        if (argc < 1) goto fmterr;

        // 分配空间用来存放读取出来的命令
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        // 从AOF文件中解析出命令及相关的参数
        for (j = 0; j < argc; j++) {
        	// 每次读取一行
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            if (buf[0] != '$') goto fmterr;
            // 读取参数值的长度信息
            len = strtol(buf+1,NULL,10);
            // 读取对应的参数值
            argsds = sdsnewlen(NULL,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            // 根据上面读出来的参数创建字符串对象
            argv[j] = createObject(REDIS_STRING,argsds);
            if (fread(buf,2,1,fp) == 0) {
                fakeClient->argc = j+1; /* Free up to j. */
                freeFakeClientArgv(fakeClient);
                goto readerr; /* discard CRLF */
            }
        }

        /* Command lookup */
        // 在命令表中查找命令，也就是判断是否存在该命令
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            redisLog(REDIS_WARNING,"Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
            exit(1);
        }

        /* Run the command in the context of a fake client */
        // 让伪客户端执行命令
        cmd->proc(fakeClient);

        /* The fake client should not have a reply */
        // 伪客户端不能有回复
        redisAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
        /* The fake client should never get blocked */
        // 伪客户端不能被blocked（阻塞）
        redisAssert((fakeClient->flags & REDIS_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        // 清除操作，释放客户端的argv数组
        freeFakeClientArgv(fakeClient);
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, log error and quit. */
    // 如果能够执行到这里，说明AOF文件的内容已经全部被正确处理
    // 接下来检查客户端是不是处于MULTI事务中
    if (fakeClient->flags & REDIS_MULTI) goto uxeof;

loaded_ok: /* DB loaded, cleanup and return REDIS_OK to the caller. */
    // 到这里，已经将AOF文件中的命令全部执行完毕，返回REDIS_OK

    // 关闭AOF文件
    fclose(fp);
    // 释放伪客户端
    freeFakeClient(fakeClient);
    // 复位
    server.aof_state = old_aof_state;
    stopLoading();
    aofUpdateCurrentSize();
    server.aof_rewrite_base_size = server.aof_current_size;
    return REDIS_OK;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    // 文件内容出错
    if (!feof(fp)) {
        redisLog(REDIS_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);
    }

uxeof: /* Unexpected AOF end of file. */
    // 非预期的文件末尾
    if (server.aof_load_truncated) {
        redisLog(REDIS_WARNING,"!!! Warning: short read while loading the AOF file !!!");
        redisLog(REDIS_WARNING,"!!! Truncating the AOF at offset %llu !!!",
            (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(filename,valid_up_to) == -1) {
            if (valid_up_to == -1) {
                redisLog(REDIS_WARNING,"Last valid command offset is invalid");
            } else {
                redisLog(REDIS_WARNING,"Error truncating the AOF file: %s",
                    strerror(errno));
            }
        } else {
            /* Make sure the AOF file descriptor points to the end of the
             * file after the truncate call. */
            if (server.aof_fd != -1 && lseek(server.aof_fd,0,SEEK_END) == -1) {
                redisLog(REDIS_WARNING,"Can't seek the end of the AOF file: %s",
                    strerror(errno));
            } else {
                redisLog(REDIS_WARNING,
                    "AOF loaded anyway because aof-load-truncated is enabled");
                goto loaded_ok;
            }
        }
    }
    redisLog(REDIS_WARNING,"Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

fmterr: /* Format error. */
    // 文件内容格式错误
    redisLog(REDIS_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

/* ----------------------------------------------------------------------------
 * AOF rewrite    AOF重写
 * ------------------------------------------------------------------------- */

/*	下面这些函数定义在rio.h文件中
		size_t rioWriteBulkCount(rio *r, char prefix, int count);
		size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
		size_t rioWriteBulkLongLong(rio *r, long long l);
		size_t rioWriteBulkDouble(rio *r, double d);
*/

/* Delegate writing an object to writing a bulk string or bulk long long.
 * This is not placed in rio.c since that adds the redis.h dependency. */
/*	将参数obj中的字符串或long long类型整数值写入rio对象中。 */
int rioWriteBulkObject(rio *r, robj *obj) {
    /* Avoid using getDecodedObject to help copy-on-write (we are often
     * in a child process when this function is called). */
    if (obj->encoding == REDIS_ENCODING_INT) {
        return rioWriteBulkLongLong(r,(long)obj->ptr);
    } else if (sdsEncodedObject(obj)) {
        return rioWriteBulkString(r,obj->ptr,sdslen(obj->ptr));
    } else {
        redisPanic("Unknown string encoding");
    }
}

/* Emit the commands needed to rebuild a list object.
 * The function returns 0 on error, 1 on success. */
/*	将重建列表list对象需要的命令（即RPUSH命令）写入rio对象中。
	该函数出错返回0，成功返回1。*/
int rewriteListObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = listTypeLength(o);

    // 处理ziplist编码的list对象
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *p = ziplistIndex(zl,0);
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        // 在AOF文件中，每条RPUSH命令只能添加REDIS_AOF_REWRITE_ITEMS_PER_CMD个元素
        // 这里遍历ziplist，将每REDIS_AOF_REWRITE_ITEMS_PER_CMD个元素组装到一条RPUSH命令中去
        // 想想为什么要这么做？如果list对象中存在大量的元素，将它们放到一条RPUSH命令中会如何
        while(ziplistGet(p,&vstr,&vlen,&vlong)) {
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"RPUSH",5) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }

            // 取出元素值并写入rio对象中
            if (vstr) {
                if (rioWriteBulkString(r,(char*)vstr,vlen) == 0) return 0;
            } else {
                if (rioWriteBulkLongLong(r,vlong) == 0) return 0;
            }
            // 移动迭代器，除以下一个元素
            p = ziplistNext(zl,p);
            // 取出元素个数加1，如果取出元素个数等于REDIS_AOF_REWRITE_ITEMS_PER_CMD规定的数量
            // 则剩余元素放到另一条RPUSH命令中
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } 
    // 处理linked list编码的list对象
    else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        list *list = o->ptr;
        listNode *ln;
        listIter li;

        // 类似ziplist的处理方式，遍历linked list将每REDIS_AOF_REWRITE_ITEMS_PER_CMD个元素组装到一条RPUSH命令中
        listRewind(list,&li);
        while((ln = listNext(&li))) {
            robj *eleobj = listNodeValue(ln);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"RPUSH",5) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else {
        redisPanic("Unknown list encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a set object.
 * The function returns 0 on error, 1 on success. */
/*	将重建集合set对象需要的命令（即SADD命令）写入rio对象中。
	该函数出错返回0，成功返回1。
	
	这里的处理方式和list类型对象一直：遍历set对象，将每REDIS_AOF_REWRITE_ITEMS_PER_CMD个元素
	组成成一条SADD命令写入rio对象中。
*/
int rewriteSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = setTypeSize(o);

    // 处理inset编码的set对象
    if (o->encoding == REDIS_ENCODING_INTSET) {
        int ii = 0;
        int64_t llval;

        while(intsetGet(o->ptr,ii++,&llval)) {
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"SADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkLongLong(r,llval) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } 
    // 处理dict编码的set对象
    else if (o->encoding == REDIS_ENCODING_HT) {
        dictIterator *di = dictGetIterator(o->ptr);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetKey(de);
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"SADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown set encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a sorted set object.
 * The function returns 0 on error, 1 on success. */
/*	将重建有序集合zset对象需要的命令（即ZADD命令）写入rio对象中。
	该函数出错返回0，成功返回1。
	
	这里的处理方式和list类型对象一直：遍历zset对象，将每REDIS_AOF_REWRITE_ITEMS_PER_CMD个元素
	组成成一条ZADD命令写入rio对象中。
*/
int rewriteSortedSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = zsetLength(o);

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;
        double score;

        eptr = ziplistIndex(zl,0);
        redisAssert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        while (eptr != NULL) {
            redisAssert(ziplistGet(eptr,&vstr,&vlen,&vll));
            score = zzlGetScore(sptr);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
                if (rioWriteBulkString(r,"ZADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            // 写入分值score
            if (rioWriteBulkDouble(r,score) == 0) return 0;
            // 写入元素值
            if (vstr != NULL) {
                if (rioWriteBulkString(r,(char*)vstr,vlen) == 0) return 0;
            } else {
                if (rioWriteBulkLongLong(r,vll) == 0) return 0;
            }
            zzlNext(zl,&eptr,&sptr);
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetKey(de);
            double *score = dictGetVal(de);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
                if (rioWriteBulkString(r,"ZADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            // 写入分值score
            if (rioWriteBulkDouble(r,*score) == 0) return 0;
            // 写入元素值
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown sorted zset encoding");
    }
    return 1;
}

/* Write either the key or the value of the currently selected item of a hash.
 * The 'hi' argument passes a valid Redis hash iterator.
 * The 'what' filed specifies if to write a key or a value and can be
 * either REDIS_HASH_KEY or REDIS_HASH_VALUE.
 *
 * The function returns 0 on error, non-zero on success. */
/*	将哈希表中一个键值对的key值或value值写入rio对象中。
	参数hi是字典dict的一个迭代器。
	参数what指出写入key值还是写入value值，其取值有REDIS_HASH_KEY（只写入key值）和REDIS_HASH_VALUE（写入key值和value值）

	该函数如果出错返回0，如果成功返回非0值。*/
static int rioWriteHashIteratorCursor(rio *r, hashTypeIterator *hi, int what) {
	// 处理ziplist编码的情况
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            return rioWriteBulkString(r, (char*)vstr, vlen);
        } else {
            return rioWriteBulkLongLong(r, vll);
        }

    } 
    // 处理dict编码的情况
    else if (hi->encoding == REDIS_ENCODING_HT) {
        robj *value;

        hashTypeCurrentFromHashTable(hi, what, &value);
        return rioWriteBulkObject(r, value);
    }

    redisPanic("Unknown hash encoding");
    return 0;
}

/* Emit the commands needed to rebuild a hash object.
 * The function returns 0 on error, 1 on success. */
/*	将重建有序集合zset对象需要的命令（即HMSET命令）写入rio对象中。
	该函数出错返回0，成功返回1。
	
	这里的处理方式和list类型对象一直：遍历hash对象，将每REDIS_AOF_REWRITE_ITEMS_PER_CMD个元素
	组成成一条HMSET命令写入rio对象中。
*/
int rewriteHashObject(rio *r, robj *key, robj *o) {
    hashTypeIterator *hi;
    long long count = 0, items = hashTypeLength(o);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != REDIS_ERR) {
        if (count == 0) {
            int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

            if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
            if (rioWriteBulkString(r,"HMSET",5) == 0) return 0;
            if (rioWriteBulkObject(r,key) == 0) return 0;
        }

        if (rioWriteHashIteratorCursor(r, hi, REDIS_HASH_KEY) == 0) return 0;
        if (rioWriteHashIteratorCursor(r, hi, REDIS_HASH_VALUE) == 0) return 0;
        if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
        items--;
    }

    hashTypeReleaseIterator(hi);

    return 1;
}

/* This function is called by the child rewriting the AOF file to read
 * the difference accumulated from the parent into a buffer, that is
 * concatenated at the end of the rewrite. */
/*	该函数由执行AOF文件重写操作的子进程调用，将父进程记录在缓存的差异化数据读入到buffer中，最后将buffer中
	的数据连接到server.aof_child_diff中。
	该函数返回从父进程读出的字节数。*/
ssize_t aofReadDiffFromParent(void) {
    char buf[65536]; /* Default pipe buffer size on most Linux systems. */
    ssize_t nread, total = 0;

    while ((nread =
            read(server.aof_pipe_read_data_from_parent,buf,sizeof(buf))) > 0) {
        server.aof_child_diff = sdscatlen(server.aof_child_diff,buf,nread);
        total += nread;
    }
    return total;
}

/* Write a sequence of commands able to fully rebuild the dataset into
 * "filename". Used both by REWRITEAOF and BGREWRITEAOF.
 *	将一系列足以重建数据集的命令写入到filename指定的文件中，该函数将被REWRITEAOF和BGREWRITEAOF命令调用。
 *
 * In order to minimize the number of commands needed in the rewritten
 * log Redis uses variadic commands when possible, such as RPUSH, SADD
 * and ZADD. However at max REDIS_AOF_REWRITE_ITEMS_PER_CMD items per time
 * are inserted using a single command. 
 *	为了最小化写入的命令数量，Redis会尽可能使用如RPUSH、SADD和ZADD等具有可变参数的命令。但是每条命令处理的元素
 *	数量最多为REDIS_AOF_REWRITE_ITEMS_PER_CMD。
 */
int rewriteAppendOnlyFile(char *filename) {
    dictIterator *di = NULL;
    dictEntry *de;
    rio aof;
    FILE *fp;
    char tmpfile[256];
    int j;
    long long now = mstime();
    char byte;
    size_t processed = 0;

    /* Note that we have to use a different temp name here compared to the
     * one used by rewriteAppendOnlyFileBackground() function. */
    // 创建临时文件，注意到这里的临时文件名和rewriteAppendOnlyFileBackground函数中的临时文件名不同
    snprintf(tmpfile,256,"temp-rewriteaof-%d.aof", (int) getpid());
    // 打开临时文件
    fp = fopen(tmpfile,"w");
    if (!fp) {
    	// 打开失败
        redisLog(REDIS_WARNING, "Opening the temp file for AOF rewrite in rewriteAppendOnlyFile(): %s", strerror(errno));
        return REDIS_ERR;
    }

    server.aof_child_diff = sdsempty();
    // 初始化文件rio对象
    rioInitWithFile(&aof,fp);
    // 每写入REDIS_AOF_AUTOSYNC_BYTES个字节数据就执行一个sync同步操作
    if (server.aof_rewrite_incremental_fsync)
        rioSetAutoSync(&aof,REDIS_AOF_AUTOSYNC_BYTES);
    // 遍历所有的数据库，重构命令
    for (j = 0; j < server.dbnum; j++) {
    	// SELECT命令
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
        // 指向当前数据库
        redisDb *db = server.db+j;
        // 指向当前数据库的键空间
        dict *d = db->dict;
        // 如果当前键空间为空，处理下一个数据库
        if (dictSize(d) == 0) continue;
        // 创建键空间的迭代器
        di = dictGetSafeIterator(d);
        if (!di) {
            fclose(fp);
            return REDIS_ERR;
        }

        /* SELECT the new DB */
        // 写入SELECT命令，确保数据恢复到相应数据库中
        if (rioWrite(&aof,selectcmd,sizeof(selectcmd)-1) == 0) goto werr;
        if (rioWriteBulkLongLong(&aof,j) == 0) goto werr;

        /* Iterate this DB writing every entry */
        // 遍历键空间中的所有key
        while((de = dictNext(di)) != NULL) {
            sds keystr;
            robj key, *o;
            long long expiretime;

            // 取出key值
            keystr = dictGetKey(de);
            // 取出对应的value值
            o = dictGetVal(de);
            initStaticStringObject(key,keystr);

            // 取出该key的过期时间
            expiretime = getExpire(db,&key);

            /* If this key is already expired skip it */
            // 如果该key已经过期，则跳过该key
            if (expiretime != -1 && expiretime < now) continue;

            /* Save the key and associated value */
            // 根据value值对象的类型还远成相应的命令进行保存

            // 处理string类型对象
            if (o->type == REDIS_STRING) {
                /* Emit a SET command */
                // 构造SET命令来保存string类型对象
                char cmd[]="*3\r\n$3\r\nSET\r\n";
                if (rioWrite(&aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                /* Key and value */
                //	保存key值和value值
                if (rioWriteBulkObject(&aof,&key) == 0) goto werr;
                if (rioWriteBulkObject(&aof,o) == 0) goto werr;
            } 
            // 保存list类型对象
            else if (o->type == REDIS_LIST) {
                if (rewriteListObject(&aof,&key,o) == 0) goto werr;
            } 
            // 保存set类型对象
            else if (o->type == REDIS_SET) {
                if (rewriteSetObject(&aof,&key,o) == 0) goto werr;
            } 
            //	保存zset类型对象
            else if (o->type == REDIS_ZSET) {
                if (rewriteSortedSetObject(&aof,&key,o) == 0) goto werr;
            } 
            //	保存hash类型对象
            else if (o->type == REDIS_HASH) {
                if (rewriteHashObject(&aof,&key,o) == 0) goto werr;
            } else {
                redisPanic("Unknown object type");
            }
            // 使用PEXPIREAT命令保存该key的过期时间
            /* Save the expire time */
            if (expiretime != -1) {
                char cmd[]="*3\r\n$9\r\nPEXPIREAT\r\n";
                if (rioWrite(&aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                if (rioWriteBulkObject(&aof,&key) == 0) goto werr;
                if (rioWriteBulkLongLong(&aof,expiretime) == 0) goto werr;
            }
            /* Read some diff from the parent process from time to time. */
            if (aof.processed_bytes > processed+1024*10) {
                processed = aof.processed_bytes;
                aofReadDiffFromParent();
            }
        }
        dictReleaseIterator(di);
        di = NULL;
    }

    /* Do an initial slow fsync here while the parent is still sending
     * data, in order to make the next final fsync faster. */
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;

    /* Read again a few times to get more data from the parent.
     * We can't read forever (the server may receive data from clients
     * faster than it is able to send data to the child), so we try to read
     * some more data in a loop as soon as there is a good chance more data
     * will come. If it looks like we are wasting time, we abort (this
     * happens after 20 ms without new data). */
    int nodata = 0;
    mstime_t start = mstime();
    while(mstime()-start < 1000 && nodata < 20) {
        if (aeWait(server.aof_pipe_read_data_from_parent, AE_READABLE, 1) <= 0)
        {
            nodata++;
            continue;
        }
        nodata = 0; /* Start counting from zero, we stop on N *contiguous*
                       timeouts. */
        aofReadDiffFromParent();
    }

    /* Ask the master to stop sending diffs. */
    // 告诉父进程停止发送数据
    if (write(server.aof_pipe_write_ack_to_parent,"!",1) != 1) goto werr;
    if (anetNonBlock(NULL,server.aof_pipe_read_ack_from_parent) != ANET_OK)
        goto werr;
    /* We read the ACK from the server using a 10 seconds timeout. Normally
     * it should reply ASAP, but just in case we lose its reply, we are sure
     * the child will eventually get terminated. */
    if (syncRead(server.aof_pipe_read_ack_from_parent,&byte,1,5000) != 1 ||
        byte != '!') goto werr;
    redisLog(REDIS_NOTICE,"Parent agreed to stop sending diffs. Finalizing AOF...");

    /* Read the final diff if any. */
	// 读取差异化数据
    aofReadDiffFromParent();

    /* Write the received diff to the file. */
    // 将接收到的差异化数据写入AOF文件中
    redisLog(REDIS_NOTICE,
        "Concatenating %.2f MB of AOF diff received from parent.",
        (double) sdslen(server.aof_child_diff) / (1024*1024));
    if (rioWrite(&aof,server.aof_child_diff,sdslen(server.aof_child_diff)) == 0)
        goto werr;

    /* Make sure data will not remain on the OS's output buffers */
    // 确保系统缓冲区中的数据已经保存到文件中
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    // 文件重命令
    if (rename(tmpfile,filename) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp append only file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }
    redisLog(REDIS_NOTICE,"SYNC append only file rewrite performed");
    return REDIS_OK;

werr:
    redisLog(REDIS_WARNING,"Write error writing append only file on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    if (di) dictReleaseIterator(di);
    return REDIS_ERR;
}

/* ----------------------------------------------------------------------------
 * AOF rewrite pipes for IPC
 * -------------------------------------------------------------------------- */

/* This event handler is called when the AOF rewriting child sends us a
 * single '!' char to signal we should stop sending buffer diffs. The
 * parent sends a '!' as well to acknowledge. */
void aofChildPipeReadable(aeEventLoop *el, int fd, void *privdata, int mask) {
    char byte;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(privdata);
    REDIS_NOTUSED(mask);

    if (read(fd,&byte,1) == 1 && byte == '!') {
        redisLog(REDIS_NOTICE,"AOF rewrite child asks to stop sending diffs.");
        server.aof_stop_sending_diff = 1;
        if (write(server.aof_pipe_write_ack_to_child,"!",1) != 1) {
            /* If we can't send the ack, inform the user, but don't try again
             * since in the other side the children will use a timeout if the
             * kernel can't buffer our write, or, the children was
             * terminated. */
            redisLog(REDIS_WARNING,"Can't send ACK to AOF child: %s",
                strerror(errno));
        }
    }
    /* Remove the handler since this can be called only one time during a
     * rewrite. */
    aeDeleteFileEvent(server.el,server.aof_pipe_read_ack_from_child,AE_READABLE);
}

/* Create the pipes used for parent - child process IPC during rewrite.
 * We have a data pipe used to send AOF incremental diffs to the child,
 * and two other pipes used by the children to signal it finished with
 * the rewrite so no more data should be written, and another for the
 * parent to acknowledge it understood this new condition. */
/*	创建父子进程通信用的匿名管道 */
int aofCreatePipes(void) {
    int fds[6] = {-1, -1, -1, -1, -1, -1};
    int j;

    if (pipe(fds) == -1) goto error; /* parent -> children data. */
    if (pipe(fds+2) == -1) goto error; /* children -> parent ack. */
    if (pipe(fds+4) == -1) goto error; /* children -> parent ack. */
    /* Parent -> children data is non blocking. */
    if (anetNonBlock(NULL,fds[0]) != ANET_OK) goto error;
    if (anetNonBlock(NULL,fds[1]) != ANET_OK) goto error;
    if (aeCreateFileEvent(server.el, fds[2], AE_READABLE, aofChildPipeReadable, NULL) == AE_ERR) goto error;

    server.aof_pipe_write_data_to_child = fds[1];
    server.aof_pipe_read_data_from_parent = fds[0];
    server.aof_pipe_write_ack_to_parent = fds[3];
    server.aof_pipe_read_ack_from_child = fds[2];
    server.aof_pipe_write_ack_to_child = fds[5];
    server.aof_pipe_read_ack_from_parent = fds[4];
    server.aof_stop_sending_diff = 0;
    return REDIS_OK;

error:
    redisLog(REDIS_WARNING,"Error opening /setting AOF rewrite IPC pipes: %s",
        strerror(errno));
    for (j = 0; j < 6; j++) if(fds[j] != -1) close(fds[j]);
    return REDIS_ERR;
}

void aofClosePipes(void) {
    aeDeleteFileEvent(server.el,server.aof_pipe_read_ack_from_child,AE_READABLE);
    aeDeleteFileEvent(server.el,server.aof_pipe_write_data_to_child,AE_WRITABLE);
    close(server.aof_pipe_write_data_to_child);
    close(server.aof_pipe_read_data_from_parent);
    close(server.aof_pipe_write_ack_to_parent);
    close(server.aof_pipe_read_ack_from_child);
    close(server.aof_pipe_write_ack_to_child);
    close(server.aof_pipe_read_ack_from_parent);
}

/* ----------------------------------------------------------------------------
 * AOF background rewrite    后台重写AOF文件
 * ------------------------------------------------------------------------- */

/* This is how rewriting of the append only file in background works:
 *	这是后台重写AOF文件的流程
 *
 * 1) The user calls BGREWRITEAOF
 *	（1）、用户调用BGREWRITEAOF命令
 * 2) Redis calls this function, that forks():
 *    2a) the child rewrite the append only file in a temp file.
 *    2b) the parent accumulates differences in server.aof_rewrite_buf.
 *	（2）Reis调用下面这个函数，该函数调用forks（）创建子进程
 *		a、子进程在一个临时文件上执行AOF文件重写操作
 *		b、父进程将AOF命令重写期间的写命令追加到server.aof_rewrite_buf缓冲区中
 * 3) When the child finished '2a' exists.
 *		子进程执行完AOF重写命令后退出
 * 4) The parent will trap the exit code, if it's OK, will append the
 *    data accumulated into server.aof_rewrite_buf into the temp file, and
 *    finally will rename(2) the temp file in the actual file name.
 *    The the new file is reopened as the new append only file. Profit!
 *		父进程会捕获子进程的退出码，如果退出码为OK，父进程就将server.aof_rewrite_buf中的数据写入临时文件中，
 *		最后对临时文件重命名。
 */
int rewriteAppendOnlyFileBackground(void) {
    pid_t childpid;
    long long start;

    // 当前正在执行AOF文件重写操作，直接退出
    if (server.aof_child_pid != -1) return REDIS_ERR;
    // 创建父子进程间通信用的匿名管道
    if (aofCreatePipes() != REDIS_OK) return REDIS_ERR;
    // 记录当前时间
    start = ustime();
    // 调用forks函数创建子进程
    if ((childpid = fork()) == 0) {
        char tmpfile[256];

        /* Child */
        /*	子进程执行下面代码	*/

        // 子进程不需要处理网络连接，关闭之
        closeListeningSockets(0);
        // 为子进程设置名称
        redisSetProcTitle("redis-aof-rewrite");
        // 创建临时文件
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) getpid());
        // AOF文件重写
        if (rewriteAppendOnlyFile(tmpfile) == REDIS_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                redisLog(REDIS_NOTICE,
                    "AOF rewrite: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }
            // 重写成功
            exitFromChild(0);
        } else {
        	// 重写失败
            exitFromChild(1);
        }
    } else {
        /* Parent */
        /*	父进程执行下面的代码	*/

        // 记录fork（）函数执行的时间
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);

        // 处理fork执行失败的情况
        if (childpid == -1) {
            redisLog(REDIS_WARNING,
                "Can't rewrite append only file in background: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }
        redisLog(REDIS_NOTICE,
            "Background append only file rewriting started by pid %d",childpid);
        // 记录AOF重写的相关状态
        server.aof_rewrite_scheduled = 0;
        server.aof_rewrite_time_start = time(NULL);
        server.aof_child_pid = childpid;
        updateDictResizePolicy();
        /* We set appendseldb to -1 in order to force the next call to the
         * feedAppendOnlyFile() to issue a SELECT command, so the differences
         * accumulated by the parent into server.aof_rewrite_buf will start
         * with a SELECT statement and it will be safe to merge. */
        //	将server.aof_selected_db设置为-1，这样当feedAppendOnlyFile（）下次执行时引发一个SELECT命令。
        // 	这样父进程存放在server.aof_rewrite_buf的命令会恢复到正确的数据库中。
        server.aof_selected_db = -1;
        replicationScriptCacheFlush();
        return REDIS_OK;
    }
    return REDIS_OK; /* unreached */
}

/*	BGREWRITEAOF命令实现	*/
void bgrewriteaofCommand(redisClient *c) {
	// 当前有BGREWRITEAOF命令正在执行
    if (server.aof_child_pid != -1) {
        addReplyError(c,"Background append only file rewriting already in progress");
    } 
    // 当前有BGSAVE命令正在执行
    else if (server.rdb_child_pid != -1) {
        server.aof_rewrite_scheduled = 1;
        addReplyStatus(c,"Background append only file rewriting scheduled");
    } 
    //	执行BGREWRITEAOF命令
    else if (rewriteAppendOnlyFileBackground() == REDIS_OK) {
        addReplyStatus(c,"Background append only file rewriting started");
    } else {
        addReply(c,shared.err);
    }
}

/*	删除AOF文件重写操作过程中产生的临时文件	*/
void aofRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) childpid);
    unlink(tmpfile);
}

/* Update the server.aof_current_size field explicitly using stat(2)
 * to check the size of the file. This is useful after a rewrite or after
 * a restart, normally the size is updated just adding the write length
 * to the current length, that is much faster. */
/*	使用stat(2)函数计算AOF文件的大小并更新到server.aof_current_size字段中。
	这个操作通常在AOF文件重写之后或服务器重启之后执行，只是简单的赋值操作，速度很快。*/
void aofUpdateCurrentSize(void) {
    struct redis_stat sb;
    mstime_t latency;

    latencyStartMonitor(latency);
    if (redis_fstat(server.aof_fd,&sb) == -1) {
        redisLog(REDIS_WARNING,"Unable to obtain the AOF file length. stat: %s",
            strerror(errno));
    } else {
        server.aof_current_size = sb.st_size;
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("aof-fstat",latency);
}

/* A background append only file rewriting (BGREWRITEAOF) terminated its work.
 * Handle this. */
/*	当子进程完成AOF文件重写操作后，父进程调用该函数进行处理	*/
void backgroundRewriteDoneHandler(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        int newfd, oldfd;
        char tmpfile[256];
        long long now = ustime();
        mstime_t latency;

        redisLog(REDIS_NOTICE,
            "Background AOF rewrite terminated with success");

        /* Flush the differences accumulated by the parent to the
         * rewritten AOF. */
        // 将父进程中记录在重写缓存中的数据追加到AOF文件中
        latencyStartMonitor(latency);
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof",
            (int)server.aof_child_pid);
        // 打开临时文件
        newfd = open(tmpfile,O_WRONLY|O_APPEND);
        if (newfd == -1) {
            redisLog(REDIS_WARNING,
                "Unable to open the temporary AOF produced by the child: %s", strerror(errno));
            goto cleanup;
        }

        // 将重写缓存中的数据追加到AOF文件中
        if (aofRewriteBufferWrite(newfd) == -1) {
            redisLog(REDIS_WARNING,
                "Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
            close(newfd);
            goto cleanup;
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rewrite-diff-write",latency);

        redisLog(REDIS_NOTICE,
            "Residual parent diff successfully flushed to the rewritten AOF (%.2f MB)", (double) aofRewriteBufferSize() / (1024*1024));

        /* The only remaining thing to do is to rename the temporary file to
         * the configured file and switch the file descriptor used to do AOF
         * writes. We don't want close(2) or rename(2) calls to block the
         * server on old file deletion.
         *	剩下的事情就是将临时文件重命名为指定的名称，并切换该文件的文件描述符为AOF重写文件。
         *	我们不想让close(2)和rename(2)函数在删除旧文件时阻塞服务器。
         *
         * There are two possible scenarios:
         *	这里有两个可能的情景：
         *
         * 1) AOF is DISABLED and this was a one time rewrite. The temporary
         * file will be renamed to the configured file. When this file already
         * exists, it will be unlinked, which may block the server.
         *	如果AOF被关闭，且这是一次单词重写操作，临时文件会被命名为指定的文件名。如果AOF文件已经存在，
         *	则会被unlink掉，这个操作可能会阻塞服务器。
         *
         * 2) AOF is ENABLED and the rewritten AOF will immediately start
         * receiving writes. After the temporary file is renamed to the
         * configured file, the original AOF file descriptor will be closed.
         * Since this will be the last reference to that file, closing it
         * causes the underlying file to be unlinked, which may block the
         * server.
         *	如果AOF被开启，并且重写后的AOF文件会马上被用来接收写命令。当临时文件被重命名为指定的名称后，原来
         *	旧的文件描述符将会被关闭。因为Redis是最后一个引用该文件的进程，所以关闭这个文件会造成该文件被
         *	unlink，这也可能阻塞服务器
         *
         * To mitigate the blocking effect of the unlink operation (either
         * caused by rename(2) in scenario 1, or by close(2) in scenario 2), we
         * use a background thread to take care of this. First, we
         * make scenario 1 identical to scenario 2 by opening the target file
         * when it exists. The unlink operation after the rename(2) will then
         * be executed upon calling close(2) for its descriptor. Everything to
         * guarantee atomicity for this switch has already happened by then, so
         * we don't care what the outcome or duration of that close operation
         * is, as long as the file descriptor is released again. 
         *	为了避免unlink操作造成服务器阻塞，这里使用一个后台线程来执行close(2)操作。
         *	如果原来的文件存在，先打开原来文件这样就可以将场景1和场景2等同考虑。
         *	那么rename操作后，因为原来的文件是打开的，所以不会unlink。
         *	将unlink推迟到关闭原来文件的描述符时。
         *	最后，将close()操作放到异步IO线程执行
         */

        if (server.aof_fd == -1) {
            /* AOF disabled */
            // AOF关闭

             /* Don't care if this fails: oldfd will be -1 and we handle that.
              * One notable case of -1 return is if the old file does
              * not exist. */
             // 打开已存在的文件
             oldfd = open(server.aof_filename,O_RDONLY|O_NONBLOCK);
        } else {
            /* AOF enabled */
            // AOF开启
            oldfd = -1; /* We'll set this to the current AOF filedes later. */
        }

        /* Rename the temporary file. This will not unlink the target file if
         * it exists, because we reference it with "oldfd". */
        latencyStartMonitor(latency);
        // 对临时文件重命名。这是旧的AOF文件（如果存在）不会被unlink掉，因为oldfd引用它
        if (rename(tmpfile,server.aof_filename) == -1) {
            redisLog(REDIS_WARNING,
                "Error trying to rename the temporary AOF file: %s", strerror(errno));
            close(newfd);
            if (oldfd != -1) close(oldfd);
            goto cleanup;
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rename",latency);

        if (server.aof_fd == -1) {
            /* AOF disabled, we don't need to set the AOF file descriptor
             * to this new file, so we can close it. */
        	// 如果AOF被关闭，则直接关闭AOF文件
            close(newfd);
        } else {
            /* AOF enabled, replace the old fd with the new one. */
            // 如果AOF被开启，用新的AOF文件的fd替代旧的AOF文件的fd
            oldfd = server.aof_fd;
            server.aof_fd = newfd;
            // 再次执行同步操作（前面讲AOF重写缓存中的数据追加到AOF文件中）
            if (server.aof_fsync == AOF_FSYNC_ALWAYS)
                aof_fsync(newfd);
            else if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
                aof_background_fsync(newfd);

            // 强制引发SELECT
            server.aof_selected_db = -1; /* Make sure SELECT is re-issued */
            aofUpdateCurrentSize();
            server.aof_rewrite_base_size = server.aof_current_size;

            /* Clear regular AOF buffer since its contents was just written to
             * the new AOF from the background rewrite buffer. */
            // 清空AOF缓冲区，因为缓冲区中的内容已经写入到了AOF文件中了
            sdsfree(server.aof_buf);
            server.aof_buf = sdsempty();
        }

        server.aof_lastbgrewrite_status = REDIS_OK;

        redisLog(REDIS_NOTICE, "Background AOF rewrite finished successfully");
        /* Change state from WAIT_REWRITE to ON if needed */
        if (server.aof_state == REDIS_AOF_WAIT_REWRITE)
            server.aof_state = REDIS_AOF_ON;

        /* Asynchronously close the overwritten AOF. */
        // 异步关闭旧AOF文件
        if (oldfd != -1) bioCreateBackgroundJob(REDIS_BIO_CLOSE_FILE,(void*)(long)oldfd,NULL,NULL);

        redisLog(REDIS_VERBOSE,
            "Background AOF rewrite signal handler took %lldus", ustime()-now);
    } 
    // AOF重写出错
    else if (!bysignal && exitcode != 0) {
        server.aof_lastbgrewrite_status = REDIS_ERR;

        redisLog(REDIS_WARNING,
            "Background AOF rewrite terminated with error");
    } else {
        server.aof_lastbgrewrite_status = REDIS_ERR;

        redisLog(REDIS_WARNING,
            "Background AOF rewrite terminated by signal %d", bysignal);
    }

cleanup:
	// 释放匿名管道
    aofClosePipes();
    // 重置AOF重写缓存
    aofRewriteBufferReset();
    // 移除临时文件
    aofRemoveTempFile(server.aof_child_pid);
    // 重置相关状态
    server.aof_child_pid = -1;
    server.aof_rewrite_time_last = time(NULL)-server.aof_rewrite_time_start;
    server.aof_rewrite_time_start = -1;
    /* Schedule a new rewrite if we are waiting for it to switch the AOF ON. */
    if (server.aof_state == REDIS_AOF_WAIT_REWRITE)
        server.aof_rewrite_scheduled = 1;
}
