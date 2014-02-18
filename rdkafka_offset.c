/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012,2013 Magnus Edenhill
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer. 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution. 
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

/**
 * This file implements the consumer offset storage.
 * It currently only supports local file storage, not zookeeper.
 *
 * Regardless of commit method (file, zookeeper, ..) this is how it works:
 *  - When rdkafka, or the application, depending on if auto.offset.commit
 *    is enabled or not, calls rd_kafka_offset_store() with an offset to store,
 *    all it does is set rktp->rktp_stored_offset to this value.
 *    This can happen from any thread and is locked by the rktp lock.
 *  - The actual commit/write of the offset to its backing store (filesystem)
 *    is performed by the main rdkafka thread and scheduled at the configured
 *    auto.commit.interval.ms interval.
 *  - The write is performed in the main rdkafka thread in a blocking manner
 *    and once the write has succeeded rktp->rktp_commited_offset is updated
 *    to the new value.
 *  - If offset.store.sync.interval.ms is configured the main rdkafka thread
 *    will also make sure to fsync() each offset file accordingly.
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "rdkafka.h"
#include "rdkafka_int.h"
#include "rdkafka_offset.h"
#include "rdkafka_topic.h"

/**
 * NOTE: toppar_lock(rktp) must be held
 */
static void rd_kafka_offset_file_close (rd_kafka_toppar_t *rktp) {
	if (rktp->rktp_offset_fd == -1)
		return;

	close(rktp->rktp_offset_fd);
	rktp->rktp_offset_fd = -1;
}


/**
 * NOTE: toppar_lock(rktp) must be held
 */
static int rd_kafka_offset_file_open (rd_kafka_toppar_t *rktp) {
	int fd;

	if ((fd = open(rktp->rktp_offset_path, O_CREAT|O_RDWR, 0644)) == -1) {
		rd_kafka_op_err(rktp->rktp_rkt->rkt_rk,
				RD_KAFKA_RESP_ERR__FS,
				"%s [%"PRId32"]: "
				"Failed to open offset file %s: %s",
				rktp->rktp_rkt->rkt_topic->str,
				rktp->rktp_partition,
				rktp->rktp_offset_path, strerror(errno));
		return -1;
	}

	rktp->rktp_offset_fd = fd;

	return 0;
}

/**
 * NOTE: toppar_lock(rktp) must be held
 */
static int64_t rd_kafka_offset_file_read (rd_kafka_toppar_t *rktp) {
	char buf[22];
	char *end;
	int64_t offset;
	int r;

	if (lseek(rktp->rktp_offset_fd, SEEK_SET, 0) == -1) {
		rd_kafka_op_err(rktp->rktp_rkt->rkt_rk,
				RD_KAFKA_RESP_ERR__FS,
				"%s [%"PRId32"]: "
				"Seek (for read) failed on offset file %s: %s",
				rktp->rktp_rkt->rkt_topic->str,
				rktp->rktp_partition,
				rktp->rktp_offset_path,
				strerror(errno));
		rd_kafka_offset_file_close(rktp);
		return -1;
	}

	if ((r = read(rktp->rktp_offset_fd, buf, sizeof(buf)-1)) == -1) {
		rd_kafka_op_err(rktp->rktp_rkt->rkt_rk,
				RD_KAFKA_RESP_ERR__FS,
				"%s [%"PRId32"]: "
				"Failed to read offset file %s: %s",
				rktp->rktp_rkt->rkt_topic->str,
				rktp->rktp_partition,
				rktp->rktp_offset_path, strerror(errno));
		rd_kafka_offset_file_close(rktp);
		return -1;
	}

	if (r == 0) {
		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OFFSET",
			     "%s [%"PRId32"]: offset file (%s) is empty",
			     rktp->rktp_rkt->rkt_topic->str,
			     rktp->rktp_partition,
			     rktp->rktp_offset_path);
		return -1;
	}

	buf[r] = '\0';

	offset = strtoull(buf, &end, 10);
	if (buf == end) {
		rd_kafka_op_err(rktp->rktp_rkt->rkt_rk,
				RD_KAFKA_RESP_ERR__FS,
				"%s [%"PRId32"]: "
				"Unable to parse offset in %s",
				rktp->rktp_rkt->rkt_topic->str,
				rktp->rktp_partition,
				rktp->rktp_offset_path);
		return -1;
	}


	rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OFFSET",
		     "%s [%"PRId32"]: Read offset %"PRId64" from offset "
		     "file (%s)",
		     rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
		     offset, rktp->rktp_offset_path);

	return offset;
}


/**
 * NOTE: rktp lock is not required.
 * Locality: rdkafka main thread
 */
static int rd_kafka_offset_file_commit (rd_kafka_toppar_t *rktp,
					int64_t offset) {
	rd_kafka_topic_t *rkt = rktp->rktp_rkt;
	int attempt;

	for (attempt = 0 ; attempt < 2 ; attempt++) {
		char buf[22];
		int len;

		if (rktp->rktp_offset_fd == -1)
			if (rd_kafka_offset_file_open(rktp) == -1)
				continue;

		if (lseek(rktp->rktp_offset_fd, 0, SEEK_SET) == -1) {
			rd_kafka_op_err(rktp->rktp_rkt->rkt_rk,
					RD_KAFKA_RESP_ERR__FS,
					"%s [%"PRId32"]: "
					"Seek failed on offset file %s: %s",
					rktp->rktp_rkt->rkt_topic->str,
					rktp->rktp_partition,
					rktp->rktp_offset_path,
					strerror(errno));
			rd_kafka_offset_file_close(rktp);
			continue;
		}

		len = snprintf(buf, sizeof(buf), "%"PRId64"\n", offset);

		if (write(rktp->rktp_offset_fd, buf, len) == -1) {
			rd_kafka_op_err(rktp->rktp_rkt->rkt_rk,
					RD_KAFKA_RESP_ERR__FS,
					"%s [%"PRId32"]: "
					"Failed to write offset %"PRId64" to "
					"offset file %s (fd %i): %s",
					rktp->rktp_rkt->rkt_topic->str,
					rktp->rktp_partition,
					offset,
					rktp->rktp_offset_path,
					rktp->rktp_offset_fd,
					strerror(errno));
			rd_kafka_offset_file_close(rktp);
			continue;
		}

		if (ftruncate(rktp->rktp_offset_fd, len) == -1)
			; /* Ignore truncate failures */

		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OFFSET",
			     "%s [%"PRId32"]: wrote offset %"PRId64" to "
			     "file %s",
			     rktp->rktp_rkt->rkt_topic->str,
			     rktp->rktp_partition, offset,
			     rktp->rktp_offset_path);

		rktp->rktp_commited_offset = offset;

		/* If sync interval is set to immediate we sync right away. */
		if (rkt->rkt_conf.offset_store_sync_interval_ms == 0)
			fsync(rktp->rktp_offset_fd);

		return 0;
	}


	return -1;
}



/**
 * Store offset.
 * Typically called from application code.
 *
 * NOTE: No lucks must be held.
 */
rd_kafka_resp_err_t rd_kafka_offset_store (rd_kafka_topic_t *rkt,
					   int32_t partition, int64_t offset) {
	rd_kafka_toppar_t *rktp;

	/* Find toppar */
	rd_kafka_topic_rdlock(rkt);
	if (!(rktp = rd_kafka_toppar_get(rkt, partition, 0/*!ua_on_miss*/))) {
		rd_kafka_topic_unlock(rkt);
		return RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
	}
	rd_kafka_topic_unlock(rkt);

	rd_kafka_offset_store0(rktp, offset, 1/*lock*/);

	rd_kafka_toppar_destroy(rktp);

	return RD_KAFKA_RESP_ERR_NO_ERROR;
}



/**
 * Offset file commit timer callback.
 */
static void rd_kafka_offset_file_commit_tmr_cb (rd_kafka_t *rk, void *arg) {
	rd_kafka_toppar_t *rktp = arg;

	rd_kafka_toppar_lock(rktp);

	rd_kafka_dbg(rk, TOPIC, "OFFSET",
		     "%s [%"PRId32"]: periodic commit: "
		     "stored offset %"PRId64" > commited offset %"PRId64" ?",
		     rktp->rktp_rkt->rkt_topic->str,
		     rktp->rktp_partition,
		     rktp->rktp_stored_offset, rktp->rktp_commited_offset);

	if (rktp->rktp_stored_offset > rktp->rktp_commited_offset)
		rd_kafka_offset_file_commit(rktp, rktp->rktp_stored_offset);

	rd_kafka_toppar_unlock(rktp);
}


/**
 * Offset file sync timer callback
 */
static void rd_kafka_offset_file_sync_tmr_cb (rd_kafka_t *rk, void *arg) {
	rd_kafka_toppar_t *rktp = arg;

	rd_kafka_toppar_lock(rktp);
	if (rktp->rktp_offset_fd != -1) {
		rd_kafka_dbg(rk, TOPIC, "SYNC",
			     "%s [%"PRId32"]: offset file sync",
			     rktp->rktp_rkt->rkt_topic->str,
			     rktp->rktp_partition);
		fsync(rktp->rktp_offset_fd);
	}
	rd_kafka_toppar_unlock(rktp);
}


/**
 * Decommissions the use of an offset file for a toppar.
 * The file content will not be touched and the file will not be removed.
 *
 * NOTE: toppar_lock(rktp) must be held.
 */
static void rd_kafka_offset_file_term (rd_kafka_toppar_t *rktp) {
	if (rktp->rktp_rkt->rkt_conf.offset_store_sync_interval_ms > 0)
		rd_kafka_timer_stop(rktp->rktp_rkt->rkt_rk,
				    &rktp->rktp_offset_sync_tmr, 1/*lock*/);

	rd_kafka_timer_stop(rktp->rktp_rkt->rkt_rk,
			    &rktp->rktp_offset_commit_tmr, 1/*lock*/);

	if (rktp->rktp_stored_offset > rktp->rktp_commited_offset)
		rd_kafka_offset_file_commit(rktp, rktp->rktp_stored_offset);

	rd_kafka_offset_file_close(rktp);

	free(rktp->rktp_offset_path);
	rktp->rktp_offset_path = NULL;
}


/**
 * Take action when the offset for a toppar becomes unusable.
 * NOTE: toppar_lock(rktp) must be held
 */
void rd_kafka_offset_reset (rd_kafka_toppar_t *rktp, int64_t err_offset,
			    rd_kafka_resp_err_t err, const char *reason) {
	int64_t offset = RD_KAFKA_OFFSET_ERROR;
	rd_kafka_op_t *rko;

	switch (rktp->rktp_rkt->rkt_conf.auto_offset_reset)
	{
	case RD_KAFKA_OFFSET_END:
	case RD_KAFKA_OFFSET_BEGINNING:
		offset = rktp->rktp_rkt->rkt_conf.auto_offset_reset;
		rktp->rktp_query_offset = offset;
		rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY;
		break;
	case RD_KAFKA_OFFSET_ERROR:
		rko = rd_kafka_op_new(RD_KAFKA_OP_ERR);

		rko->rko_err                 = err;
		rko->rko_rkmessage.offset    = err_offset;
		rko->rko_rkmessage.rkt       = rktp->rktp_rkt;
		rko->rko_rkmessage.partition = rktp->rktp_partition;
		rko->rko_payload             = strdup(reason);
		rko->rko_len                 = strlen(rko->rko_payload);
		rko->rko_flags              |= RD_KAFKA_OP_F_FREE;

		rd_kafka_q_enq(&rktp->rktp_fetchq, rko);
		rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_NONE;
		break;
	}

	rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OFFSET",
		     "%s [%"PRId32"]: offset reset (at offset %"PRId64") "
		     "to %"PRId64": %s: %s",
		     rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
		     err_offset, offset, reason, rd_kafka_err2str(err));
}


/**
 * Prepare a toppar for using an offset file.
 *
 * NOTE: toppar_lock(rktp) must be held.
 */
static void rd_kafka_offset_file_init (rd_kafka_toppar_t *rktp) {
	struct stat st;
	char spath[4096];
	const char *path = rktp->rktp_rkt->rkt_conf.offset_store_path;
	int64_t offset = -1;

	if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
		snprintf(spath, sizeof(spath),
			 "%s%s%s-%"PRId32".offset",
			 path, path[strlen(path)-1] == '/' ? "" : "/",
			 rktp->rktp_rkt->rkt_topic->str,
			 rktp->rktp_partition);
		path = spath;
	}

	rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OFFSET",
		     "%s [%"PRId32"] using offset file %s",
		     rktp->rktp_rkt->rkt_topic->str,
		     rktp->rktp_partition,
		     path);
	rktp->rktp_offset_path = strdup(path);

	rd_kafka_timer_start(rktp->rktp_rkt->rkt_rk,
			     &rktp->rktp_offset_commit_tmr,
			     rktp->rktp_rkt->rkt_conf.auto_commit_interval_ms *
			     1000,
			     rd_kafka_offset_file_commit_tmr_cb, rktp);

	if (rktp->rktp_rkt->rkt_conf.offset_store_sync_interval_ms > 0)
		rd_kafka_timer_start(rktp->rktp_rkt->rkt_rk,
				     &rktp->rktp_offset_sync_tmr,
				     rktp->rktp_rkt->rkt_conf.
				     offset_store_sync_interval_ms * 1000,
				     rd_kafka_offset_file_sync_tmr_cb, rktp);

	if (rd_kafka_offset_file_open(rktp) != -1) {
		/* Read offset from offset file. */
		offset = rd_kafka_offset_file_read(rktp);
	}

	if (offset != -1) {
		/* Start fetching from offset */
		rktp->rktp_commited_offset = offset;
		rktp->rktp_next_offset     = offset;
		rktp->rktp_fetch_state     = RD_KAFKA_TOPPAR_FETCH_ACTIVE;

	} else {
		/* Offset was not usable: perform offset reset logic */
		rktp->rktp_commited_offset = 0;
		rd_kafka_offset_reset(rktp, RD_KAFKA_OFFSET_ERROR,
				      RD_KAFKA_RESP_ERR__FS,
				      "non-readable offset file");
	}
}


/**
 * Terminates toppar's offset store.
 * NOTE: toppar_lock(rktp) must be held.
 */
void rd_kafka_offset_store_term (rd_kafka_toppar_t *rktp) {
	rd_kafka_offset_file_term(rktp);
}


/**
 * Initialize toppar's offset store.
 * NOTE: toppar_lock(rktp) must be held.
 */
void rd_kafka_offset_store_init (rd_kafka_toppar_t *rktp) {
	rd_kafka_offset_file_init(rktp);
}
