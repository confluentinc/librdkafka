#pragma once

#if !defined(_MSC_VER) && !defined(MINGW_VER)
#include <poll.h>
#endif

#include "rdaddr.h"

typedef struct rd_kafka_transport_s {
	int rktrans_s;

#if !defined(_MSC_VER) && !defined(MINGW_VER)
	struct pollfd rktrans_pfd;
#else
	WSAPOLLFD rktrans_pfd;
// FIXME: Are these equal?
//#define POLLIN  POLLRDNORM
//#define POLLOUT POLLWRNORM

#endif
} rd_kafka_transport_t;


ssize_t rd_kafka_transport_sendmsg(rd_kafka_transport_t *rktrans, const struct msghdr *msg,
	char *errstr, size_t errstr_size);
ssize_t rd_kafka_transport_recvmsg(rd_kafka_transport_t *rktrans, struct msghdr *msg,
	char *errstr, size_t errstr_size);
struct rd_kafka_broker_s;
rd_kafka_transport_t *rd_kafka_transport_connect(struct rd_kafka_broker_s *rkb, const rd_sockaddr_inx_t *sinx,
	char *errstr, int errstr_size);
void rd_kafka_transport_close(rd_kafka_transport_t *rktrans);
void rd_kafka_transport_poll_set(rd_kafka_transport_t *rktrans, int event);
void rd_kafka_transport_poll_clear(rd_kafka_transport_t *rktrans, int event);
int rd_kafka_transport_poll(rd_kafka_transport_t *rktrans, int tmout);
void rd_kafka_transport_init(void);
