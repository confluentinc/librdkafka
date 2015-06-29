#ifdef MINGW_VER
#pragma comment(lib, "ws2_32.lib")
#endif

#define _DARWIN_C_SOURCE  /* MSG_DONTWAIT */

#include "rdkafka_int.h"
#include "rdaddr.h"
#include "rdkafka_transport.h"

#include <errno.h>


#ifdef MINGW_VER
#define socket_errno WSAGetLastError()
#else
#include <sys/socket.h>
#define socket_errno errno
#define SOCKET_ERROR -1
#endif


static const char *socket_strerror(int err) {
#ifdef MINGW_VER
	static RD_TLS char buf[256];
	FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
		err, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)buf, sizeof(buf)-1, NULL);
	return buf;
#else
	return rd_strerror(err);
#endif
}

ssize_t rd_kafka_transport_sendmsg(rd_kafka_transport_t *rktrans, const struct msghdr *msg,
	char *errstr, size_t errstr_size) {
#ifndef MINGW_VER
	ssize_t r;

#ifdef sun
	/* See recvmsg() comment. Setting it here to be safe. */
	socket_errno = EAGAIN;
#endif
	r = sendmsg(rktrans->rktrans_s, msg, MSG_DONTWAIT
#ifdef MSG_NOSIGNAL
		| MSG_NOSIGNAL
#endif
		);
	if (r == -1) {
		if (socket_errno == EAGAIN)
			return 0;
		rd_snprintf(errstr, errstr_size, "%s", rd_strerror(errno));
	}
	return r;
#else
	int i;
	ssize_t sum = 0;

	for (i = 0; i < msg->msg_iovlen; i++) {
		ssize_t r;

		r = send(rktrans->rktrans_s, msg->msg_iov[i].iov_base, msg->msg_iov[i].iov_len, 0);
		if (r == SOCKET_ERROR) {
			if (sum > 0 || WSAGetLastError() == WSAEWOULDBLOCK)
				return sum;
			else {
				rd_snprintf(errstr, errstr_size, "%s", socket_strerror(WSAGetLastError()));
				return -1;
			}
		}

		sum += r;
		if ((size_t)r < msg->msg_iov[i].iov_len)
			break;

	}
	return sum;
#endif
}


ssize_t rd_kafka_transport_recvmsg(rd_kafka_transport_t *rktrans, struct msghdr *msg,
									char *errstr, size_t errstr_size) {
#ifndef MINGW_VER
	ssize_t r;
#ifdef sun
	/* SunOS doesn't seem to set errno when recvmsg() fails
	* due to no data and MSG_DONTWAIT is set. */
	socket_errno = EAGAIN;
#endif
	r = recvmsg(rktrans->rktrans_s, msg, MSG_DONTWAIT);
	if (r == -1 && socket_errno == EAGAIN)
		return 0;
	else if (r == 0) {
		/* Receive 0 after POLLIN event means connection closed. */
		rd_snprintf(errstr, errstr_size, "Disconnected");
		return -1;
	} else if (r == -1)
		rd_snprintf(errstr, errstr_size, "%s", rd_strerror(errno));

	return r;
#else
	ssize_t sum = 0;
	int i;

	for (i = 0; i < msg->msg_iovlen; i++) {
		ssize_t r;

		r = recv(rktrans->rktrans_s,
			msg->msg_iov[i].iov_base, msg->msg_iov[i].iov_len, 0);
		if (r == SOCKET_ERROR) {
			if (WSAGetLastError() == WSAEWOULDBLOCK)
				break;
			rd_snprintf(errstr, errstr_size, "%s", socket_strerror(WSAGetLastError()));
			return -1;
		}
		sum += r;
		if ((size_t)r < msg->msg_iov[i].iov_len)
			break;
	}
	return sum;
#endif
}


rd_kafka_transport_t *rd_kafka_transport_connect(rd_kafka_broker_t *rkb, const rd_sockaddr_inx_t *sinx,
	char *errstr, int errstr_size) {
	rd_kafka_transport_t *rktrans;
	int s;
	int on = 1;

	
	s = rkb->rkb_rk->rk_conf.socket_cb(sinx->in.sin_family, SOCK_STREAM, IPPROTO_TCP, rkb->rkb_rk->rk_conf.opaque);
	if (s == -1) {
		rd_snprintf(errstr, errstr_size, "Failed to create socket: %s", socket_strerror(socket_errno));
		return NULL;
	}


#ifdef SO_NOSIGPIPE
	/* Disable SIGPIPE signalling for this socket on OSX */
	if (setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on)) == -1)
		rd_rkb_dbg(rkb, BROKER, "SOCKET", "Failed to set SO_NOSIGPIPE: %s",
		socket_strerror(socket_errno));
#endif

	/* Enable TCP keep-alives, if configured. */
	if (rkb->rkb_rk->rk_conf.socket_keepalive) {
#ifdef SO_KEEPALIVE
		if (setsockopt(s, SOL_SOCKET, SO_KEEPALIVE,
			(void *)&on, sizeof(on)) == SOCKET_ERROR)
			rd_rkb_dbg(rkb, BROKER, "SOCKET",
			"Failed to set SO_KEEPALIVE: %s",
			socket_strerror(socket_errno));
#else
		rd_rkb_dbg(rkb, BROKER, "SOCKET",
			"System does not support "
			"socket.keepalive.enable (SO_KEEPALIVE)");
#endif
	}

	rd_rkb_dbg(rkb, BROKER, "CONNECT", "Connecting to %s with socket %i",
		rd_sockaddr2str(sinx, RD_SOCKADDR2STR_F_FAMILY | RD_SOCKADDR2STR_F_PORT), s);
	/* Connect to broker */
	if (connect(s, (struct sockaddr *)sinx,
		RD_SOCKADDR_INX_LEN(sinx)) == SOCKET_ERROR) {
		rd_rkb_dbg(rkb, BROKER, "CONNECT",
			"couldn't connect to %s: %s (%i)",
			rd_sockaddr2str(sinx,
			RD_SOCKADDR2STR_F_PORT |
			RD_SOCKADDR2STR_F_FAMILY),
			socket_strerror(socket_errno), socket_errno);
		rd_snprintf(errstr, errstr_size, "Failed to connect to broker at %s: %s",
			rd_sockaddr2str(sinx, RD_SOCKADDR2STR_F_NICE), socket_strerror(socket_errno));
		goto err;
	}

	rd_rkb_dbg(rkb, BROKER, "CONNECTED", "connected to %s",
		rd_sockaddr2str(sinx, RD_SOCKADDR2STR_F_FAMILY|RD_SOCKADDR2STR_F_PORT));

	/* Set socket send & receive buffer sizes if configuerd */
	if (rkb->rkb_rk->rk_conf.socket_sndbuf_size != 0) {
		if (setsockopt(s, SOL_SOCKET, SO_SNDBUF,
			(void *)&rkb->rkb_rk->rk_conf.socket_sndbuf_size,
			sizeof(rkb->rkb_rk->rk_conf.
			socket_sndbuf_size)) == SOCKET_ERROR)
			rd_rkb_log(rkb, LOG_WARNING, "SNDBUF",
			"Failed to set socket send "
			"buffer size to %i: %s",
			rkb->rkb_rk->rk_conf.socket_sndbuf_size,
			socket_strerror(socket_errno));
	}

	if (rkb->rkb_rk->rk_conf.socket_rcvbuf_size != 0) {
		if (setsockopt(s, SOL_SOCKET, SO_RCVBUF,
			(void *)&rkb->rkb_rk->rk_conf.socket_rcvbuf_size,
			sizeof(rkb->rkb_rk->rk_conf.
			socket_rcvbuf_size)) == SOCKET_ERROR)
			rd_rkb_log(rkb, LOG_WARNING, "RCVBUF",
			"Failed to set socket receive "
			"buffer size to %i: %s",
			rkb->rkb_rk->rk_conf.socket_rcvbuf_size,
			socket_strerror(socket_errno));
	}

	/* After connection has been established set the socket to non-blocking */
#ifdef MINGW_VER
	if (ioctlsocket(s, FIONBIO, &on) == SOCKET_ERROR) {
		rd_snprintf(errstr, errstr_size,
			"Failed to set socket non-blocking: %s",
			socket_strerror(socket_errno));
		goto err;
	}
#else
	{
		int fl = fcntl(s, F_GETFL, 0);
		if (fl == -1 ||
			fcntl(s, F_SETFL, fl | O_NONBLOCK) == -1) {
			rd_snprintf(errstr, errstr_size,
				"Failed to set socket non-blocking: %s",
				socket_strerror(socket_errno));
			goto err;
		}
	}
#endif
	rktrans = rd_calloc(1, sizeof(*rktrans));
	rktrans->rktrans_s = s;
	rktrans->rktrans_pfd.fd = s;

	return rktrans;

err:
#ifndef MINGW_VER
	close(s);
#else
	closesocket(s);
#endif
	return NULL;
}


void rd_kafka_transport_close(rd_kafka_transport_t *rktrans) {
#ifndef MINGW_VER
	close(rktrans->rktrans_s);
#else
	closesocket(rktrans->rktrans_s);
#endif
	rd_free(rktrans);
}

void rd_kafka_transport_poll_set(rd_kafka_transport_t *rktrans, int event) {
	rktrans->rktrans_pfd.events |= event;
}

void rd_kafka_transport_poll_clear(rd_kafka_transport_t *rktrans, int event) {
	rktrans->rktrans_pfd.events &= ~event;
}


int rd_kafka_transport_poll(rd_kafka_transport_t *rktrans, int tmout) {
#ifndef MINGW_VER
	int r;
	
	r = poll(&rktrans->rktrans_pfd, 1, tmout);
	if (r <= 0)
		return r;
	return rktrans->rktrans_pfd.revents;
#else
	int r;

	r = WSAPoll(&rktrans->rktrans_pfd, 1, tmout);
	if (r == 0)
		return 0;
	else if (r == SOCKET_ERROR)
		return -1;
	return rktrans->rktrans_pfd.revents;
#endif
}


void rd_kafka_transport_init(void) {
#ifdef MINGW_VER
	WSADATA d;
	(void)WSAStartup(MAKEWORD(2, 2), &d);
#endif
}
