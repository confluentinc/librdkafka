/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2015 Magnus Edenhill
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

#include "rdkafka_int.h"
#include "rdkafka_transport.h"
#include "rdkafka_transport_int.h"
#include "rdkafka_sasl.h"


#include <sasl/sasl.h>

static mtx_t rd_kafka_sasl_kinit_lock;


/**
 * Send auth message with framing.
 * This is a blocking call.
 */
static int rd_kafka_sasl_send (rd_kafka_transport_t *rktrans,
			       const void *payload, int len,
			       char *errstr, int errstr_size) {
	struct msghdr msg = RD_ZERO_INIT;
	struct iovec iov[1];
	int32_t hdr;
	char *frame;
	int total_len = sizeof(hdr) + len;

	rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASL",
		   "Send SASL frame to broker (%d bytes)", len);

	frame = rd_malloc(total_len);

	hdr = htobe32(len);
	memcpy(frame, &hdr, sizeof(hdr));
	if (payload)
		memcpy(frame+sizeof(hdr), payload, len);
	
	msg.msg_iov = iov;
	msg.msg_iovlen = 1;
	iov[0].iov_base = frame;
	iov[0].iov_len  = total_len;

	/* Simulate blocking behaviour on non-blocking socket..
	 * FIXME: This isn't optimal but is highly unlikely to stall since
	 *        the socket buffer will most likely not be exceeded. */
	do {
		int r;

		r = rd_kafka_transport_sendmsg(rktrans, &msg,
					       errstr, errstr_size);
		if (r == -1) {
			rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASL",
				   "SASL send of %d bytes failed: %s",
				   total_len, errstr);
			free(frame);
			return -1;
		}

		if (r == total_len)
			break;

		/* Avoid busy-looping */
		rd_usleep(10*1000, NULL);

		/* Shave off written bytes */
		total_len -= r;
		iov[0].iov_base = ((char *)(iov[0].iov_base)) + r;
		iov[0].iov_len  = total_len;

	} while (total_len > 0);

	free(frame);

	return 0;
}



/**
 * Handle received frame from broker.
 */
static int rd_kafka_sasl_handle_recv (rd_kafka_transport_t *rktrans,
				      rd_kafka_buf_t *rkbuf,
				      char *errstr, int errstr_size) {
	int r;

	rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASL",
		   "Received SASL frame from broker (%"PRIdsz" bytes)",
		   rkbuf ? rkbuf->rkbuf_len : 0);

	if (rktrans->rktrans_sasl.complete && (!rkbuf || rkbuf->rkbuf_len == 0))
		goto auth_successful;

	do {
		sasl_interact_t *interact = NULL;
		const char *out;
		unsigned int outlen;

		r = sasl_client_step(rktrans->rktrans_sasl.conn,
				     rkbuf && rkbuf->rkbuf_len > 0 ?
				     rkbuf->rkbuf_rbuf : NULL,
				     rkbuf ? rkbuf->rkbuf_len : 0,
				     &interact,
				     &out, &outlen);

		if (rkbuf) {
			rd_kafka_buf_destroy(rkbuf);
			rkbuf = NULL;
		}

		if (r >= 0) {
			/* Note: outlen may be 0 here for an empty response */
			if (rd_kafka_sasl_send(rktrans, out, outlen,
					       errstr, errstr_size) == -1)
				return -1;
		}

		if (r == SASL_INTERACT)
			rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASL",
				   "SASL_INTERACT: %lu %s, %s, %s, %p",
				   interact->id,
				   interact->challenge,
				   interact->prompt,
				   interact->defresult,
				   interact->result);

	} while (r == SASL_INTERACT);

	if (r == SASL_CONTINUE)
		return 0;  /* Wait for more data from broker */
	else if (r != SASL_OK) {
		rd_snprintf(errstr, errstr_size,
			    "SASL handshake failed (step): %s",
			    sasl_errdetail(rktrans->rktrans_sasl.conn));
		return -1;
	}

	/* Authentication successful */
auth_successful:
	if (rktrans->rktrans_rkb->rkb_rk->rk_conf.debug &
	    RD_KAFKA_DBG_SECURITY) {
		const char *user, *mech, *authsrc;

		if (sasl_getprop(rktrans->rktrans_sasl.conn, SASL_USERNAME,
				 (const void **)&user) != SASL_OK)
			user = "(unknown)";
		
		if (sasl_getprop(rktrans->rktrans_sasl.conn, SASL_MECHNAME,
				 (const void **)&mech) != SASL_OK)
			mech = "(unknown)";

		if (sasl_getprop(rktrans->rktrans_sasl.conn, SASL_AUTHSOURCE,
				 (const void **)&authsrc) != SASL_OK)
			authsrc = "(unknown)";
			
		rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASL",
			   "Authenticated as %s using %s (%s)",
			   user, mech, authsrc);
	}

	rd_kafka_broker_connect_up(rktrans->rktrans_rkb);

	return 0;
}


int rd_kafka_sasl_io_event (rd_kafka_transport_t *rktrans, int events,
			    char *errstr, int errstr_size) {

	if (events & POLLIN) {
		rd_kafka_buf_t *rkbuf;
		int r;
		
		r = rd_kafka_transport_framed_recvmsg(rktrans, &rkbuf,
						      errstr, errstr_size);
		if (r == -1)
			return -1;
		else if (r == 0)
			return 0;

		return rd_kafka_sasl_handle_recv(rktrans, rkbuf,
						 errstr, errstr_size);
	}

	return 0;
}



static ssize_t render_callback (const char *key, char *buf,
				size_t size, void *opaque) {
	rd_kafka_broker_t *rkb = opaque;

	if (!strcmp(key, "broker.name")) {
		char *val, *t;
		size_t len;
		rd_kafka_broker_lock(rkb);
		rd_strdupa(&val, rkb->rkb_nodename);
		rd_kafka_broker_unlock(rkb);

		/* Just the broker name, no port */
		if ((t = strchr(val, ':')))
			len = (size_t)(t-val);
		else
			len = strlen(val);

		if (buf)
			memcpy(buf, val, RD_MIN(len, size));

		return len;

	} else {
		rd_kafka_conf_res_t res;
		size_t destsize = size;

		/* Try config lookup. */
		res = rd_kafka_conf_get(&rkb->rkb_rk->rk_conf, key,
					buf, &destsize);
		if (res != RD_KAFKA_CONF_OK)
			return -1;

		/* Dont include \0 in returned size */
		return (destsize > 0 ? destsize-1 : destsize);
	}
}


/**
 * Execute kinit to refresh ticket.
 *
 * Returns 0 on success, -1 on error.
 *
 * Locality: any
 */
static int rd_kafka_sasl_kinit_refresh (rd_kafka_broker_t *rkb) {
	rd_kafka_t *rk = rkb->rkb_rk;
	int r;
	char *cmd;
	char errstr[128];

	if (!rk->rk_conf.sasl.kinit_cmd ||
	    !strstr(rk->rk_conf.sasl.mechanisms, "GSSAPI"))
		return 0; /* kinit not configured */

	/* Build kinit refresh command line using string rendering and config */
	cmd = rd_string_render(rk->rk_conf.sasl.kinit_cmd,
			       errstr, sizeof(errstr),
			       render_callback, rkb);
	if (!cmd) {
		rd_rkb_log(rkb, LOG_ERR, "SASLREFRESH",
			   "Failed to construct kinit command "
			   "from sasl.kerberos.kinit.cmd template: %s",
			   errstr);
		return -1;
	}

	/* Execute kinit */
	rd_rkb_dbg(rkb, SECURITY, "SASLREFRESH",
		   "Refreshing SASL keys with command: %s", cmd);

	mtx_lock(&rd_kafka_sasl_kinit_lock);
	r = system(cmd);
	mtx_unlock(&rd_kafka_sasl_kinit_lock);

	if (r == -1) {
		rd_rkb_log(rkb, LOG_ERR, "SASLREFRESH",
			   "SASL key refresh failed: Failed to execute %s",
			   cmd);
		rd_free(cmd);
		return -1;
	} else if (WIFSIGNALED(r)) {
		rd_rkb_log(rkb, LOG_ERR, "SASLREFRESH",
			   "SASL key refresh failed: %s: received signal %d",
			   cmd, WTERMSIG(r));
		rd_free(cmd);
		return -1;
	} else if (WIFEXITED(r) && WEXITSTATUS(r) != 0) {
		rd_rkb_log(rkb, LOG_ERR, "SASLREFRESH",
			   "SASL key refresh failed: %s: exited with code %d",
			   cmd, WEXITSTATUS(r));
		rd_free(cmd);
		return -1;
	}

	rd_free(cmd);

	rd_rkb_dbg(rkb, SECURITY, "SASLREFRESH", "SASL key refreshed");
	return 0;
}


/**
 * Refresh timer callback
 *
 * Locality: kafka main thread
 */
static void rd_kafka_sasl_kinit_refresh_tmr_cb (rd_kafka_timers_t *rkts,
						void *arg) {
	rd_kafka_broker_t *rkb = arg;

	rd_kafka_sasl_kinit_refresh(rkb);
}



/**
 *
 * libsasl callbacks
 *
 */
static RD_UNUSED int
rd_kafka_sasl_cb_getopt (void *context, const char *plugin_name,
			 const char *option,
			 const char **result, unsigned *len) {
	rd_kafka_transport_t *rktrans = context;

	if (!strcmp(option, "client_mech_list"))
		*result = "GSSAPI";
	if (!strcmp(option, "canon_user_plugin"))
		*result = "INTERNAL";

	if (*result && len)
		*len = strlen(*result);

	rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "LIBSASL",
		   "CB_GETOPT: plugin %s, option %s: returning %s",
		   plugin_name, option, *result);
		
	return SASL_OK;
}

static int rd_kafka_sasl_cb_log (void *context, int level, const char *message){
	rd_kafka_transport_t *rktrans = context;

	if (level >= LOG_DEBUG)
		rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "LIBSASL",
			   "%s", message);
	else
		rd_rkb_log(rktrans->rktrans_rkb, level, "LIBSASL",
			   "%s", message);
	return SASL_OK;
}


static int rd_kafka_sasl_cb_getsimple (void *context, int id,
				       const char **result, unsigned *len) {
	rd_kafka_transport_t *rktrans = context;

	switch (id)
	{
	case SASL_CB_USER:
	case SASL_CB_AUTHNAME:
		*result = rktrans->rktrans_rkb->rkb_rk->rk_conf.sasl.username;
		break;

	default:
		*result = NULL;
		break;
	}

	if (len)
		*len = *result ? strlen(*result) : 0;

	rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "LIBSASL",
		   "CB_GETSIMPLE: id 0x%x: returning %s", id, *result);
		
	return *result ? SASL_OK : SASL_FAIL;
}


static int rd_kafka_sasl_cb_getsecret (sasl_conn_t *conn, void *context,
				       int id, sasl_secret_t **psecret) {
	rd_kafka_transport_t *rktrans = context;
	const char *password;

	password = rktrans->rktrans_rkb->rkb_rk->rk_conf.sasl.password;

	if (!password) {
		*psecret = NULL;
	} else {
		size_t passlen = strlen(password);
		*psecret = rd_realloc(*psecret, sizeof(**psecret) + passlen);
		(*psecret)->len = passlen;
		memcpy((*psecret)->data, password, passlen);
	}

	rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "LIBSASL",
		   "CB_GETSECRET: id 0x%x: returning %s",
		   id, *psecret ? "(hidden)":"NULL");
	
	return SASL_OK;
}

static int rd_kafka_sasl_cb_chalprompt (void *context, int id,
					const char *challenge,
					const char *prompt,
					const char *defres,
					const char **result, unsigned *len) {
	rd_kafka_transport_t *rktrans = context;

	*result = "min_chalprompt";
	*len = strlen(*result);

	rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "LIBSASL",
		   "CB_CHALPROMPT: id 0x%x, challenge %s, prompt %s, "
		   "default %s: returning %s",
		   id, challenge, prompt, defres, *result);

	return SASL_OK;
}

static int rd_kafka_sasl_cb_getrealm (void *context, int id,
				      const char **availrealms,
				      const char **result) {
	rd_kafka_transport_t *rktrans = context;

	*result = *availrealms;

	rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "LIBSASL",
		   "CB_GETREALM: id 0x%x: returning %s", id, *result);

	return SASL_OK;
}


static RD_UNUSED int rd_kafka_sasl_cb_canon (sasl_conn_t *conn,
					     void *context,
					     const char *in, unsigned inlen,
					     unsigned flags,
					     const char *user_realm,
					     char *out, unsigned out_max,
					     unsigned *out_len) {
	rd_kafka_transport_t *rktrans = context;

	if (strstr(rktrans->rktrans_rkb->rkb_rk->rk_conf.
		   sasl.mechanisms, "GSSAPI")) {
		*out_len = rd_snprintf(out, out_max, "%s",
				       rktrans->rktrans_rkb->rkb_rk->
				       rk_conf.sasl.principal);
	} else if (!strcmp(rktrans->rktrans_rkb->rkb_rk->rk_conf.
			   sasl.mechanisms, "PLAIN")) {
		*out_len = rd_snprintf(out, out_max, "%.*s", inlen, in);
	} else
		out = NULL;

	rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "LIBSASL",
		   "CB_CANON: flags 0x%x, \"%.*s\" @ \"%s\": returning \"%.*s\"",
		   flags, (int)inlen, in, user_realm, (int)(*out_len), out);

	return out ? SASL_OK : SASL_FAIL;
}


/**
 * Initialize and start SASL authentication.
 *
 * Returns 0 on successful init and -1 on error.
 *
 * Locality: broker thread
 */
int rd_kafka_sasl_client_new (rd_kafka_transport_t *rktrans,
			      char *errstr, int errstr_size) {
	int r;
	rd_kafka_broker_t *rkb = rktrans->rktrans_rkb;
	rd_kafka_t *rk = rkb->rkb_rk;
	char *hostname, *t;
	sasl_callback_t callbacks[16] = {
		// { SASL_CB_GETOPT, (void *)rd_kafka_sasl_cb_getopt, rktrans },
		{ SASL_CB_LOG, (void *)rd_kafka_sasl_cb_log, rktrans },
		{ SASL_CB_AUTHNAME, (void *)rd_kafka_sasl_cb_getsimple, rktrans },
		{ SASL_CB_PASS, (void *)rd_kafka_sasl_cb_getsecret, rktrans },
		{ SASL_CB_ECHOPROMPT, (void *)rd_kafka_sasl_cb_chalprompt, rktrans },
		{ SASL_CB_GETREALM, (void *)rd_kafka_sasl_cb_getrealm, rktrans },
		{ SASL_CB_CANON_USER, (void *)rd_kafka_sasl_cb_canon, rktrans },
		{ SASL_CB_LIST_END }
	};

	/* SASL_CB_USER is needed for PLAIN but breaks GSSAPI */
	if (!strcmp(rk->rk_conf.sasl.service_name, "PLAIN")) {
		int endidx;
		/* Find end of callbacks array */
		for (endidx = 0 ;
		     callbacks[endidx].id != SASL_CB_LIST_END ; endidx++)
			;

		callbacks[endidx].id = SASL_CB_USER;
		callbacks[endidx].proc = (void *)rd_kafka_sasl_cb_getsimple;
		endidx++;
		callbacks[endidx].id = SASL_CB_LIST_END;
	}

	rd_strdupa(&hostname, rktrans->rktrans_rkb->rkb_nodename);
	if ((t = strchr(hostname, ':')))
		*t = '\0';  /* remove ":port" */

	rd_rkb_dbg(rkb, SECURITY, "SASL",
		   "Initializing SASL client: service name %s, "
		   "hostname %s, mechanisms %s",
		   rk->rk_conf.sasl.service_name, hostname,
		   rk->rk_conf.sasl.mechanisms);

	/* Acquire or refresh ticket if kinit is configured */ 
	rd_kafka_sasl_kinit_refresh(rkb);

	r = sasl_client_new(rk->rk_conf.sasl.service_name, hostname,
			    NULL, NULL, /* no local & remote IP checks */
			    callbacks, 0, &rktrans->rktrans_sasl.conn);
	if (r != SASL_OK) {
		rd_snprintf(errstr, errstr_size, "%s",
			    sasl_errstring(r, NULL, NULL));
		return -1;
	}

	if (rk->rk_conf.debug & RD_KAFKA_DBG_SECURITY) {
		const char *avail_mechs;
		sasl_listmech(rktrans->rktrans_sasl.conn, NULL, NULL, " ", NULL,
			      &avail_mechs, NULL, NULL);
		rd_rkb_dbg(rkb, SECURITY, "SASL",
			   "My supported SASL mechanisms: %s", avail_mechs);
	}


	rd_kafka_transport_poll_set(rktrans, POLLIN);
	
	do {
		const char *out;
		unsigned int outlen;
		const char *mech = NULL;

		r = sasl_client_start(rktrans->rktrans_sasl.conn,
				      rk->rk_conf.sasl.mechanisms,
				      NULL, &out, &outlen, &mech);

		if (r >= 0)
			if (rd_kafka_sasl_send(rktrans, out, outlen,
					       errstr, errstr_size))
				return -1;
	} while (r == SASL_INTERACT);

	if (r == SASL_OK) {
		/* PLAIN is appearantly done here, but we still need to make sure
		 * the PLAIN frame is sent and we get a response back (but we must
		 * not pass the response to libsasl or it will fail). */
		rktrans->rktrans_sasl.complete = 1;
		return 0;

	} else if (r != SASL_CONTINUE) {
		rd_snprintf(errstr, errstr_size,
			    "SASL handshake failed (start (%d)): %s",
			    r, sasl_errdetail(rktrans->rktrans_sasl.conn));
		return -1;
	}

	return 0;
}







/**
 * Per handle SASL term.
 *
 * Locality: broker thread
 */
void rd_kafka_broker_sasl_term (rd_kafka_broker_t *rkb) {
	rd_kafka_t *rk = rkb->rkb_rk;

	if (!rk->rk_conf.sasl.kinit_cmd)
		return;

	rd_kafka_timer_stop(&rk->rk_timers, &rkb->rkb_sasl_kinit_refresh_tmr,1);
}

/**
 * Broker SASL init.
 *
 * Locality: broker thread
 */
void rd_kafka_broker_sasl_init (rd_kafka_broker_t *rkb) {
	rd_kafka_t *rk = rkb->rkb_rk;

	if (!rk->rk_conf.sasl.kinit_cmd ||
	    !strstr(rk->rk_conf.sasl.mechanisms, "GSSAPI"))
		return; /* kinit not configured, no need to start timer */

	rd_kafka_timer_start(&rk->rk_timers, &rkb->rkb_sasl_kinit_refresh_tmr,
			     rk->rk_conf.sasl.relogin_min_time * 1000,
			     rd_kafka_sasl_kinit_refresh_tmr_cb, rkb);
}



int rd_kafka_sasl_conf_validate (rd_kafka_t *rk,
				 char *errstr, size_t errstr_size) {
	rd_kafka_broker_t rkb;
	char *cmd;
	char tmperr[128];

	if (strcmp(rk->rk_conf.sasl.mechanisms, "GSSAPI"))
		return 0;

	memset(&rkb, 0, sizeof(rkb));
	strcpy(rkb.rkb_nodename, "ATestBroker:9092");
	rkb.rkb_rk = rk;
	mtx_init(&rkb.rkb_lock, mtx_plain);

	cmd = rd_string_render(rk->rk_conf.sasl.kinit_cmd,
			       tmperr, sizeof(tmperr),
			       render_callback, &rkb);

	mtx_destroy(&rkb.rkb_lock);

	if (!cmd) {
		rd_snprintf(errstr, errstr_size,
			    "Invalid sasl.kerberos.kinit.cmd value: %s",
			    tmperr);
		return -1;
	}

	rd_free(cmd);
	return 0;
}


/**
 * Global SASL termination.
 * NOTE: Should not be called since the application may be using SASL too.
 */
void rd_kafka_sasl_global_term (void) {
	sasl_done();
	mtx_destroy(&rd_kafka_sasl_kinit_lock);
}


/**
 * Global SASL init, called once per runtime.
 */
int rd_kafka_sasl_global_init (void) {
	int r;

	mtx_init(&rd_kafka_sasl_kinit_lock, mtx_plain);

	r = sasl_client_init(NULL);
	if (r != SASL_OK) {
		fprintf(stderr, "librdkafka: sasl_client_init() failed: %s\n",
			sasl_errstring(r, NULL, NULL));
		return -1;
	}

	return 0;
}
      
