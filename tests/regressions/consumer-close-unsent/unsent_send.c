/* Linux-only fault injector for the native consumer-close regression probe. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>

/* One owned probe process controls its send fault; Kafka threads observe these
 * atomics. */
static atomic_int hold;
static atomic_uint_fast64_t hits;
static pthread_once_t resolve_once = PTHREAD_ONCE_INIT;
static ssize_t (*original_sendmsg)(int, const struct msghdr *, int);

/* Resolve libc exactly once, before concurrent broker threads forward sends. */
static void resolve_sendmsg(void) {
        original_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
        if (!original_sendmsg) {
                fputs("Cannot resolve the original sendmsg\n", stderr);
                _exit(2);
        }
}

/* These exports let only the owning probe arm, release and verify its fault. */
void hold_offset_commits(int enabled) {
        atomic_store(&hold, enabled);
}
int offset_commits_held(void) {
        return atomic_load(&hold);
}
uint64_t held_offset_commit_sends(void) {
        return atomic_load(&hits);
}

/* Keep OffsetCommit unsent, rather than returning a broker error after
 * transmission. Only the protocol header is inspected; no document or
 * credential data is retained. */
ssize_t sendmsg(int fd, const struct msghdr *message, int flags) {
        pthread_once(&resolve_once, resolve_sendmsg);
        unsigned char header[6];
        size_t count = 0;
        for (size_t i = 0; i < message->msg_iovlen && count < sizeof(header);
             ++i) {
                const unsigned char *bytes = message->msg_iov[i].iov_base;
                for (size_t j = 0;
                     j < message->msg_iov[i].iov_len && count < sizeof(header);
                     ++j)
                        header[count++] = bytes[j];
        }
        /* Kafka request size precedes its two-byte API key; OffsetCommit is
         * API 8.
         */
        if (atomic_load(&hold) && count == sizeof(header) && header[4] == 0 &&
            header[5] == 8) {
                atomic_fetch_add(&hits, 1);
                errno = EAGAIN;
                return -1;
        }
        return original_sendmsg(fd, message, flags);
}
