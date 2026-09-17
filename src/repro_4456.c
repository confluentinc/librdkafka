/* librdkafka #4456: descriptors leak when broker thread creation fails.
 *
 * rd_kafka_broker_add() opens a wake-up pipe before it starts the broker
 * thread. If thrd_create() fails the function frees the broker struct with a
 * plain rd_free() and returns, which does not run the destructor that owns the
 * close, so both pipe descriptors stay open for the life of the process and the
 * only record of their numbers is freed with the struct.
 *
 * Reproducing it needs thread creation to fail, so this lowers RLIMIT_NPROC to
 * just above what the process is already using and then asks for brokers. The
 * descriptor count is read from /proc/self/fd, which counts what the kernel
 * actually holds rather than what the library believes it holds.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/resource.h>
#include "rdkafka.h"

static int open_fds(void) {
    DIR *d = opendir("/proc/self/fd");
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] != '.') n++;
    }
    closedir(d);
    return n - 1;                 /* the handle opendir itself is holding */
}

/* Every pipe still open, so a leak can be named rather than just counted. */
static void list_pipes(const char *when) {
    DIR *d = opendir("/proc/self/fd");
    if (!d) return;
    struct dirent *e;
    int pipes = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        char p[256], t[256];
        snprintf(p, sizeof p, "/proc/self/fd/%s", e->d_name);
        ssize_t r = readlink(p, t, sizeof t - 1);
        if (r <= 0) continue;
        t[r] = 0;
        if (strncmp(t, "pipe:", 5) == 0) pipes++;
    }
    closedir(d);
    printf("  %-18s pipes held: %d\n", when, pipes);
}

int main(int argc, char **argv) {
    int rounds = argc > 1 ? atoi(argv[1]) : 3;

    printf("librdkafka %s\n", rd_kafka_version_str());
    int before = open_fds();
    printf("  descriptors before: %d\n", before);
    list_pipes("before");

    /* Hold thread creation just out of reach. The library's own failure path is
       what is under test, so the failure has to be real rather than simulated. */
    struct rlimit rl;
    getrlimit(RLIMIT_NPROC, &rl);
    rl.rlim_cur = 1;
    if (setrlimit(RLIMIT_NPROC, &rl) != 0) {
        perror("  setrlimit");
        return 77;
    }

    for (int i = 0; i < rounds; i++) {
        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "bootstrap.servers", "127.0.0.1:9092", errstr, sizeof errstr);
        rd_kafka_conf_set(conf, "log_level", "0", errstr, sizeof errstr);
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof errstr);
        if (rk) {
            /* thread creation succeeded, so this round proves nothing */
            rd_kafka_destroy(rk);
        }
    }

    int after = open_fds();
    printf("  descriptors after %d rounds: %d   (leaked %d)\n", rounds, after, after - before);
    list_pipes("after");
    return (after - before) > 0 ? 1 : 0;
}
