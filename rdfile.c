/*
 * librd - Rapid Development C library
 *
 * Copyright (c) 2012, Magnus Edenhill
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

#include "rd.h"
#include "rdfile.h"


const char *rd_basename (const char *path) {
	const char *t = path;
	const char *t2 = t;

	while ((t = strchr(t, '/')))
		t2 = ++t;

	return t2;
}


const char *rd_pwd (void) {
	static __thread char path[PATH_MAX];

	if (!getcwd(path, sizeof(path)-1))
		return NULL;

	return path;
}


ssize_t rd_file_size (const char *path) {
	struct stat st;
	
	if (stat(path, &st) == -1)
		return (ssize_t)-1;

	return st.st_size;
}

ssize_t rd_file_size_fd (int fd) {
	struct stat st;

	if (fstat(fd, &st) == -1)
		return (ssize_t)-1;

	return st.st_size;
}


mode_t rd_file_mode (const char *path) {
	struct stat st;

	if (stat(path, &st) == -1)
		return 0;

	return st.st_mode;
}


char *rd_file_read (const char *path, int *lenp) {
	char *buf;
	int fd;
	ssize_t size;
	int r;

	if ((fd = open(path, O_RDONLY)) == -1)
		return NULL;

	if ((size = rd_file_size_fd(fd)) == -1) {
		close(fd);
		return NULL;
	}

	if (!(buf = malloc(size+1))) {
		close(fd);
		return NULL;
	}

	if ((r = read(fd, buf, size)) == -1) {
		close(fd);
		free(buf);
		return NULL;
	}

	buf[r] = '\0';
	
	if (lenp)
		*lenp = r;

	return buf;
}



int rd_file_write (const char *path, const char *buf, int len,
		   int flags, mode_t mode) {
	int fd;
	int r;
	int of = 0;

	if ((fd = open(path, O_CREAT|O_WRONLY|flags, mode)) == -1)
		return -1;

	while (of < len) {
		if ((r = write(fd, buf+of, RD_MIN(16384, len - of))) == -1) {
			close(fd);
			return -1;
		}

		of += r;
	}

	close(fd);

	return 0;
}
