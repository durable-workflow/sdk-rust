#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int intercept(const char *path, int flags, mode_t mode, int large) {
    int (*real_open)(const char *, int, ...) = dlsym(RTLD_NEXT, large ? "open64" : "open");
    const char *target = getenv("SQLITE_PROBE_TARGET");
    const char *armed = getenv("SQLITE_PROBE_ARMED");
    if (target && armed && strcmp(armed, "1") == 0 && strcmp(path, target) == 0) {
        char saved[4096], replacement[4096];
        if (snprintf(saved, sizeof(saved), "%s.saved", target) >= (int) sizeof(saved) ||
            snprintf(replacement, sizeof(replacement), "%s.replacement", target) >= (int) sizeof(replacement)) _exit(70);
        unsetenv("SQLITE_PROBE_ARMED");
        if (rename(target, saved) || rename(replacement, target)) _exit(71);
        int fd = real_open(path, flags, mode);
        if (rename(target, replacement) || rename(saved, target)) _exit(72);
        const char notice[] = "intercepted native pathname open\n";
        write(STDERR_FILENO, notice, sizeof(notice) - 1);
        return fd;
    }
    return real_open(path, flags, mode);
}

int open(const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list args; va_start(args, flags); mode = va_arg(args, int); va_end(args); }
    return intercept(path, flags, mode, 0);
}
int open64(const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list args; va_start(args, flags); mode = va_arg(args, int); va_end(args); }
    return intercept(path, flags, mode, 1);
}
