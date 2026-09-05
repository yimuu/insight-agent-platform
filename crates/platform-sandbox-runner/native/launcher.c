#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

extern char **environ;

#define RUNNER_UID ((uid_t)65532)
#define RUNNER_GID ((gid_t)65532)
#define EXPECTED_CAPS ((uint64_t)0xe0)
#define MAX_CAPABILITY 63
#define MAX_RUNNER_CONFIG_BYTES 65536
#define MAX_SANDBOX_ID_BYTES 128
#define CORE_PATH "/usr/local/libexec/platform-sandbox-runner-core"
#define LINUX_CAPABILITY_VERSION_3 UINT32_C(0x20080522)

#ifndef SYS_close_range
#define SYS_close_range 436
#endif

struct capability_header {
    uint32_t version;
    int32_t pid;
};

struct capability_data {
    uint32_t effective;
    uint32_t permitted;
    uint32_t inheritable;
};

static __attribute__((noreturn)) void fail(const char *reason)
{
    dprintf(STDERR_FILENO, "platform-sandbox-launcher: %s\n", reason);
    _exit(126);
}

static void require(bool condition, const char *reason)
{
    if (!condition)
        fail(reason);
}

static const char *unique_env(const char *name)
{
    const size_t name_len = strlen(name);
    const char *value = NULL;

    for (char **entry = environ; entry != NULL && *entry != NULL; entry++) {
        if (strncmp(*entry, name, name_len) != 0 || (*entry)[name_len] != '=')
            continue;
        if (value != NULL)
            fail("duplicate required environment variable");
        value = *entry + name_len + 1;
    }
    if (value == NULL)
        fail("required environment variable is absent");
    return value;
}

static size_t bounded_length(const char *value, size_t maximum, const char *reason)
{
    const size_t length = strnlen(value, maximum + 1);

    if (length == 0 || length > maximum)
        fail(reason);
    return length;
}

static bool is_lower_hex(const char *value, size_t length)
{
    for (size_t index = 0; index < length; index++) {
        const unsigned char byte = (unsigned char)value[index];
        if (!((byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f')))
            return false;
    }
    return true;
}

static uint64_t capability_set(int which)
{
    struct capability_header header = {
        .version = LINUX_CAPABILITY_VERSION_3,
        .pid = 0,
    };
    struct capability_data data[2] = {{0}};

    if (syscall(SYS_capget, &header, data) != 0)
        fail("capget failed");
    switch (which) {
    case 0:
        return ((uint64_t)data[1].effective << 32) | data[0].effective;
    case 1:
        return ((uint64_t)data[1].permitted << 32) | data[0].permitted;
    case 2:
        return ((uint64_t)data[1].inheritable << 32) | data[0].inheritable;
    default:
        fail("invalid capability set selector");
    }
}

static uint64_t bounding_capability_set(void)
{
    uint64_t result = 0;

    for (int capability = 0; capability <= MAX_CAPABILITY; capability++) {
        errno = 0;
        const int present = prctl(PR_CAPBSET_READ, capability, 0, 0, 0);
        if (present < 0) {
            if (errno == EINVAL)
                break;
            fail("capability prctl failed");
        }
        if (present != 0)
            result |= UINT64_C(1) << capability;
    }
    return result;
}

static uint64_t ambient_capability_set(void)
{
    uint64_t result = 0;

    for (int capability = 0; capability <= MAX_CAPABILITY; capability++) {
        errno = 0;
        const int present = prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_IS_SET,
                                  capability, 0, 0);
        if (present < 0) {
            if (errno == EINVAL)
                break;
            fail("ambient capability prctl failed");
        }
        if (present != 0)
            result |= UINT64_C(1) << capability;
    }
    return result;
}

static void set_capabilities(uint64_t effective, uint64_t permitted, uint64_t inheritable)
{
    struct capability_header header = {
        .version = LINUX_CAPABILITY_VERSION_3,
        .pid = 0,
    };
    struct capability_data data[2] = {{0}};

    data[0].effective = (uint32_t)effective;
    data[0].permitted = (uint32_t)permitted;
    data[0].inheritable = (uint32_t)inheritable;
    data[1].effective = (uint32_t)(effective >> 32);
    data[1].permitted = (uint32_t)(permitted >> 32);
    data[1].inheritable = (uint32_t)(inheritable >> 32);
    if (syscall(SYS_capset, &header, data) != 0)
        fail("capset failed");
}

static void verify_ids(void)
{
    uid_t real_uid = 0;
    uid_t effective_uid = 0;
    uid_t saved_uid = 0;
    gid_t real_gid = 0;
    gid_t effective_gid = 0;
    gid_t saved_gid = 0;

    require(getresuid(&real_uid, &effective_uid, &saved_uid) == 0,
            "getresuid failed");
    require(getresgid(&real_gid, &effective_gid, &saved_gid) == 0,
            "getresgid failed");
    require(real_uid == RUNNER_UID && effective_uid == RUNNER_UID &&
                saved_uid == RUNNER_UID,
            "runner uid boundary is invalid");
    require(real_gid == RUNNER_GID && effective_gid == RUNNER_GID &&
                saved_gid == RUNNER_GID,
            "runner gid boundary is invalid");
}

static void verify_initial_boundary(void)
{
    verify_ids();
    require(getppid() == 1 && getpid() != 1,
            "launcher is not the execd init child");
    require(getpgrp() == getpid(), "launcher is not its process-group leader");
    require(prctl(PR_GET_DUMPABLE, 0, 0, 0, 0) >= 0,
            "PR_GET_DUMPABLE failed");
    require(prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) == 0 &&
                prctl(PR_GET_DUMPABLE, 0, 0, 0, 0) == 0,
            "dumpable boundary is unavailable");
    require(capability_set(0) == EXPECTED_CAPS &&
                capability_set(1) == EXPECTED_CAPS && capability_set(2) == 0,
            "launcher capability sets are invalid");
    require(bounding_capability_set() == EXPECTED_CAPS,
            "launcher capability bounding set is invalid");
    require(ambient_capability_set() == 0,
            "launcher ambient capability set is not empty");
    require(prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) == 0,
            "launcher unexpectedly has no-new-privileges");
    require(prctl(PR_GET_SECUREBITS, 0, 0, 0, 0) == 0,
            "launcher securebits are invalid");
    require(prctl(PR_GET_SECCOMP, 0, 0, 0, 0) == 2,
            "launcher seccomp boundary is invalid");

    errno = 0;
    const int init_environment = open("/proc/1/environ", O_RDONLY | O_CLOEXEC);
    if (init_environment >= 0) {
        (void)close(init_environment);
        fail("execd environment is readable");
    }
    require(errno == EACCES || errno == EPERM,
            "execd environment protection could not be verified");
}

static void close_inherited_fds(void)
{
    if (syscall(SYS_close_range, 3U, UINT_MAX, 0U) == 0)
        return;

    require(errno == ENOSYS || errno == EINVAL || errno == EPERM,
            "close_range failed");
    struct rlimit limit = {0};
    require(getrlimit(RLIMIT_NOFILE, &limit) == 0 &&
                limit.rlim_cur != RLIM_INFINITY && limit.rlim_cur <= INT_MAX,
            "file descriptor limit is invalid");
    for (int fd = 3; fd < (int)limit.rlim_cur; fd++) {
        while (close(fd) != 0) {
            if (errno == EINTR)
                continue;
            require(errno == EBADF, "inherited file descriptor close failed");
            break;
        }
    }
}

static char *environment_entry(const char *name, const char *value, size_t value_len)
{
    const size_t name_len = strlen(name);
    char *entry = malloc(name_len + value_len + 2);

    if (entry == NULL)
        fail("environment allocation failed");
    memcpy(entry, name, name_len);
    entry[name_len] = '=';
    memcpy(entry + name_len + 1, value, value_len);
    entry[name_len + value_len + 1] = '\0';
    return entry;
}

int main(int argc, char **argv)
{
    const char *token;
    const char *config;
    const char *config_digest;
    const char *sandbox_id;
    size_t config_len;
    size_t sandbox_id_len;

    require(argc == 1 && argv != NULL && argv[0] != NULL,
            "launcher arguments are invalid");
    verify_initial_boundary();

    token = unique_env("EXECD_ACCESS_TOKEN");
    require(strnlen(token, 65) == 64 && is_lower_hex(token, 64),
            "execd access token is invalid");
    config = unique_env("INSIGHT_SANDBOX_RUNNER_CONFIG");
    config_len = bounded_length(config, MAX_RUNNER_CONFIG_BYTES,
                                "runner configuration is invalid");
    config_digest = unique_env("INSIGHT_SANDBOX_RUNNER_CONFIG_DIGEST");
    require(strnlen(config_digest, 72) == 71 &&
                memcmp(config_digest, "sha256:", 7) == 0 &&
                is_lower_hex(config_digest + 7, 64),
            "runner configuration digest is invalid");
    sandbox_id = unique_env("OPENSANDBOX_ID");
    sandbox_id_len = bounded_length(sandbox_id, MAX_SANDBOX_ID_BYTES,
                                   "OpenSandbox id is invalid");

    require(setgroups(0, NULL) == 0 && getgroups(0, NULL) == 0,
            "supplementary groups could not be cleared");
    set_capabilities(EXPECTED_CAPS, EXPECTED_CAPS, EXPECTED_CAPS);
    for (int capability = 0; capability <= 7; capability++) {
        if ((EXPECTED_CAPS & (UINT64_C(1) << capability)) == 0)
            continue;
        require(prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE, capability, 0, 0) == 0,
                "ambient capability handoff failed");
    }
    require(capability_set(0) == EXPECTED_CAPS &&
                capability_set(1) == EXPECTED_CAPS &&
                capability_set(2) == EXPECTED_CAPS &&
                ambient_capability_set() == EXPECTED_CAPS,
            "launcher capability handoff is invalid");
    require(prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0 &&
                prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) == 1,
            "launcher no-new-privileges handoff failed");
    require(prctl(PR_GET_SECUREBITS, 0, 0, 0, 0) == 0,
            "launcher securebits drifted");

    char *clean_environment[] = {
        environment_entry("INSIGHT_SANDBOX_RUNNER_CONFIG", config, config_len),
        environment_entry("INSIGHT_SANDBOX_RUNNER_CONFIG_DIGEST", config_digest, 71),
        environment_entry("OPENSANDBOX_ID", sandbox_id, sandbox_id_len),
        NULL,
    };
    char *core_argv[] = {CORE_PATH, NULL};

    explicit_bzero((void *)token, 64);
    close_inherited_fds();
    execve(CORE_PATH, core_argv, clean_environment);
    fail("runner core exec failed");
}
