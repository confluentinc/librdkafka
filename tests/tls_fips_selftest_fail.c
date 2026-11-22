/* Linux test-only fault injection. Never link into librdkafka or an installed
 * client. Build: cc -shared -fPIC tests/tls_fips_selftest_fail.c -o fail.so
 * Run the policy test in an isolated process with LD_PRELOAD=/absolute/fail.so.
 * This verifies refusal control flow, not actual module self-test coverage. */
typedef struct ossl_provider_st OSSL_PROVIDER;
int OSSL_PROVIDER_self_test(const OSSL_PROVIDER *provider) {
        (void)provider;
        return 0;
}
