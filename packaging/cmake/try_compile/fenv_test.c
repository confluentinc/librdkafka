#include <fenv.h>

int main() {
        feclearexcept(FE_DIVBYZERO | FE_INVALID);
        return fetestexcept(FE_DIVBYZERO | FE_INVALID);
}
