#include "helper.hpp"
#ifdef CORDIS_HELPER_CHILD
#ifdef _WIN32
extern "C" __declspec(dllimport) const char* CORDIS_HELPER_CHILD();
#else
extern "C" const char* CORDIS_HELPER_CHILD();
#endif
#endif
extern "C" CORDIS_HELPER_API const char* CORDIS_HELPER_SYMBOL() {
#ifdef CORDIS_HELPER_CHILD
    return CORDIS_HELPER_CHILD();
#else
    return CORDIS_HELPER_VERSION;
#endif
}
