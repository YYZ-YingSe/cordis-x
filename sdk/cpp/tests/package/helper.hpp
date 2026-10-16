#pragma once
#if defined(_WIN32)
#if defined(CORDIS_BUILD_HELPER)
#define CORDIS_HELPER_API __declspec(dllexport)
#else
#define CORDIS_HELPER_API __declspec(dllimport)
#endif
#else
#define CORDIS_HELPER_API __attribute__((visibility("default")))
#endif
extern "C" CORDIS_HELPER_API const char* CORDIS_HELPER_SYMBOL();
