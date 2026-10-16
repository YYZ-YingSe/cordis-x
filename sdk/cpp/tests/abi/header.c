#include <cordis/abi.h>
#include <cordis/abi_composition.h>
#include <cordis/abi_context.h>
#include <cordis/abi_reactive.h>
/* Compile the exact public header as C, without any C++ types or dependencies.
 */
int main(void) {
  CordisAbiApi api = {0};
  CordisAbiResult result = {0};
  CordisAbiContextApi context = {0};
  CordisAbiHostApi host = {0};
  CordisAbiManagedApi managed = {0};
  CordisAbiCompositionApi composition = {0};
  return (api.abi_version != 0 || result.status != CORDIS_ABI_OK ||
          context.version != 0 || host.version != 0 || managed.version != 0 ||
          composition.version != 0);
}
