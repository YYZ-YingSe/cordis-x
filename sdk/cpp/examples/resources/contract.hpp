#pragma once
#include <cordis/examples/counters.hpp>
namespace example::resources { inline cordis::Value counter_interface(){return fixtures_counters::CountersClient::interface();} }
