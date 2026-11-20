#pragma once
#include <algorithm>
#include <cstddef>
#include <limits>
#include <map>

namespace cordis::detail {
// Registry owners hold their existing lock while sweeping terminal history.
class RetiredSweep {
  std::size_t threshold = 4096;
public:
  template <class Map, class Retired>
  void collect(Map &entries, Retired retired) {
    if (entries.size() < threshold) return;
    std::erase_if(entries, retired);
    const auto size = entries.size();
    threshold = size + std::min(std::max<std::size_t>(4096, size),
                                std::numeric_limits<std::size_t>::max() - size);
  }
};
} // namespace cordis::detail
