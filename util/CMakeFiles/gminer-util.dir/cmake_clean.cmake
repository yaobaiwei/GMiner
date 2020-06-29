file(REMOVE_RECURSE
  "libgminer-util.pdb"
  "libgminer-util.a"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/gminer-util.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
