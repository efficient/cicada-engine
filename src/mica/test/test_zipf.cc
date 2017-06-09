#include "mica/util/zipf.h"
#include <cstdlib>

int main() {
  ::mica::util::ZipfGen::test(-1.);
  ::mica::util::ZipfGen::test(0.0);
  ::mica::util::ZipfGen::test(0.99);
  ::mica::util::ZipfGen::test(40.);

  return EXIT_SUCCESS;
}
