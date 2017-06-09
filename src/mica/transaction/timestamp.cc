#include "timestamp.h"

namespace mica {
namespace transaction {

volatile uint64_t CentralizedTimestamp::next_t2 = 0;

}
}
