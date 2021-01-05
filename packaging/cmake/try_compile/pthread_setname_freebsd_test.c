#include <pthread.h>
#include <pthread_np.h>

int main() {
  pthread_set_name_np("abc");
  return 0;
}
