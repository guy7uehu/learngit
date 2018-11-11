// Copyright [2018]
//

#ifndef ENGINE_RACE_UTIL_MUTEXLOCK_H_
#define ENGINE_RACE_UTIL_MUTEXLOCK_H_

#include "port_stdcxx.h"
#include "thread_annotations.h"

namespace polar_race {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(port::Mutex *mu) EXCLUSIVE_LOCK_FUNCTION(mu)
      : mu_(mu)  {
    this->mu_->Lock();
  }
  ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

  MutexLock(const MutexLock&) = delete;
  MutexLock& operator=(const MutexLock&) = delete;

 private:
  port::Mutex *const mu_;
};

}  // namespace polar_race


#endif  // ENGINE_RACE_UTIL_MUTEXLOCK_H_