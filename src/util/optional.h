// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_OPTIONAL_H_
#define LOG2HDFS_UTIL_OPTIONAL_H_

#include <string>
#include <utility>

namespace log2hdfs {

template <class T>
class Optional {
 public:
  static Optional Invalid() {
    return Optional();
  }

  Optional():valid_(false), value_(T()) {}

  explicit Optional(const T &value):
      valid_(true), value_(value) {}

  Optional(const Optional &other): valid_(other.valid_),
      value_(other.value_) {}

  Optional &operator=(const Optional &other) {
    if (this != &other) {
      valid_ = other.valid_;
      value_ = other.value_;
    }
    return *this;
  }

  Optional &operator=(const T &value) {
    value_ = value;
    valid_ = true;
    return *this;
  }

  Optional &operator=(T &&value) {
    value_ = std::move(value);
    valid_ = true;
    return *this;
  }

  void Reset() {
    valid_ = false;
  }

  bool valid() {
    return valid_;
  }

  const T &value() {
    return value_;
  }

  bool operator==(const Optional &other) const {
    return (valid_ == other.valid_) &&
        (!valid_ || value_ == other.value_);
  }

  bool operator!=(const Optional &other) const {
    return !(*this == other);
  }

 private:
  bool valid_;
  T value_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_OPTIONAL_H_
