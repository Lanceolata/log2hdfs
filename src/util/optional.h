// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_OPTIONAL_H_
#define LOG2HDFS_UTIL_OPTIONAL_H_

#include <utility>

/**
 * Represents a type that may be invalid, similar to std::optional.
 */
template <class T>
class Optional {
 public:
  /**
   * Static function to create a invalid Optional object.
   * 
   * @returns invalid optional object.
   */
  static Optional Invalid() {
    return Optional();
  }

  /**
   * Constructor
   */
  Optional():valid_(false), value_(T()) {}

  /**
   * Constructor
   * 
   * Implicit conversion from template value.
   */
  explicit Optional(const T& value):
      valid_(true), value_(value) {}

  /**
   * Copy constructor
   */
  Optional(const Optional& other):
      valid_(other.valid_), value_(other.value_) {}

  /**
   * Rvalue constructor
   */
  Optional(Optional&& other):
      valid_(other.valid_), value_(std::move(other.value_)) {
    other.valid_ = false;
  }

  /**
   * Copy assignment function
   */
  Optional& operator=(const Optional& other) {
    if (this != &other) {
      valid_ = other.valid_;
      value_ = other.value_;
    }
    return *this;
  }

  /**
   * Rvalue assignment function
   */
  Optional& operator=(Optional&& other) {
    value_ = std::move(other.value_);
    valid_ = other.valid_;
    other.valid_ = false;
    return *this;
  }

  /**
   * Rvalue assignment function
   * 
   * Rvalue implicit conversion from template value.
   */
  Optional& operator=(T&& value) {
    value_ = std::move(value);
    valid_ = true;
    return *this;
  }

  /**
   * Reset Optional
   * 
   * Set valid to false.
   */
  void Reset() {
    valid_ = false;
  }

  /**
   * Optional valid
   *
   * @returns true if valid; false otherwise.
   */
  bool valid() {
    return valid_;
  }

  /**
   * Optional value
   * 
   * @returns value if optional valid; undefined behavior otherwise.
   */
  const T& value() {
    return value_;
  }

  /**
   * Operators ==
   * 
   * @returns true if equal; false otherwise.
   */
  bool operator==(const Optional& other) const {
    return (valid_ == other.valid_) && (!valid_ || value_ == other.value_);
  }

  /**
   * Operators =!
   * 
   * @returns true if not equal; false otherwise.
   */
  bool operator!=(const Optional& other) const {
    return !(*this == other);
  }

 private:
  bool valid_;
  T value_;
};

#endif  // LOG2HDFS_UTIL_OPTIONAL_H_
