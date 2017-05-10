// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_CONFIGPARSER_H_
#define LOG2HDFS_UTIL_CONFIGPARSER_H_

#include <string>
#include <vector>
#include <fstream>
#include <memory>
#include <unordered_map>
#include "util/optional.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// Section

class Section {
 public:
  typedef std::unordered_map<std::string, std::string>::iterator iterator;
  typedef std::unordered_map<std::string, std::string>::const_iterator
      const_iterator;

  static std::shared_ptr<Section> Init() {
    return std::make_shared<Section>();
  }

  Section() {}

  Section(const Section& other): options_(other.options_) {}

  Section(Section&& other) {
    options_ = std::move(other.options_);
  }

  Section& operator=(const Section& other) {
    if (this != &other)
      options_ = other.options_;
    return *this;
  }

  Section& operator=(Section&& other) {
    options_ = std::move(other.options_);
    return *this;
  }

  bool Has(const std::string& option) const {
    auto it = options_.find(option);
    return it != options_.end();
  }

  bool Remove(const std::string& option) {
    return options_.erase(option);
  }

  Optional<std::string> Get(const std::string& option) const {
    auto it = options_.find(option);
    if (it == options_.end())
      return Optional<std::string>::Invalid();
    return Optional<std::string>(it->second);
  }

  std::string Get(const std::string& option,
                  const std::string& default_value) const {
    auto it = options_.find(option);
    if (it == options_.end()) {
      return default_value;
    } else {
      return it->second;
    }
  }

  bool Set(const std::string& option, const std::string& value) {
    options_[option] = value;
    return true;
  }

  Section::iterator Begin() {
    return options_.begin();
  }

  Section::iterator End() {
    return options_.end();
  }

  Section::const_iterator Begin() const {
    return options_.begin();
  }

  Section::const_iterator End() const {
    return options_.end();
  }

  bool Empty() const {
    return options_.empty();
  }

  void Clear() {
    options_.clear();
  }

  bool Read(const std::string& filepath);

  bool Write(const std::string& filepath) const;

  bool Write(std::ofstream& ofs) const;

 private:
  std::unordered_map<std::string, std::string> options_;
};

// ------------------------------------------------------------------
// KvConfigParser

typedef Section KvConfigParser;

// ------------------------------------------------------------------
// IniConfigParser

class IniConfigParser {
 public:
  typedef std::unordered_map<std::string, std::shared_ptr<Section>>::iterator
      iterator;
  typedef std::unordered_map<std::string, std::shared_ptr<Section>>
      ::const_iterator const_iterator;

  static std::shared_ptr<IniConfigParser> Init() {
    return std::make_shared<IniConfigParser>();
  }

  IniConfigParser() {}

  IniConfigParser(const IniConfigParser& other):
      sections_(other.sections_) {}

  IniConfigParser(IniConfigParser&& other):
      sections_(std::move(other.sections_)) {}

  IniConfigParser& operator=(const IniConfigParser& other) {
    if (this != &other)
      sections_ = other.sections_;
    return *this;
  }

  IniConfigParser& operator=(const IniConfigParser&& other) {
    sections_ = std::move(other.sections_);
    return *this;
  }

  bool HasSection(const std::string& section) const {
    auto it = sections_.find(section);
    return it != sections_.end();
  }

  bool AddSection(const std::string& section) {
    if (HasSection(section))
      return false;
    sections_[section] = Section::Init();
    return true;
  }

  bool RemoveSection(const std::string& section) {
    return sections_.erase(section);
  }

  std::shared_ptr<Section> GetSection(const std::string& section) {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return nullptr;
    return it->second;
  }

  bool HasOption(const std::string& section,
                 const std::string& option) const {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return false;
    return (it->second)->Has(option);
  }

  bool RemoveOption(const std::string& section,
                    const std::string& option) {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return false;
    return (it->second)->Remove(option);
  }

  Optional<std::string> Get(const std::string& section,
                  const std::string& option) const {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return Optional<std::string>::Invalid();
    return (it->second)->Get(option);
  }

  std::string Get(const std::string& section,
                  const std::string& option,
                  const std::string &default_value) const {
    auto it = sections_.find(section);
    if (it == sections_.end()) {
      return default_value;
    } else {
      return (it->second)->Get(option, default_value);
    }
  }

  bool Set(const std::string& section,
           const std::string& option,
           const std::string& value) {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return false;
    return (it->second)->Set(option, value);
  }

  IniConfigParser::iterator Begin() {
    return sections_.begin();
  }

  IniConfigParser::iterator End() {
    return sections_.end();
  }

  IniConfigParser::const_iterator Begin() const {
    return sections_.begin();
  }

  IniConfigParser::const_iterator End() const {
    return sections_.end();
  }

  bool Empty() const {
    return sections_.empty();
  }

  void Clear() {
    sections_.clear();
  }

  bool Read(const std::string& filepath);

  bool Write(const std::string& filepath) const;

 private:
  std::unordered_map<std::string, std::shared_ptr<Section>> sections_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_CONFIGPARSER_H_
