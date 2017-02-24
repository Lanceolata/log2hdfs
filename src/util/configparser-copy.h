// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_CONFIGPARSER_H_
#define LOG2HDFS_UTIL_CONFIGPARSER_H_

#include <string>
#include <vector>
#include <memory>
#include <fstream>
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

  static std::shared_ptr<Section> Init(const std::string &delimiters) {
    return std::make_shared<Section>(delimiters);
  }

  Section(): delimiters_("=") {}

  explicit Section(const std::string &delimiters): delimiters_(delimiters) {}

  Section(const Section &other): delimiters_(other.delimiters_),
      options_(other.options_) {}

  Section(Section &&other) {
    delimiters_ = std::move(other.delimiters_);
    options_ = std::move(other.options_);
  }

  ~Section() {}

  Section &operator=(const Section &other) {
    if (this != &other) {
      delimiters_ = other.delimiters_;
      options_ = other.options_;
    }
    return *this;
  }

  Section &operator=(Section &&other) {
    if (this != &other) {
      delimiters_ = std::move(other.delimiters_);
      options_ = std::move(other.options_);
    }
    return *this;
  }

  const std::string &separator() const {
    return delimiters_;
  }

  bool set_delimiters(const std::string &delimiters) {
    if (delimiters.empty()) {
      return false;
    }
    delimiters_.assign(delimiters);
    return true;
  }

  bool Has(const std::string &option) const {
    Section::const_iterator it = options_.find(option);
    return it != options_.end();
  }

  bool Remove(const std::string &option) {
    return options_.erase(option);
  }

  Optional<std::string> Get(const std::string &option) const {
    Section::const_iterator it = options_.find(option);
    if (it == options_.end()) {
      return Optional<std::string>::Invalid();
    }
    return Optional<std::string>(it->second);
  }

  bool Set(const std::string &option, const std::string &value) {
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

  bool Read(const std::string &filepath);

  bool Write(const std::string &filepath) const;

  bool Write(const std::string &filepath, const std::string &delimiters) const;

  bool Write(std::ofstream &ofs) const;

  bool Write(std::ofstream &ofs, const std::string &delimiters) const;

 private:
  void Parse(std::vector<std::string> *lines);

  std::string delimiters_;
  std::unordered_map<std::string, std::string> options_;
};

// ------------------------------------------------------------------
// KvConfigParser

typedef Section KvConfigParser;

// ------------------------------------------------------------------
// IniConfigParser

class IniConfigParser;

typedef std::shared_ptr<IniConfigParser> IniConfigPtr;
typedef std::unordered_map<std::string, std::shared_ptr<Section>> SectionMap;
typedef SectionMap::iterator SectionIter;
typedef SectionMap::const_iterator SectionConstIter;

class IniConfigParser {
 public:
  typedef std::unordered_map<std::string, std::shared_ptr<Section> >::iterator
      iterator;

  typedef std::unordered_map<std::string, std::shared_ptr<Section> >::const_iterator
      const_iterator;

  static std::shared_ptr<IniConfigParser> Init() {
    return std::make_shared<IniConfigParser>();
  }

  static std::shared_ptr<IniConfigParser> Init(
      const std::string &delimiters) {
    if (delimiters.empty()) {
      return nullptr;
    }
    return std::make_shared<IniConfigParser>(delimiters);
  }


 private:
  void Parse(std::vector<std::string> *lines);

  std::string delimiters_;
  std::unordered_map<std::string, std::shared_ptr<Section> > sections_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_CONFIGPARSER_H_
