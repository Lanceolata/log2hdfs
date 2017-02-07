// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_CONFIGPARSERIMP_H_
#define LOG2HDFS_UTIL_CONFIGPARSERIMP_H_

#include <string>
#include <vector>
// #include "util/configparser.h"
#include "configparser.h"

namespace log2hdfs {

namespace util {

namespace constants {

const char SECTION_START = '[';
const char SECTION_END = ']';
const char DECIMAL = '.';
const char COMMA = ',';
const char SPACE = ' ';

}   // namespace constants

// ------------------------------------------------------------------
// Parser

class Parser {
 public:
  Parser(): separator_("=") {}
  explicit Parser(const std::string &separator): separator_(separator) {}

  Parser(const Parser &p): separator_(p.separator_) {}
  Parser &operator=(const Parser &p) {
    if (this != &p) {
      separator_ = p.separator_;
    }
    return *this;
  }

  static bool Load(const std::string &filepath,
                   std::vector<std::string> *lines);

  bool IsSection(const std::string &line) const {
    if (line.length() < 3 || line[0] != constants::SECTION_START
            || line[line.length() - 1] != constants::SECTION_END) {
      return false;
    }
    return true;
  }

  std::string GetSection(const std::string &line) const {
    std::string section = line.substr(1, line.length()-2);
    return section;
  }

  bool Parse(const std::string &line, std::string *key,
             std::string *value) const;

 private:
  std::string separator_;
};

// ------------------------------------------------------------------
// SectionImp

class SectionImp: public Section {
 public:
  static SectionPtr Init() {
    return SectionPtr(new SectionImp());
  }
  static SectionPtr Init(const std::string &separator) {
    return SectionPtr(new SectionImp(separator));
  }

  SectionImp(): separator_("=") {}
  explicit SectionImp(const std::string &separator): separator_(separator) {}
  SectionImp(const std::string &separator, const OptionMap &options):
      separator_(separator), options_(options) {}

  ~SectionImp() {}

  SectionImp(const SectionImp &si): separator_(si.separator_),
    options_(si.options_) {}
  SectionImp &operator=(const SectionImp &si) {
    if (this != &si) {
      separator_ = si.separator_;
      options_ = si.options_;
    }
    return *this;
  }

  const std::string &separator() const {
    return separator_;
  }
  bool set_separator(const std::string &separator) {
    if (separator.empty()) {
      return false;
    }
    separator_.assign(separator);
    return true;
  }

  bool Read(const std::string &filepath);

  bool Has(const std::string &option) const {
    OptionConstIter it = options_.find(option);
    return it != options_.end();
  }

  bool Remove(const std::string &option) {
    return options_.erase(option);
  }

  const std::string *Get(const std::string &option) const {
    OptionConstIter it = options_.find(option);
    if (it == options_.end()) {
      return nullptr;
    }
    return &(it->second);
  }

  bool Set(const std::string &option, const std::string &value) {
    options_[option] = value;
    return true;
  }

  OptionIter Begin() {
    return options_.begin();
  }
  OptionConstIter Begin() const {
    return options_.begin();
  }

  OptionIter End() {
    return options_.end();
  }
  OptionConstIter End() const {
    return options_.end();
  }

  bool Options(OptionIter *begin, OptionIter *end) {
    *begin = options_.begin();
    *end = options_.end();
    return true;
  }
  bool Options(OptionConstIter *begin, OptionConstIter *end) const {
    *begin = options_.begin();
    *end = options_.end();
    return true;
  }

  bool Empty() const {
    return options_.empty();
  }

  SectionPtr Copy() const {
    return SectionPtr(new SectionImp(separator_, options_));
  }

  void Clear() {
    options_.clear();
  }

  bool Write(const std::string &filepath) const;
  bool Write(const std::string &filepath, const std::string &separator) const;

  bool Write(std::ofstream &ofs) const;
  bool Write(std::ofstream &ofs, const std::string &separator) const;

 private:
  void Parse(std::vector<std::string> *lines);

  std::string separator_;

  OptionMap options_;
};

// ------------------------------------------------------------------
// IniConfigParserImp

class IniConfigParserImp: public IniConfigParser {
 public:
  static IniConfigPtr Init() {
    return IniConfigPtr(new IniConfigParserImp());
  }
  static IniConfigPtr Init(const std::string &separator) {
    return IniConfigPtr(new IniConfigParserImp(separator));
  }

  IniConfigParserImp(): separator_("=") {}
  explicit IniConfigParserImp(const std::string &separator):
      separator_(separator) {}
  IniConfigParserImp(const std::string &separator, const SectionMap &sections):
      separator_(separator), sections_(sections) {}

  ~IniConfigParserImp() {}

  IniConfigParserImp(const IniConfigParserImp &icpi):
      separator_(icpi.separator_), sections_(icpi.CopySections()) {}
  IniConfigParserImp &operator=(const IniConfigParserImp &icpi) {
    if (this != &icpi) {
      separator_ = icpi.separator_;
      sections_ = icpi.CopySections();
    }
    return *this;
  }

  const std::string &separator() const {
    return separator_;
  }
  bool set_separator(const std::string &separator) {
    if (separator.empty()) {
      return false;
    }
    separator_.assign(separator);
    return true;
  }

  bool Read(const std::string &filepath);

  bool HasSection(const std::string &section) const {
    SectionConstIter it = sections_.find(section);
    return it != sections_.end();
  }

  bool AddSection(const std::string &section) {
    if (HasSection(section)) {
      return false;
    }
    sections_[section] = Section::Create(separator_);
    return true;
  }

  bool RemoveSection(const std::string &section) {
    return sections_.erase(section);
  }

  SectionPtr GetSection(const std::string &section) {
    SectionIter it = sections_.find(section);
    if (it == sections_.end()) {
      return nullptr;
    }
    return it->second;
  }

  bool HasOption(const std::string &section,
                 const std::string &option) const {
    SectionConstIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    return (it->second)->Has(option);
  }

  bool RemoveOption(const std::string &section, const std::string &option) {
    SectionIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    return (it->second)->Remove(option);
  }

  const std::string *Get(const std::string &section,
                         const std::string &option) const {
    SectionConstIter it = sections_.find(section);
    if (it == sections_.end()) {
      return nullptr;
    }
    return (it->second)->Get(option);
  }

  bool Set(const std::string &section, const std::string &option,
           const std::string &value) {
    SectionIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    return (it->second)->Set(option, value);
  }

  SectionIter Begin() {
    return sections_.begin();
  }
  SectionConstIter Begin() const {
    return sections_.begin();
  }

  SectionIter End() {
    return sections_.end();
  }
  SectionConstIter End() const {
    return sections_.end();
  }

  bool Sections(SectionIter *begin, SectionIter *end) {
    *begin = sections_.begin();
    *end = sections_.end();
    return true;
  }
  bool Sections(SectionConstIter *begin, SectionConstIter *end) const {
    *begin = sections_.begin();
    *end = sections_.end();
    return true;
  }

  bool Options(const std::string &section, OptionIter *begin,
               OptionIter *end) {
    SectionIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    *begin = (it->second)->Begin();
    *end = (it->second)->End();
    return true;
  }
  bool Options(const std::string &section, OptionConstIter *begin,
               OptionConstIter *end) const {
    SectionConstIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    *begin = (it->second)->Begin();
    *end = (it->second)->End();
    return true;
  }

  bool Empty() const {
    return sections_.empty();
  }

  IniConfigPtr Copy() const {
    SectionMap sm = CopySections();
    return IniConfigPtr(new IniConfigParserImp(separator_, sm));
  }

  void Clear() {
    sections_.clear();
  }

  bool Write(const std::string &filepath) const;
  bool Write(const std::string &filepath, const std::string &separator) const;

 private:
  SectionMap CopySections() const {
    SectionMap sm;
    SectionConstIter sci;
    for (sci = sections_.begin(); sci != sections_.end(); sci++) {
      sm[sci->first] = (sci->second)->Copy();
    }
    return sm;
  }

  void Parse(std::vector<std::string> *lines);

  std::string separator_;

  SectionMap sections_;
};

}   // namespace util

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_CONFIGPARSERIMP_H
