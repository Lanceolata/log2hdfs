#ifndef LOG2HDFS_UTIL_CONFIGPARSER_H
#define LOG2HDFS_UTIL_CONFIGPARSER_H

#include <string>
#include <memory>
#include <utility>
#include <vector>
#include <fstream>
#include <unordered_map>

namespace log2hdfs {

namespace util {

// ------------------------------------------------------------------
// Section
 
class Section;

typedef std::shared_ptr<Section> SectionPtr;

typedef std::unordered_map<std::string, std::string> OptionMap;
typedef OptionMap::const_iterator OptionConstIter;

class Section {
 public:

  static SectionPtr Make() {
    return std::make_shared<Section>();
  }
  static SectionPtr Make(const std::string& separator) {
    return std::make_shared<Section>(separator);
  }

  Section():separator_("=") {}
  Section(const std::string& separator):separator_(separator) {}

  ~Section() {}

  Section(const Section& s) {
    separator_ = s.separator_;
    options_ = s.options_;
  }
  Section& operator=(const Section& s) {
    separator_ = s.separator_;
    options_ = s.options_;
  }

  const std::string& get_separator() const {
    return separator_;
  }

  void set_separator(const std::string& separator) {
    separator_ = separator;
  }

  bool read(const std::string& filepath);

  bool has(const std::string& option) const {
    OptionConstIter it = options_.find(option);
    return it != options_.end();
  }

  bool remove(const std::string& option) {
    return options_.erase(option);
  }

  const std::string *get(const std::string& option) const {
    OptionConstIter it = options_.find(option);
    if (it == options_.end()) {
      return nullptr;
    }
    return &(it->second);
  }

  void set(const std::string& option, const std::string& value) {
    options_[option] = value;
  }

  std::pair<OptionConstIter, OptionConstIter> options() const {
    return std::make_pair(options_.begin(), options_.end());
  }

  bool write(const std::string& filepath) const;
  bool write(const std::string& filepath, const std::string& separator) const;

  bool write(std::ofstream& ofs) const;
  bool write(std::ofstream& ofs, const std::string& separator) const;

 private:

  void parse(std::vector<std::string>& lines);

  std::string separator_;

  OptionMap options_;
};


// ------------------------------------------------------------------
// KvConfigParser

typedef Section KvConfigParser;
typedef SectionPtr KvConfigPtr;


// ------------------------------------------------------------------
// IniConfigParser

class IniConfigParser;

typedef std::shared_ptr<IniConfigParser> IniConfigPtr;

typedef std::unordered_map<std::string, Section> SectionMap;
typedef SectionMap::iterator SectionIter;
typedef SectionMap::const_iterator SectionConstIter;

class IniConfigParser {
 public:

  static IniConfigPtr Make() {
    return std::make_shared<IniConfigParser>();
  }
  static IniConfigPtr Make(const std::string& separator) {
    return std::make_shared<IniConfigParser>(separator);
  }

  IniConfigParser(): separator_("=") {}
  IniConfigParser(const std::string& separator): separator_(separator) {}

  IniConfigParser(const IniConfigParser& icp) {
    separator_ = icp.separator_;
    sections_ = icp.sections_;
  }
  IniConfigParser& operator=(const IniConfigParser& icp) {
    separator_ = icp.separator_;
    sections_ = icp.sections_;
  }

  const std::string& get_separator() const {
    return separator_;
  }

  void set_separator(const std::string& separator) {
    separator_ = separator;
  }

  bool read(const std::string& filepath);

  bool has_section(const std::string& section) const {
    SectionConstIter it = sections_.find(section);
    return it != sections_.end();
  }

  bool add_section(const std::string& section) {
    if (has_section(section)) {
      return false;
    }
    sections_[section] = Section();
    return true;
  }

  bool remove_section(const std::string& section) {
    return sections_.erase(section);
  }
  
  bool has_option(const std::string& section, 
          const std::string& option) const {
    SectionConstIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    return (it->second).has(option);
  }

  bool remove_option(const std::string& section, const std::string& option) {
    SectionIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    return (it->second).remove(option);
  }

  const std::string *get(const std::string& section,
          const std::string& option) const {
    SectionConstIter it = sections_.find(section);
    if (it == sections_.end()) {
      return nullptr;
    }
    return (it->second).get(option);
  }

  bool set(const std::string& section, const std::string& option,
          const std::string& value) {
    SectionIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    (it->second).set(option, value);
    return true;
  }

  std::pair<SectionConstIter, SectionConstIter> sections() const {
    return std::make_pair(sections_.begin(), sections_.end());
  }

  bool options(const std::string& section, 
          std::pair<OptionConstIter, OptionConstIter>& p) const {
    SectionConstIter it = sections_.find(section);
    if (it == sections_.end()) {
      return false;
    }
    p = (it->second).options();
    return true;
  }

  bool write(const std::string& filepath) const;
  bool write(const std::string& filepath, const std::string& separator) const;

 private:

  void parse(std::vector<std::string>& lines);

  // separator
  std::string separator_;

  // A unordered_multimap to hold all sections
  SectionMap sections_;

};

// ------------------------------------------------------------------
// ConfigParser

class ConfigParser {
 public:

  static KvConfigPtr KvConfig() {
    return KvConfigParser::Make();
  }

  static KvConfigPtr KvConfig(const std::string& separator) {
    return KvConfigParser::Make(separator);
  }

  static IniConfigPtr IniConfig() {
    return IniConfigParser::Make();
  }

  static IniConfigPtr IniConfig(const std::string& separator) {
    return IniConfigParser::Make(separator);
  }
};

}   // namespace log2hdfs

}   // namespace util

#endif  // LOG2HDFS_UTIL_CONFIGPARSER_H
