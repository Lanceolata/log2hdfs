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

  /**
   * Static function to create a Section shared_ptr.
   * 
   * @return std::shared_ptr<Section>.
   */
  static std::shared_ptr<Section> Init() {
    return std::make_shared<Section>();
  }

  /**
   * Constructor.
   */
  Section() {}

  /**
   * Copy Constructor.
   */
  Section(const Section& other): options_(other.options_) {}

  /**
   * Rvalue Constructor.
   */
  Section(Section&& other) {
    options_ = std::move(other.options_);
  }

  /**
   * Copy assignment function.
   */
  Section& operator=(const Section& other) {
    if (this != &other)
      options_ = other.options_;
    return *this;
  }

  /**
   * Rvalue assignment function.
   */
  Section& operator=(Section&& other) {
    options_ = std::move(other.options_);
    return *this;
  }

  /**
   * Whether section has option.
   * 
   * @param option      option to match.
   * 
   * @return            true if has the option; false otherwise.
   */
  bool Has(const std::string& option) const {
    auto it = options_.find(option);
    return it != options_.end();
  }

  /**
   * Remove option.
   * 
   * @param option      option to remove.
   * 
   * @return            true if remove success; false otherwise.
   */
  bool Remove(const std::string& option) {
    return options_.erase(option);
  }

  /**
   * Get option value.
   * 
   * @param option      option to match.
   * 
   * @return            Optional<std::string> if get success;
   *                    Optional<std::string>::Invalid() otherwise.
   */
  Optional<std::string> Get(const std::string& option) const {
    auto it = options_.find(option);
    if (it == options_.end())
      return Optional<std::string>::Invalid();
    return Optional<std::string>(it->second);
  }

  /**
   * Get option value.
   * 
   * @param option          option to match.
   * @param default_value   default value to return when option not found.
   * 
   * @return                option valid if get success;
   *                        default_value otherwise.
   */
  std::string Get(const std::string& option,
                  const std::string& default_value) const {
    auto it = options_.find(option);
    if (it == options_.end()) {
      return default_value;
    } else {
      return it->second;
    }
  }

  /**
   * Set option value.
   * 
   * @param option          option to set.
   * @param value           set value.
   * 
   * @return                true if set success; false otherwise.
   */
  bool Set(const std::string& option, const std::string& value) {
    options_[option] = value;
    return true;
  }

  /**
   * First element iterator.
   * 
   * @return an iterator pointing to the first element.
   */
  Section::iterator Begin() {
    return options_.begin();
  }

  /**
   * Past-the-end element iterator.
   * 
   * @return a iterator pointing to the past-the-end element.
   */
  Section::iterator End() {
    return options_.end();
  }

  /**
   * First element const iterator.
   * 
   * @return an const iterator pointing to the first element.
   */
  Section::const_iterator Begin() const {
    return options_.begin();
  }

  /**
   * Past-the-end element const iterator.
   *
   * @return a const iterator pointing to the past-the-end element.
   */
  Section::const_iterator End() const {
    return options_.end();
  }

  /**
   * Whether section is empty.
   * 
   * @return true if section is empty; false otherwise.
   */
  bool Empty() const {
    return options_.empty();
  }

  /**
   * Clear all options in section.
   */
  void Clear() {
    options_.clear();
  }

  /**
   * Read section from file.
   * 
   * @param filepath        file path to read.
   * 
   * @return true if read success and set option values; false otherwise.
   */
  bool Read(const std::string& filepath);

  /**
   * Save section to file.
   * 
   * @param filepath        file path to write.
   * 
   * @return true if write success; false otherwise.
   */
  bool Write(const std::string& filepath) const;

  /**
   * Write section to file output stream.
   * 
   * @param ofs             file output stream to write.
   * 
   * @return true if write success; false otherwise.
   */
  bool Write(std::ofstream& ofs) const;

 private:
  std::unordered_map<std::string, std::string> options_;
};

// ------------------------------------------------------------------
// KvConfigParser

typedef Section KvConfigParser;

// ------------------------------------------------------------------
// IniConfigParser

/**
 * Parser ini config file
 */
class IniConfigParser {
 public:
  typedef std::unordered_map<std::string, std::shared_ptr<Section>>::iterator
      iterator;
  typedef std::unordered_map<std::string, std::shared_ptr<Section>>
      ::const_iterator const_iterator;

  /**
   * Static function to create a IniConfigParser shared_ptr.
   * 
   * @return std::shared_ptr<IniConfigParser>.
   */
  static std::shared_ptr<IniConfigParser> Init() {
    return std::make_shared<IniConfigParser>();
  }

  /**
   * Constructor.
   */
  IniConfigParser() {}

  /**
   * Copy Constructor.
   */
  IniConfigParser(const IniConfigParser& other):
      sections_(other.sections_) {}

  /**
   * Rvalue Constructor.
   */
  IniConfigParser(IniConfigParser&& other):
      sections_(std::move(other.sections_)) {}

  /**
   * Copy assignment function.
   */
  IniConfigParser& operator=(const IniConfigParser& other) {
    if (this != &other)
      sections_ = other.sections_;
    return *this;
  }

  /**
   * Rvalue assignment function.
   */
  IniConfigParser& operator=(const IniConfigParser&& other) {
    sections_ = std::move(other.sections_);
    return *this;
  }

  /**
   * Whether has section.
   */
  bool HasSection(const std::string& section) const {
    auto it = sections_.find(section);
    return it != sections_.end();
  }

  /**
   * Add section.
   * 
   * @param section     section to add.
   * 
   * @return            true if add success; false otherwise.
   */
  bool AddSection(const std::string& section) {
    if (HasSection(section))
      return false;
    sections_[section] = Section::Init();
    return true;
  }

  /**
   * Remove section.
   * 
   * @param section     section to remove.
   * 
   * @return            true if remove success; false otherwise.
   */
  bool RemoveSection(const std::string& section) {
    return sections_.erase(section);
  }

  /**
   * Get section.
   * 
   * @param             section to match.
   * 
   * @return            std::shared_ptr<Section> if get success;
   *                    nullptr otherwise.
   */
  std::shared_ptr<Section> GetSection(const std::string& section) {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return nullptr;
    return it->second;
  }

  /**
   * Whether section has option.
   * 
   * @param section     section to match.
   * @param option      option to match.
   * 
   * @return            true if has section and section has option.
   */
  bool HasOption(const std::string& section,
                 const std::string& option) const {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return false;
    return (it->second)->Has(option);
  }

  /**
   * Remove option in section.
   * 
   * @param section     section to match.
   * @param option      option to remove.
   * 
   * @return            true if remove option in section success;
   *                    false otherwise.
   */
  bool RemoveOption(const std::string& section,
                    const std::string& option) {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return false;
    return (it->second)->Remove(option);
  }

  /**
   * Get option in section.
   * 
   * @param section     section to match.
   * @param option      option to match.
   * 
   * @return            Optional<std::string> if get option in section;
   *                    Optional<std::string>::Invalid otherwise.
   */
  Optional<std::string> Get(const std::string& section,
                  const std::string& option) const {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return Optional<std::string>::Invalid();
    return (it->second)->Get(option);
  }

  /**
   * Get option in section.
   * 
   * @param section         section to match.
   * @param option          option to match.
   * @param default_value   default value to return when option not found.
   * 
   * @return                option valid if get success;
   *                        default_value otherwise.
   */
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

  /**
   * Set option value in section.
   * 
   * @param section         section to match.
   * @param option          option to set.
   * @param value           set value.
   * 
   * @return                true if set success; false otherwise.
   */
  bool Set(const std::string& section,
           const std::string& option,
           const std::string& value) {
    auto it = sections_.find(section);
    if (it == sections_.end())
      return false;
    return (it->second)->Set(option, value);
  }

  /**
   * First element iterator.
   * 
   * @return an iterator pointing to the first element.
   */
  IniConfigParser::iterator Begin() {
    return sections_.begin();
  }

  /**
   * Past-the-end element iterator.
   * 
   * @return a iterator pointing to the past-the-end element.
   */
  IniConfigParser::iterator End() {
    return sections_.end();
  }

  /**
   * First element const iterator.
   * 
   * @return an const iterator pointing to the first element.
   */
  IniConfigParser::const_iterator Begin() const {
    return sections_.begin();
  }

  /**
   * Past-the-end element const iterator.
   * 
   * @return a const iterator pointing to the past-the-end element.
   */
  IniConfigParser::const_iterator End() const {
    return sections_.end();
  }

  /**
   * Whether empty.
   * 
   * @return true if empty; false otherwise.
   */
  bool Empty() const {
    return sections_.empty();
  }

  /**
   * Clear all sections.
   */
  void Clear() {
    sections_.clear();
  }

  /**
   * Read sections from file.
   * 
   * @param filepath        file path to read.
   * 
   * @return true if read success and set section values; false otherwise.
   */
  bool Read(const std::string& filepath);

  /**
   * Write sections to file.
   * 
   * @param filepath        file path to write.
   * 
   * @return true if write success; false otherwise.
   */
  bool Write(const std::string& filepath) const;

 private:
  std::unordered_map<std::string, std::shared_ptr<Section>> sections_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_CONFIGPARSER_H_
