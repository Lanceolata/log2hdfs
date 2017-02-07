// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_CONFIGPARSER_H_
#define LOG2HDFS_UTIL_CONFIGPARSER_H_

#include <string>
#include <memory>
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
typedef OptionMap::iterator OptionIter;
typedef OptionMap::const_iterator OptionConstIter;

class Section {
 public:
  static SectionPtr Create();
  static SectionPtr Create(const std::string &separator);

  virtual ~Section() {}

  virtual const std::string &separator() const = 0;
  virtual bool set_separator(const std::string &separator) = 0;

  virtual bool Read(const std::string &filepath) = 0;

  virtual bool Has(const std::string &option) const = 0;
  virtual bool Remove(const std::string &option) = 0;
  virtual const std::string *Get(const std::string &option) const = 0;
  virtual bool Set(const std::string &option, const std::string &value) = 0;

  virtual OptionIter Begin() = 0;
  virtual OptionConstIter Begin() const = 0;

  virtual OptionIter End() = 0;
  virtual OptionConstIter End() const = 0;

  virtual bool Options(OptionIter *begin, OptionIter *end) = 0;
  virtual bool Options(OptionConstIter *begin, OptionConstIter *end) const = 0;

  virtual bool Write(const std::string &filepath) const = 0;
  virtual bool Write(const std::string &filepath,
                     const std::string &separator) const = 0;

  virtual bool Empty() const = 0;
  virtual SectionPtr Copy() const = 0;
  virtual void Clear() = 0;

  virtual bool Write(std::ofstream &ofs) const = 0;
  virtual bool Write(std::ofstream &ofs,
                     const std::string &separator) const = 0;
};

// ------------------------------------------------------------------
// KvConfigParser

typedef Section KvConfigParser;
typedef SectionPtr KvConfigPtr;

// ------------------------------------------------------------------
// IniConfigParser

class IniConfigParser;

typedef std::shared_ptr<IniConfigParser> IniConfigPtr;
typedef std::unordered_map<std::string, SectionPtr> SectionMap;
typedef SectionMap::iterator SectionIter;
typedef SectionMap::const_iterator SectionConstIter;

class IniConfigParser {
 public:
  static IniConfigPtr Create();
  static IniConfigPtr Create(const std::string &separator);

  virtual ~IniConfigParser() {}

  virtual const std::string &separator() const = 0;
  virtual bool set_separator(const std::string &separator) = 0;

  virtual bool Read(const std::string &filepath) = 0;

  virtual bool HasSection(const std::string &section) const = 0;
  virtual bool AddSection(const std::string &section) = 0;
  virtual bool RemoveSection(const std::string &section) = 0;
  virtual SectionPtr GetSection(const std::string &section) = 0;

  virtual bool HasOption(const std::string &section,
                         const std::string &option) const = 0;
  virtual bool RemoveOption(const std::string &section,
                            const std::string &option) = 0;

  virtual const std::string *Get(const std::string &section,
                                 const std::string &option) const = 0;
  virtual bool Set(const std::string &section, const std::string &option,
                   const std::string &value) = 0;

  virtual SectionIter Begin() = 0;
  virtual SectionConstIter Begin() const = 0;

  virtual SectionIter End() = 0;
  virtual SectionConstIter End() const = 0;

  virtual bool Sections(SectionIter *begin, SectionIter *end) = 0;
  virtual bool Sections(SectionConstIter *begin,
                        SectionConstIter *end) const = 0;

  virtual bool Options(const std::string &section, OptionIter *begin,
                       OptionIter *end) = 0;
  virtual bool Options(const std::string &section, OptionConstIter *begin,
                       OptionConstIter *end) const = 0;

  virtual bool Empty() const = 0;
  virtual IniConfigPtr Copy() const = 0;
  virtual void Clear() = 0;

  virtual bool Write(const std::string &filepath) const = 0;
  virtual bool Write(const std::string &filepath,
                     const std::string &separator) const = 0;
};


}   // namespace util

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_CONFIGPARSER_H_
