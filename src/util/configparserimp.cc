// Copyright (c) 2017 Lanceolata

// #include "util/configparserimp.h"
#include "configparserimp.h"
// #include "util/tools.h"
#include "tools.h"

namespace log2hdfs {

namespace util {

// ------------------------------------------------------------------
// Parser

bool Parser::Load(const std::string &filepath,
                  std::vector<std::string> *lines) {
  // Try to open file
  std::ifstream ifs(filepath);
  if (!ifs.is_open()) {
    return false;
  }

  // Read each line
  (*lines).clear();
  std::string line;
  while (std::getline(ifs, line)) {
    RemoveComments(&line);
    Trim(&line);
    if (line.empty()) {
      continue;
    }
    (*lines).push_back(line);
  }
  ifs.close();

  return true;
}

bool Parser::Parse(const std::string &line, std::string *key,
                   std::string *value) const {
  std::size_t equal = line.find(separator_);

  if (equal == std::string::npos) {
    return false;
  }

  *key = line.substr(0, equal);
  *value = line.substr(equal + 1);

  Trim(key);
  Trim(value);

  return true;
}

// ------------------------------------------------------------------
// Section

SectionPtr Section::Create() {
  return SectionImp::Init();
}

SectionPtr Section::Create(const std::string &separator) {
  return SectionImp::Init(separator);
}

// ------------------------------------------------------------------
// SectionImp

bool SectionImp::Read(const std::string &filepath) {
  std::vector<std::string> lines;
  if (!Parser::Load(filepath, &lines)) {
    return false;
  }
  Parse(&lines);
  return true;
}

void SectionImp::Parse(std::vector<std::string> *lines) {
  Parser parser(separator_);
  std::string key, value;

  for (std::string& line : (*lines)) {
    if (parser.Parse(line, &key, &value)) {
      options_[key] = value;
    }
  }
}

bool SectionImp::Write(const std::string &filepath) const {
  return Write(filepath, separator_);
}

bool SectionImp::Write(const std::string &filepath,
                       const std::string &separator) const {
  if (filepath.empty()) {
    return false;
  }

  std::ofstream ofs(filepath, std::ofstream::out | std::ofstream::trunc);
  bool res = Write(ofs, separator);
  if (ofs.is_open()) {
    ofs.close();
  }
  return res;
}

bool SectionImp::Write(std::ofstream &ofs) const {
  return Write(ofs, separator_);
}

bool SectionImp::Write(std::ofstream &ofs,
                       const std::string &separator) const {
  if (!ofs.is_open()) {
    return false;
  }

  OptionConstIter it;
  for (it = options_.begin(); it != options_.end(); it++) {
    ofs << it->first << " " << separator << " "
        << it->second << std::endl;
  }
  return true;
}

// ------------------------------------------------------------------
// IniConfigParser

IniConfigPtr IniConfigParser::Create() {
  return IniConfigParserImp::Init();
}

IniConfigPtr IniConfigParser::Create(const std::string &separator) {
  return IniConfigParserImp::Init(separator);
}

// ------------------------------------------------------------------
// IniConfigParserImp

bool IniConfigParserImp::Read(const std::string &filepath) {
  std::vector<std::string> lines;
  if (!Parser::Load(filepath, &lines)) {
    return false;
  }

  Parse(&lines);
  return true;
}

void IniConfigParserImp::Parse(std::vector<std::string> *lines) {
  Parser parser(separator_);

  std::string section, key, value;
  SectionPtr sp;

  for (std::string& line : (*lines)) {
    if (parser.IsSection(line)) {
      section = parser.GetSection(line);
      sp = Section::Create(separator_);
      sections_[section] = sp;
    } else {
      if (!sp) {
        sp = Section::Create(separator_);
        sections_[section] = sp;
      }

      if (parser.Parse(line, &key, &value)) {
        sp->Set(key, value);
      }
    }
  }
}

bool IniConfigParserImp::Write(const std::string &filepath) const {
  return Write(filepath, separator_);
}

bool IniConfigParserImp::Write(const std::string &filepath,
                               const std::string &separator) const {
  if (filepath.empty()) {
    return false;
  }

  std::ofstream ofs(filepath, std::ofstream::out | std::ofstream::trunc);
  if (!ofs.is_open()) {
    return false;
  }

  SectionConstIter sci = sections_.find("");
  if (sci != sections_.end()) {
    if (!(sci->second)->Write(ofs, separator)) {
      ofs.close();
      return false;
    }
    ofs << std::endl;
  }

  for (sci = sections_.begin(); sci != sections_.end(); sci++) {
    if ((sci->first).empty()) {
      continue;
    }
    ofs << constants::SECTION_START << sci->first
        << constants::SECTION_END << std::endl;
    if (!(sci->second)->Write(ofs, separator)) {
      ofs.close();
      return false;
    }
    ofs << std::endl;
  }

  ofs.close();
  return false;
}

}   // namespace util

}   // namespace log2hdfs
