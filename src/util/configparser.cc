// Copyright (c) 2017 Lanceolata

#include "util/configparser.h"
#include "util/string_utils.h"

namespace log2hdfs {

namespace {

const char SECTION_START = '[';
const char SECTION_END = ']';
const char DECIMAL = '.';
const char COMMA = ',';
const char SPACE = ' ';

static Optional<std::vector<std::string> > ReadLinesFromFile(
    const std::string &filepath) {
  // Try to open file
  std::ifstream ifs(filepath);
  if (!ifs.is_open()) {
    return Optional<std::vector<std::string> >::Invalid();
  }

  // Read each line
  std::vector<std::string> vec;
  std::string line;
  while (std::getline(ifs, line)) {
    line = TrimString(RemoveComments(line));
    if (line.empty()) {
      continue;
    }
    vec.push_back(line);
  }
  ifs.close();
  return Optional<std::vector<std::string> >(vec);
}

static bool IsSection(const std::string &line) {
  if (line.length() < 3 || line[0] != SECTION_START
          || line[line.length() - 1] != SECTION_END) {
    return false;
  }
  return true;
}

static std::string ExtractSectionName(const std::string &line) {
  std::string section = line.substr(1, line.length() - 2);
  return section;
}

static bool ParseLine(const std::string &line, const std::string &delimiters,
    std::string *key, std::string *value) {
  std::size_t equal = line.find(delimiters);

  if (equal == std::string::npos) {
    return false;
  }

  *key = TrimString(line.substr(0, equal));
  *value = TrimString(line.substr(equal + 1));
  return true;
}

}   // namespace

// ------------------------------------------------------------------
// Section

bool Section::Read(const std::string &filepath) {
  Optional<std::vector<std::string> > lines = ReadLinesFromFile(filepath);
  if (!lines.valid()) {
    return false;
  }

  std::string key, value;
  for (const std::string &line : lines.value()) {
    if (ParseLine(line, delimiters_, &key, &value)) {
      options_[key] = value;
    }
  }
  return true;
}

bool Section::Write(const std::string &filepath) const {
  return Write(filepath, delimiters_);
}

bool Section::Write(const std::string &filepath,
                    const std::string &delimiters) const {
  if (filepath.empty()) {
    return false;
  }

  std::ofstream ofs(filepath, std::ofstream::out | std::ofstream::trunc);
  bool res = Write(ofs, delimiters);
  if (ofs.is_open()) {
    ofs.close();
  }
  return res;
}

bool Section::Write(std::ofstream &ofs) const {
  return Write(ofs, delimiters_);
}

bool Section::Write(std::ofstream &ofs,
                    const std::string &delimiters) const {
  if (!ofs.is_open()) {
    return false;
  }

  Section::const_iterator it;
  for (it = options_.begin(); it != options_.end(); ++it) {
    ofs << it->first << " " << delimiters << " "
        << it->second << std::endl;
  }
  return true;
}

// ------------------------------------------------------------------
// IniConfigParser

bool IniConfigParser::Read(const std::string &filepath) {
  Optional<std::vector<std::string> > lines = ReadLinesFromFile(filepath);
  if (!lines.valid()) {
    return false;
  }

  std::string section, key, value;
  std::shared_ptr<Section> sptr;
  for (const std::string &line : lines.value()) {
    if (IsSection(line)) {
      section = ExtractSectionName(line);
      sptr = Section::Init(delimiters_);
      sections_[section] = sptr;
    } else {
      if (!sptr || !sptr.get()) {
        sptr = Section::Init(delimiters_);
        sections_[section] = sptr;
      }
      if (ParseLine(line, delimiters_, &key, &value)) {
        sptr->Set(key, value);
      }
    }
  }
  return true;
}

bool IniConfigParser::Write(const std::string &filepath) const {
  return Write(filepath, delimiters_);
}

bool IniConfigParser::Write(const std::string &filepath,
                            const std::string &delimiters) const {
  if (filepath.empty()) {
    return false;
  }

  std::ofstream ofs(filepath, std::ofstream::out | std::ofstream::trunc);
  if (!ofs.is_open()) {
    return false;
  }

  IniConfigParser::const_iterator it = sections_.find("");
  if (it != sections_.end()) {
    if (!(it->second)->Write(ofs, delimiters)) {
      ofs.close();
      return false;
    }
    ofs << std::endl;
  }

  for (it = sections_.begin(); it != sections_.end(); ++it) {
    if ((it->first).empty()) {
      continue;
    }
    ofs << SECTION_START << it->first << SECTION_END << std::endl;
    if (!(it->second)->Write(ofs, delimiters)) {
      ofs.close();
      return false;
    }
    ofs << std::endl;
  }
  ofs.close();
  return true;
}

}   // namespace log2hdfs
