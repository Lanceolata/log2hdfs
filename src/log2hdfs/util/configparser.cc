#include "configparser.h"
#include "tools.h"

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

  Parser(const std::string& separator): separator_(separator) {}

  static bool load(const std::string& filepath, 
          std::vector<std::string>& lines);

  bool is_section(const std::string& line) const {
    if (line.length() < 3 || line[0] != constants::SECTION_START 
            || line[line.length() - 1] != constants::SECTION_END) {
      return false;
    }
    return true;
  }

  std::string get_section(const std::string& line) const {
    std::string section = line.substr(1, line.length()-2);
    return section;
  }

  std::pair<std::string, std::string> parse(const std::string& line) const;

 private:

  std::string separator_;
};

bool Parser::load(const std::string& filepath, 
        std::vector<std::string>& lines) {
  // Try to open file
  std::ifstream ifs(filepath);
  if (!ifs.is_open()) {
    return false;
  }

  // Read each line
  lines.clear();
  std::string line;
  while (std::getline(ifs, line)) {
    remove_comments(line);
    trim(line);
    if (line.empty()) {
      continue;
    }
    lines.push_back(line);
  }
  ifs.close();

  return true;
}

std::pair<std::string, std::string> Parser::parse(
        const std::string& line) const {
  std::string key, value;
  std::size_t equal = line.find(separator_);

  if (equal != std::string::npos) {
    key = line.substr(0, equal);
    value = line.substr(equal + 1);

    trim(key);
    trim(value);
  }

  return std::make_pair(key, value);
}


// ------------------------------------------------------------------
// Section

bool Section::read(const std::string& filepath) {
  std::vector<std::string> lines;
  if (!Parser::load(filepath, lines)) {
    return false;
  }

  parse(lines);
  return true;
}

void Section::parse(std::vector<std::string>& lines) {
  Parser parser(separator_);
  for (std::string& line : lines) {
    std::pair<std::string, std::string> p = parser.parse(line);
    if (!p.first.empty() || !p.second.empty()) {
        options_[p.first] = p.second;
    }
  }
}

bool Section::write(const std::string& filepath) const {
  return write(filepath, separator_);
}

bool Section::write(const std::string& filepath, 
        const std::string& separator) const {
  if (filepath.empty()) {
    return false;
  }

  std::ofstream ofs(filepath, std::ofstream::out | std::ofstream::trunc);
  if (!ofs.is_open()) {
    return false;
  }

  OptionConstIter it;
  for (it = options_.begin(); it != options_.end(); it++) {
    ofs << it->first << " " << separator << " "
        << it->second << std::endl;
  }
  ofs.close();
  return true;
}

bool Section::write(std::ofstream& ofs) const {
  return write(ofs, separator_);
}

bool Section::write(std::ofstream& ofs, const std::string& separator) const {
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
// IniConfParser

bool IniConfigParser::read(const std::string& filepath) {
  std::vector<std::string> lines;
  if (!Parser::load(filepath, lines)) {
    return false;
  }

  parse(lines);
  return true;
}

void IniConfigParser::parse(std::vector<std::string>& lines) {
  Parser parser(separator_);

  std::string section;
  Section *sp = NULL;

  for (std::string& line : lines) {
    if (parser.is_section(line)) {
      section = parser.get_section(line);
      sections_[section] = Section();
      sp = &(sections_[section]);
    } else {
      if (!sp) {
        sections_[section] = Section();
        sp = &(sections_[section]);
      }
      std::pair<std::string, std::string> p = parser.parse(line);
      if (!p.first.empty() || !p.second.empty()) {
        sp->set(p.first, p.second);
      }
    }
  }
}

bool IniConfigParser::write(const std::string& filepath) const {
  return write(filepath, separator_);
}

bool IniConfigParser::write(const std::string& filepath, 
        const std::string& separator) const {
  if (filepath.empty()) {
    return false;
  }

  std::ofstream ofs(filepath, std::ofstream::out | std::ofstream::trunc);
  if (!ofs.is_open()) {
    return false;
  }

  SectionConstIter sci = sections_.find("");
  if (sci != sections_.end()) {
    if (!(sci->second).write(ofs, separator)) {
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
    if (!(sci->second).write(ofs, separator)) {
      return false;
    }
    ofs << std::endl;
  }

  ofs.close();
  return false;
}

}   // namespace util

}   // namespace log2hdfs
