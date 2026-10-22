// Shared CSV and parsing helpers for the C++ Hadoop Streaming jobs.
//
// The DataCo source is ISO-8859-1 with CRLF line endings. This parser is
// RFC-4180 style: it handles quoted commas, escaped quotes, and quoted
// newlines. It is header-aware but can also use header-derived column
// positions passed by the Streaming submit scripts through -cmdenv.

#pragma once

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace bda {

inline std::string trim(const std::string& value) {
    std::size_t begin = 0;
    std::size_t end = value.size();
    while (begin < end &&
           std::isspace(static_cast<unsigned char>(value[begin]))) {
        ++begin;
    }
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return value.substr(begin, end - begin);
}

inline std::string lower_ascii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) {
                       return static_cast<char>(std::tolower(c));
                   });
    return value;
}

inline std::string normalize_header(const std::string& value) {
    std::string result;
    result.reserve(value.size());
    for (unsigned char c : value) {
        if (std::isalnum(c)) {
            result.push_back(static_cast<char>(std::tolower(c)));
        }
    }
    return result;
}

// Keys are written to Hadoop as text. Replace separators so a malformed input
// field cannot corrupt the mapper's tab-separated contract.
inline std::string sanitize_key(std::string value) {
    for (char& c : value) {
        if (c == '\t' || c == '\n' || c == '\r') {
            c = ' ';
        }
    }
    return trim(value);
}

// Reads one logical CSV record, joining physical lines when a quoted field
// contains a newline. Trailing CR from CRLF input is removed.
inline bool read_logical_record(std::istream& input, std::string& record) {
    record.clear();
    std::string line;
    bool got_any = false;
    bool in_quotes = false;

    while (std::getline(input, line)) {
        got_any = true;
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!record.empty()) {
            record.push_back('\n');
        }
        record.append(line);

        // Track quote parity across this physical line. Doubled quotes inside
        // a quoted field are escapes and do not change the parity.
        for (std::size_t i = 0; i < line.size(); ++i) {
            if (line[i] != '"') {
                continue;
            }
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                ++i;
            } else {
                in_quotes = !in_quotes;
            }
        }

        if (!in_quotes) {
            return true;
        }
    }

    // A final record without a trailing newline is still a record. An
    // unterminated quoted field is treated as malformed input by the caller.
    return got_any;
}

inline std::vector<std::string> parse_csv_record(const std::string& record) {
    std::vector<std::string> fields;
    std::string field;
    bool in_quotes = false;
    bool at_field_start = true;

    for (std::size_t i = 0; i < record.size(); ++i) {
        const char c = record[i];

        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < record.size() && record[i + 1] == '"') {
                    field.push_back('"');
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                field.push_back(c);
            }
            continue;
        }

        if (c == '"' && at_field_start) {
            in_quotes = true;
            at_field_start = false;
        } else if (c == ',') {
            fields.push_back(field);
            field.clear();
            at_field_start = true;
        } else {
            field.push_back(c);
            at_field_start = false;
        }
    }

    fields.push_back(field);
    return fields;
}

inline void strip_utf8_bom(std::vector<std::string>& fields) {
    if (fields.empty()) {
        return;
    }
    std::string& first = fields.front();
    if (first.size() >= 3 &&
        static_cast<unsigned char>(first[0]) == 0xEF &&
        static_cast<unsigned char>(first[1]) == 0xBB &&
        static_cast<unsigned char>(first[2]) == 0xBF) {
        first.erase(0, 3);
    }
}

inline bool looks_like_header(const std::vector<std::string>& fields,
                              const std::vector<std::string>& known_names,
                              int minimum_matches = 2) {
    int matches = 0;
    for (const std::string& field : fields) {
        const std::string name = normalize_header(field);
        if (std::find(known_names.begin(), known_names.end(), name) !=
            known_names.end()) {
            ++matches;
        }
    }
    return matches >= minimum_matches;
}

inline bool read_env_column(const char* name, int& column) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    char* end = nullptr;
    const long value = std::strtol(raw, &end, 10);
    if (end == raw || *end != '\0' || value < 0 || value > 1000000) {
        return false;
    }
    column = static_cast<int>(value);
    return true;
}

inline std::string map_status(const std::string& raw) {
    const std::string value = lower_ascii(trim(raw));

    if (value == "late delivery" || value == "late") {
        return "late";
    }
    if (value == "shipping on time" || value == "on time" ||
        value == "on_time" || value == "advance shipping" ||
        value == "advance") {
        return "on_time";
    }
    if (value == "shipping canceled" || value == "shipping cancelled" ||
        value == "canceled" || value == "cancelled") {
        return "canceled";
    }

    if (value.find("cancel") != std::string::npos) {
        return "canceled";
    }
    if (value.find("late") != std::string::npos) {
        return "late";
    }
    if (value.find("advance") != std::string::npos ||
        value.find("on time") != std::string::npos) {
        return "on_time";
    }
    return "other";
}

inline bool parse_double(const std::string& raw, double& output) {
    const std::string value = trim(raw);
    if (value.empty()) {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const double parsed = std::strtod(value.c_str(), &end);
    if (end == value.c_str() || *end != '\0' || errno == ERANGE ||
        !std::isfinite(parsed)) {
        return false;
    }
    output = parsed;
    return true;
}

inline bool parse_nonnegative_int(const std::string& raw, int& output) {
    const std::string value = trim(raw);
    if (value.empty()) {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const long parsed = std::strtol(value.c_str(), &end, 10);
    if (end == value.c_str() || *end != '\0' || errno == ERANGE ||
        parsed < 0 || parsed > 1000000000L) {
        return false;
    }
    output = static_cast<int>(parsed);
    return true;
}

inline std::vector<std::string> split_tab_fields(const std::string& value) {
    std::vector<std::string> fields;
    std::size_t start = 0;
    while (true) {
        const std::size_t tab = value.find('\t', start);
        if (tab == std::string::npos) {
            fields.push_back(value.substr(start));
            break;
        }
        fields.push_back(value.substr(start, tab - start));
        start = tab + 1;
    }
    return fields;
}

}  // namespace bda
