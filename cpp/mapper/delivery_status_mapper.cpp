// Hadoop Streaming mapper: one record per order item -> (shipping_mode|status, order_id).
//
// Input : DataCoSupplyChainDataset.csv (ISO-8859-1, CRLF, header row).
// Output: "<shipping_mode>|<status>\t<order_id>"
//
// The reducer counts distinct order ids, so repeated order-item rows for the same
// order are counted once. The CSV parser is RFC-4180 style (quoted commas,
// escaped quotes, CRLF and quoted newlines) rather than a naive comma split.

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

struct Schema {
    int order_id = -1;
    int status = -1;
    int shipping_mode = -1;

    bool valid() const {
        return order_id >= 0 && status >= 0 && shipping_mode >= 0;
    }
};

std::string trim(const std::string& value) {
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

std::string lower_ascii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) {
                       return static_cast<char>(std::tolower(c));
                   });
    return value;
}

std::string normalize_header(const std::string& value) {
    std::string result;
    result.reserve(value.size());
    for (unsigned char c : value) {
        if (std::isalnum(c)) {
            result.push_back(static_cast<char>(std::tolower(c)));
        }
    }
    return result;
}

// Reads one logical CSV record, joining physical lines when a quoted field
// contains a newline. Trailing CR from CRLF input is removed.
bool read_logical_record(std::istream& input, std::string& record) {
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

        // Track quote parity across this physical line. Doubled quotes inside a
        // quoted field are escapes and do not change the parity.
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

std::vector<std::string> parse_csv_record(const std::string& record) {
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

void strip_utf8_bom(std::vector<std::string>& fields) {
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

// A header is recognised only on the first record of a split. Requiring at
// least two known column names avoids treating a normal data row as a header.
bool looks_like_header(const std::vector<std::string>& fields) {
    int matches = 0;
    for (const std::string& field : fields) {
        const std::string name = normalize_header(field);
        if (name == "orderid" || name == "deliverystatus" ||
            name == "shippingmode") {
            ++matches;
        }
    }
    return matches >= 2;
}

Schema schema_from_header(const std::vector<std::string>& fields) {
    Schema schema;
    for (std::size_t i = 0; i < fields.size(); ++i) {
        const std::string name = normalize_header(fields[i]);
        if (name == "orderid") {
            schema.order_id = static_cast<int>(i);
        } else if (name == "deliverystatus") {
            schema.status = static_cast<int>(i);
        } else if (name == "shippingmode") {
            schema.shipping_mode = static_cast<int>(i);
        }
    }
    return schema;
}

bool read_env_column(const char* name, int& column) {
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

std::string map_status(const std::string& raw) {
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

}  // namespace

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    // In a multi-split Hadoop job only the split that starts at offset 0 sees the
    // header. The submit script passes the header-derived column positions via
    // -cmdenv so every mapper uses the same schema.
    Schema env_schema;
    const bool have_env_schema =
        read_env_column("BDA_ORDER_ID_COL", env_schema.order_id) &&
        read_env_column("BDA_STATUS_COL", env_schema.status) &&
        read_env_column("BDA_SHIPPING_MODE_COL", env_schema.shipping_mode);

    Schema schema = env_schema;
    bool schema_ready = have_env_schema;
    bool first_record = true;

    std::size_t emitted = 0;
    std::size_t malformed = 0;
    std::size_t unknown_status = 0;

    std::string record;
    while (read_logical_record(std::cin, record)) {
        std::vector<std::string> fields = parse_csv_record(record);
        if (first_record) {
            strip_utf8_bom(fields);
        }

        // Ignore blank lines without consuming the "first record" slot.
        if (fields.size() == 1 && trim(fields[0]).empty()) {
            continue;
        }

        if (first_record) {
            first_record = false;

            if (looks_like_header(fields)) {
                const Schema header_schema = schema_from_header(fields);
                if (!header_schema.valid()) {
                    std::cerr
                        << "ERROR: CSV header is missing one of the required "
                           "columns: Order Id, Delivery Status, Shipping Mode\n";
                    return 2;
                }
                if (have_env_schema &&
                    (header_schema.order_id != env_schema.order_id ||
                     header_schema.status != env_schema.status ||
                     header_schema.shipping_mode != env_schema.shipping_mode)) {
                    std::cerr
                        << "ERROR: BDA_*_COL environment variables disagree "
                           "with the CSV header\n";
                    return 2;
                }
                schema = header_schema;
                schema_ready = true;
                continue;
            }

            if (!schema_ready) {
                std::cerr
                    << "ERROR: no CSV header in this input split and no "
                       "BDA_*_COL environment variables were supplied; "
                       "refusing to guess the column positions\n";
                return 2;
            }
        }

        if (!schema_ready) {
            std::cerr << "ERROR: internal schema state error\n";
            return 2;
        }

        const int max_column =
            std::max(schema.order_id, std::max(schema.status, schema.shipping_mode));
        if (static_cast<int>(fields.size()) <= max_column) {
            ++malformed;
            continue;
        }

        const std::string order_id = trim(fields[schema.order_id]);
        const std::string shipping_mode = trim(fields[schema.shipping_mode]);
        const std::string raw_status = trim(fields[schema.status]);

        if (order_id.empty() || shipping_mode.empty() || raw_status.empty()) {
            ++malformed;
            continue;
        }

        const std::string status = map_status(raw_status);
        if (status == "other") {
            ++unknown_status;
        }

        std::cout << shipping_mode << '|' << status << '\t' << order_id
                  << '\n';
        ++emitted;
    }

    if (malformed != 0 || unknown_status != 0) {
        std::cerr << "mapper summary: emitted=" << emitted
                  << " malformed=" << malformed
                  << " unknown_status=" << unknown_status << '\n';
    }

    return 0;
}
