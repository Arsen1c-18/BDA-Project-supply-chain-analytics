// Hadoop Streaming reducer: counts distinct order ids per
// "<shipping_mode>|<status>" key.
//
// Input : "<key>\t<order_id>" lines sorted by Hadoop.
// Output: "<shipping_mode>\t<status>\t<distinct_order_count>"
//
// Because all rows of one order carry the same shipping mode and status in the
// source data, keeping an unordered_set per key removes order-item duplicates.

#include <cstddef>
#include <iostream>
#include <string>
#include <unordered_set>

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    std::string line;
    std::string current_key;
    std::unordered_set<std::string> order_ids;
    order_ids.reserve(4096);

    bool have_key = false;
    std::size_t groups = 0;
    std::size_t records = 0;

    auto emit_group = [&]() {
        if (!have_key) {
            return;
        }
        const std::size_t separator = current_key.find('|');
        if (separator == std::string::npos) {
            return;
        }
        const std::string shipping_mode = current_key.substr(0, separator);
        const std::string status = current_key.substr(separator + 1);
        std::cout << shipping_mode << '\t' << status << '\t'
                  << order_ids.size() << '\n';
        ++groups;
    };

    while (std::getline(std::cin, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        const std::size_t tab = line.find('\t');
        if (tab == std::string::npos) {
            continue;
        }

        const std::string key = line.substr(0, tab);
        const std::string order_id = line.substr(tab + 1);

        if (!have_key || key != current_key) {
            emit_group();
            current_key = key;
            order_ids.clear();
            have_key = true;
        }

        if (!order_id.empty()) {
            order_ids.insert(order_id);
        }
        ++records;
    }

    emit_group();

    std::cerr << "reducer summary: groups=" << groups
              << " input_records=" << records << '\n';
    return 0;
}
