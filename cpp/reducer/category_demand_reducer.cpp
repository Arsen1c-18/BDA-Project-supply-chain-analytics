// Hadoop Streaming reducer: product-category demand proxies.
//
// Input : <category>\t<order_id>\t<quantity>\t<sales>\t<status>
// Output: <category>\t<records>\t<distinct_orders>\t<total_quantity>
//         \t<units_per_order>\t<total_sales>\t<avg_sales_per_item>
//         \t<late_records>\t<late_rate_pct>
//
// Records are order-item rows. Distinct orders use an unordered_set. Quantity
// and sales are demand/revenue proxies; the dataset has no inventory-stock
// level, so this reducer does not measure shortages or inventory risk.

#include <cstddef>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include "../common/csv.hpp"

namespace {

struct Accumulator {
    std::string key;
    std::unordered_set<std::string> order_ids;
    std::size_t records = 0;
    std::size_t total_quantity = 0;
    std::size_t late_records = 0;
    double total_sales = 0.0;

    void reset(const std::string& new_key) {
        key = new_key;
        order_ids.clear();
        records = 0;
        total_quantity = 0;
        late_records = 0;
        total_sales = 0.0;
    }
};

void emit(const Accumulator& acc) {
    if (acc.key.empty()) {
        return;
    }

    const double units_per_order =
        acc.order_ids.empty()
            ? 0.0
            : static_cast<double>(acc.total_quantity) / acc.order_ids.size();
    const double average_sales_per_item =
        acc.records == 0 ? 0.0 : acc.total_sales / acc.records;
    const double late_rate =
        acc.records == 0
            ? 0.0
            : 100.0 * static_cast<double>(acc.late_records) / acc.records;

    std::cout << acc.key << '\t' << acc.records << '\t' << acc.order_ids.size()
              << '\t' << acc.total_quantity << '\t' << std::fixed
              << std::setprecision(2) << units_per_order << '\t'
              << acc.total_sales << '\t' << average_sales_per_item << '\t'
              << acc.late_records << '\t' << late_rate << '\n';
}

}  // namespace

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    Accumulator acc;
    bool have_key = false;
    std::size_t groups = 0;
    std::size_t bad_values = 0;

    std::string line;
    while (std::getline(std::cin, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        const std::size_t tab = line.find('\t');
        if (tab == std::string::npos) {
            ++bad_values;
            continue;
        }

        const std::string key = line.substr(0, tab);
        const std::vector<std::string> values =
            bda::split_tab_fields(line.substr(tab + 1));
        if (values.size() != 4 || key.empty() || values[0].empty()) {
            ++bad_values;
            continue;
        }

        int quantity = 0;
        double sales = 0.0;
        if (!bda::parse_nonnegative_int(values[1], quantity) ||
            !bda::parse_double(values[2], sales)) {
            ++bad_values;
            continue;
        }

        if (!have_key || key != acc.key) {
            emit(acc);
            acc.reset(key);
            have_key = true;
            ++groups;
        }

        acc.order_ids.insert(values[0]);
        ++acc.records;
        acc.total_quantity += static_cast<std::size_t>(quantity);
        acc.total_sales += sales;
        if (values[3] == "late") {
            ++acc.late_records;
        }
    }

    emit(acc);

    std::cerr << "category reducer summary: groups=" << groups
              << " bad_values=" << bad_values << '\n';
    return 0;
}
