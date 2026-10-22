// Hadoop Streaming reducer: shipping-mode performance.
//
// Input : <shipping_mode>\t<order_id>\t<actual_days>\t<scheduled_days>\t<status>\t<sales>
// Output: <shipping_mode>\t<records>\t<distinct_orders>\t<late_records>
//         \t<on_time_records>\t<canceled_records>\t<other_records>
//         \t<avg_actual_days>\t<avg_scheduled_days>\t<avg_delay_days>
//         \t<total_sales>\t<avg_sales_per_order>
//
// Record counts are order-item records. Distinct-order counts use an
// unordered_set per shipping mode. The sales columns are explicitly an
// order-value proxy because the dataset has no shipping-cost field.

#include <cstddef>
#include <cmath>
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
    std::size_t late_records = 0;
    std::size_t on_time_records = 0;
    std::size_t canceled_records = 0;
    std::size_t other_records = 0;
    double total_actual_days = 0.0;
    double total_scheduled_days = 0.0;
    double total_sales = 0.0;

    void reset(const std::string& new_key) {
        key = new_key;
        order_ids.clear();
        records = 0;
        late_records = 0;
        on_time_records = 0;
        canceled_records = 0;
        other_records = 0;
        total_actual_days = 0.0;
        total_scheduled_days = 0.0;
        total_sales = 0.0;
    }
};

void emit(const Accumulator& acc) {
    if (acc.key.empty()) {
        return;
    }

    const double average_actual =
        acc.records == 0 ? 0.0 : acc.total_actual_days / acc.records;
    const double average_scheduled =
        acc.records == 0 ? 0.0 : acc.total_scheduled_days / acc.records;
    const double average_delay = average_actual - average_scheduled;
    const double normalized_delay =
        std::abs(average_delay) < 0.005 ? 0.0 : average_delay;
    const double average_sales_per_order =
        acc.order_ids.empty() ? 0.0 : acc.total_sales / acc.order_ids.size();

    std::cout << acc.key << '\t' << acc.records << '\t' << acc.order_ids.size()
              << '\t' << acc.late_records << '\t' << acc.on_time_records << '\t'
              << acc.canceled_records << '\t' << acc.other_records << '\t'
              << std::fixed << std::setprecision(2) << average_actual << '\t'
              << average_scheduled << '\t' << normalized_delay << '\t'
              << acc.total_sales << '\t' << average_sales_per_order << '\n';
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
        if (values.size() != 5 || key.empty() || values[0].empty()) {
            ++bad_values;
            continue;
        }

        int actual_days = 0;
        int scheduled_days = 0;
        double sales = 0.0;
        if (!bda::parse_nonnegative_int(values[1], actual_days) ||
            !bda::parse_nonnegative_int(values[2], scheduled_days) ||
            !bda::parse_double(values[4], sales)) {
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
        if (values[3] == "late") {
            ++acc.late_records;
        } else if (values[3] == "on_time") {
            ++acc.on_time_records;
        } else if (values[3] == "canceled") {
            ++acc.canceled_records;
        } else {
            ++acc.other_records;
        }
        acc.total_actual_days += static_cast<double>(actual_days);
        acc.total_scheduled_days += static_cast<double>(scheduled_days);
        acc.total_sales += sales;
    }

    emit(acc);

    std::cerr << "shipping reducer summary: groups=" << groups
              << " bad_values=" << bad_values << '\n';
    return 0;
}
