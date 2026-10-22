// Hadoop Streaming mapper: one order-item record -> shipping-mode performance.
//
// Output key/value:
//   <shipping_mode>\t<order_id>\t<actual_days>\t<scheduled_days>\t<status>\t<sales>
//
// The reducer aggregates record counts and distinct orders per shipping mode.
// There is no shipping-cost column in DataCo; the sales field is retained only
// as a clearly labelled order-value proxy.

#include <algorithm>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "../common/csv.hpp"

namespace {

struct Schema {
    int order_id = -1;
    int shipping_mode = -1;
    int status = -1;
    int actual_days = -1;
    int scheduled_days = -1;
    int sales = -1;

    bool valid() const {
        return order_id >= 0 && shipping_mode >= 0 && status >= 0 &&
               actual_days >= 0 && scheduled_days >= 0 && sales >= 0;
    }

    int max_column() const {
        return std::max(
            {order_id, shipping_mode, status, actual_days, scheduled_days, sales});
    }
};

Schema schema_from_header(const std::vector<std::string>& fields) {
    Schema schema;
    for (std::size_t i = 0; i < fields.size(); ++i) {
        const std::string name = bda::normalize_header(fields[i]);
        if (name == "orderid") {
            schema.order_id = static_cast<int>(i);
        } else if (name == "shippingmode") {
            schema.shipping_mode = static_cast<int>(i);
        } else if (name == "deliverystatus") {
            schema.status = static_cast<int>(i);
        } else if (name == "daysforshippingreal") {
            schema.actual_days = static_cast<int>(i);
        } else if (name == "daysforshipmentscheduled") {
            schema.scheduled_days = static_cast<int>(i);
        } else if (name == "sales") {
            schema.sales = static_cast<int>(i);
        }
    }
    return schema;
}

bool same_schema(const Schema& left, const Schema& right) {
    return left.order_id == right.order_id &&
           left.shipping_mode == right.shipping_mode &&
           left.status == right.status &&
           left.actual_days == right.actual_days &&
           left.scheduled_days == right.scheduled_days &&
           left.sales == right.sales;
}

const std::vector<std::string> KNOWN_HEADERS = {
    "orderid", "shippingmode", "deliverystatus", "daysforshippingreal",
    "daysforshipmentscheduled", "sales"};

}  // namespace

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    Schema env_schema;
    const bool have_env_schema =
        bda::read_env_column("BDA_ORDER_ID_COL", env_schema.order_id) &&
        bda::read_env_column("BDA_SHIPPING_MODE_COL", env_schema.shipping_mode) &&
        bda::read_env_column("BDA_STATUS_COL", env_schema.status) &&
        bda::read_env_column("BDA_ACTUAL_DAYS_COL", env_schema.actual_days) &&
        bda::read_env_column("BDA_SCHEDULED_DAYS_COL", env_schema.scheduled_days) &&
        bda::read_env_column("BDA_SALES_COL", env_schema.sales);

    Schema schema = env_schema;
    bool schema_ready = have_env_schema;
    bool first_record = true;

    std::size_t emitted = 0;
    std::size_t malformed = 0;
    std::size_t unknown_status = 0;

    std::string record;
    while (bda::read_logical_record(std::cin, record)) {
        std::vector<std::string> fields = bda::parse_csv_record(record);
        if (first_record) {
            bda::strip_utf8_bom(fields);
        }

        if (fields.size() == 1 && bda::trim(fields[0]).empty()) {
            continue;
        }

        if (first_record) {
            first_record = false;

            if (bda::looks_like_header(fields, KNOWN_HEADERS)) {
                const Schema header_schema = schema_from_header(fields);
                if (!header_schema.valid()) {
                    std::cerr
                        << "ERROR: CSV header is missing one of the required "
                           "columns: Order Id, Shipping Mode, Delivery Status, "
                           "Days for shipping (real), Days for shipment "
                           "(scheduled), Sales\n";
                    return 2;
                }
                if (have_env_schema && !same_schema(header_schema, env_schema)) {
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
                       "header-derived BDA_*_COL environment variables were "
                       "supplied; refusing to guess column positions\n";
                return 2;
            }
        }

        if (!schema_ready) {
            std::cerr << "ERROR: internal schema state error\n";
            return 2;
        }

        if (static_cast<int>(fields.size()) <= schema.max_column()) {
            ++malformed;
            continue;
        }

        const std::string order_id = bda::trim(fields[schema.order_id]);
        const std::string shipping_mode =
            bda::sanitize_key(bda::trim(fields[schema.shipping_mode]));
        const std::string raw_status = bda::trim(fields[schema.status]);

        int actual_days = 0;
        int scheduled_days = 0;
        double sales = 0.0;
        if (order_id.empty() || shipping_mode.empty() || raw_status.empty() ||
            !bda::parse_nonnegative_int(fields[schema.actual_days], actual_days) ||
            !bda::parse_nonnegative_int(fields[schema.scheduled_days],
                                       scheduled_days) ||
            !bda::parse_double(fields[schema.sales], sales)) {
            ++malformed;
            continue;
        }

        const std::string status = bda::map_status(raw_status);
        if (status == "other") {
            ++unknown_status;
        }

        std::cout << shipping_mode << '\t' << order_id << '\t' << actual_days
                  << '\t' << scheduled_days << '\t' << status << '\t'
                  << std::setprecision(17) << sales << '\n';
        ++emitted;
    }

    if (malformed != 0 || unknown_status != 0) {
        std::cerr << "shipping mapper summary: emitted=" << emitted
                  << " malformed=" << malformed
                  << " unknown_status=" << unknown_status << '\n';
    }

    return 0;
}
