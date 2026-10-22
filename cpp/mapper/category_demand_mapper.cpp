// Hadoop Streaming mapper: one order-item record -> product-category demand.
//
// Output key/value:
//   <category>\t<order_id>\t<quantity>\t<sales>\t<status>
//
// The reducer computes order-item counts, distinct orders, quantity units and
// sales by category. There is no inventory-stock level in DataCo, so these are
// demand proxies, not stockout or inventory-risk measurements.

#include <algorithm>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "../common/csv.hpp"

namespace {

struct Schema {
    int category = -1;
    int order_id = -1;
    int quantity = -1;
    int sales = -1;
    int status = -1;

    bool valid() const {
        return category >= 0 && order_id >= 0 && quantity >= 0 && sales >= 0 &&
               status >= 0;
    }

    int max_column() const {
        return std::max({category, order_id, quantity, sales, status});
    }
};

Schema schema_from_header(const std::vector<std::string>& fields) {
    Schema schema;
    for (std::size_t i = 0; i < fields.size(); ++i) {
        const std::string name = bda::normalize_header(fields[i]);
        if (name == "categoryname") {
            schema.category = static_cast<int>(i);
        } else if (name == "orderid") {
            schema.order_id = static_cast<int>(i);
        } else if (name == "orderitemquantity") {
            schema.quantity = static_cast<int>(i);
        } else if (name == "sales") {
            schema.sales = static_cast<int>(i);
        } else if (name == "deliverystatus") {
            schema.status = static_cast<int>(i);
        }
    }
    return schema;
}

bool same_schema(const Schema& left, const Schema& right) {
    return left.category == right.category &&
           left.order_id == right.order_id &&
           left.quantity == right.quantity && left.sales == right.sales &&
           left.status == right.status;
}

const std::vector<std::string> KNOWN_HEADERS = {
    "categoryname", "orderid", "orderitemquantity", "sales",
    "deliverystatus"};

}  // namespace

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    Schema env_schema;
    const bool have_env_schema =
        bda::read_env_column("BDA_CATEGORY_COL", env_schema.category) &&
        bda::read_env_column("BDA_ORDER_ID_COL", env_schema.order_id) &&
        bda::read_env_column("BDA_QUANTITY_COL", env_schema.quantity) &&
        bda::read_env_column("BDA_SALES_COL", env_schema.sales) &&
        bda::read_env_column("BDA_STATUS_COL", env_schema.status);

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
                           "columns: Category Name, Order Id, Order Item "
                           "Quantity, Sales, Delivery Status\n";
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

        const std::string category =
            bda::sanitize_key(bda::trim(fields[schema.category]));
        const std::string order_id = bda::trim(fields[schema.order_id]);
        const std::string raw_status = bda::trim(fields[schema.status]);

        int quantity = 0;
        double sales = 0.0;
        if (category.empty() || order_id.empty() || raw_status.empty() ||
            !bda::parse_nonnegative_int(fields[schema.quantity], quantity) ||
            !bda::parse_double(fields[schema.sales], sales)) {
            ++malformed;
            continue;
        }

        const std::string status = bda::map_status(raw_status);
        if (status == "other") {
            ++unknown_status;
        }

        std::cout << category << '\t' << order_id << '\t' << quantity << '\t'
                  << std::setprecision(17) << sales << '\t' << status << '\n';
        ++emitted;
    }

    if (malformed != 0 || unknown_status != 0) {
        std::cerr << "category mapper summary: emitted=" << emitted
                  << " malformed=" << malformed
                  << " unknown_status=" << unknown_status << '\n';
    }

    return 0;
}
