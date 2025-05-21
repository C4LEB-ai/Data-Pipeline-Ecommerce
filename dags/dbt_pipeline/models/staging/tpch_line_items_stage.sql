-- -- A surrogate key in dbt (and in data modeling generally) is a unique identifier for a row in a model or table that is not derived from the source system’s natural key. It’s typically used to:
-- 	•	Uniquely identify a record when no reliable natural key exists.
-- 	•	Track slowly changing dimensions (SCDs).
-- 	•	Join across tables where composite keys are unwieldy.
-- 🔑 Example in dbt
-- In dbt, a surrogate key is often generated using a hash function like dbt_utils.surrogate_key or the built-in md5()/sha() functions.

select
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
	l_orderkey as order_key,
	l_partkey as part_key,
	l_linenumber as line_number,
	l_quantity as quantity,
	l_extendedprice as extended_price,
	l_discount as discount_percentage,
	l_tax as tax_rate
from
    {{ source('tpch', 'lineitem') }}