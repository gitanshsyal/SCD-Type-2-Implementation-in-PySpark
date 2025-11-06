# SCD-Type-2-Implementation-in-PySpark
## Requirement

Maintain historical changes for customer records when updates arrive in a separate customer_updates dataset.

## Approach

Performed outer join between customer (target) and customer_updates (source)

Filtered rows where values changed and customer_id matched → change_df

Created:

old_change_df → updated with:

valid_to = current_date
is_current = 0


new_data_df → inserted with:

valid_to = null
is_current = 1


Used unionByName to merge final records into the customer dimension table
## Tech Stack

PySpark, Delta Lake concepts, Data Lakes

 ## Outcome

Successfully enabled SCD2 history tracking for customer data with accurate versioning and traceability across updates.
