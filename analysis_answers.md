# Data Analysis

## Commercial Insights
Note queries evidencing these claims are referenced where appropriate.

### 1. Repeat purchase rate is very low
Of approx. 94,000 customers who placed a fulfilled order, only 2.13% order again, and only 1.27% do so within 90 days (Q6b).
A couple of caveats on how these figures were derived - taking our data date range of Jan 2017 - Aug 2018, customers who ordered
near the end of this date range can't be accounted for in repeat purchase measures, as they haven't had time to return,
whereas those who first purchased in Jan 2017 had 18 months to return.
Using a 90-day return window helps remove this distortion, with the first-purchase cutoff being May 2018.

Additionally, nearly a third of repeat purchases are same-day repeats - essentially one shopping session counted as 2 orders.
869 customers had their second order on the same day as their first order (Q6a).

Customer satisfaction is not the driver of this lack of repeat purchases. From (Q6c), we can see that of customers who left
a 5-star rating on their first order, only 1.42% returned within 90 days. While 5-star customer do have a higher 90 day return rate
than customers who gave lower ratings, the difference is minor.

The instinctive response to such a low repeat purchase rate is to fix the customer experience. However, in this case customers aren't
dissatisfied with their experience, which leads me to assume there is no post-purchase contact with customers, and no incentive for them
to return.

### 2. Customer who do return, return quickly
Among genuine returners (i.e. not same-day returners), the median gap between purchases is 26 days, and monthly cohorts sit roughly
between 20-45 days (Q5). Monthly repeat purchase rates remain reasonably steady across the period, suggesting the business is not losing
ground, it just never had a retention mechanism in the first place.

This sets the window for implementing a potential campaign to improve customer retention. Retention effort belongs in the weeks after purchase,
and results should be visible within months rather than years.

A caveat here - a number of customers do return outside the 90 day window, so a 90 day measurement does understate the number of returning customers.
However, as mentioned in the previous insight, the overall number of repeat customers was just 2.13%, so improving the 90 day return rate will go 
a long way to improving the overall return rate.

### 3. Freight prices low-value demand out of certain states
Order value tends to be higher in the worst-served states (Q4, Q7). These worst-served states also tend to have higher freight costs, but even removing freight from 
order value, these states still have the highest revenue per customer.
There are a couple of potential explanations for this - either customers in these states are buying more items, or buying more expensive items.
Basket size is flat everywhere (Q7), so the difference is driven by the price of what people buy.

|  | São Paulo | Paraíba |
| --- | --- | --- |
| Items per order | 1.15 | 1.13 |
| Average item price | R$114.53 | R$202.09 |
| Freight per item | R$15.30 | R$41.71 |
| Freight as share of order value | 12.2% | 18.3% |

An explanation for this is that freight acts as a price filter. When shipping makes up a large fraction of a purchase cost, a customer is less likely to make the purchase.
This means only higher-value orders get placed, and customers may be priced out of making smaller purchases.

A caveat here is that correlation may not necessarily be causation in this case. There may be other factors impacting these states - distance from sellers, delivery times, different socioeconomic conditions, are all things that could be leading to lower purchase rates.

## Recommendation: implementing a customer retention program
### Target segment
Every new customer, particularly prioritising those who spent above the median. In 2018, there were around 6500 new customers per month, on average (Q8c).
The pilot group could be the customers who made a purchase in the last 90 days, and spent above the median - 9046 customers in total, with an average first order
of $260 (Q8a). A further 37,000 above-median one-time buyers are available to target at launch. These customers are identifiable from the customer dimension table.

### Mechanic

Customers would be contacted 21 - 30 days after their order arrives. This number is based on the 26 median figures from insight 2, and would aim to contact customers
just before the point where they tend to naturally return. This period would be post-deliver rather than post-order being made, as long delivery times tend to negatively
impact customer satisfaction (Q3), and it would be unlikely for a customer to return if they are yet to receive their first order.

If we say 21 days post-delivery is the first point of contact - on day 21, a category-relevant recommendation is sent to the customer based on their most recent purchase,
with some sort of promotional offer. Non-responders could receive a follow up a couple of weeks later (say 45 days post-delivery) featuring a different category or popular product.

For the promotional offer, I would recommend free freight (above a basket threshold) rather than an X% discount. Freight is worth $15 -$42 depending on region, and is worth most exactly where insight 3 says demand is suppressed. Offering an X% discount might also lead customers to just wait for markdowns rather than purchasing at full price.

### Expected commercial impact
As there is currently no retention mechanism, this is a test of whether one works at all, rather than an optimisation of an existing one.

Assuming the following:
  - average value of a second order is $146 (Q8b)
  - 78,000 new customers per year (based on ~6500 per month from Q8c)
  - 90-day repeat purchase rate increase to 10% (rough estimate)
  - average freight cost of roughly $25 given free to returning customers

If we can increase 90-day retention rate from 1.27% to 5%, that would lead to (5% - 1.27%) * 78,000 = approx 2,900 extra orders, leading to roughly $425,000 in revenue.
If we increased it to 10%, that would lead to (10% - 1.27%) * 78,000 = approx. 6,800 extra orders, leading to roughly $824,000 in revenue.

### Measurement
Perform a holdout test, where 50% of eligible customers receive the communications/discounts.

Look at the following metrics:
  - Primary: share of customers placing a second order within 90 days
  - Secondary: revenue per enrolled customer, second-order value, redemption rate

Compare these metrics between the control group and the group receiving the programme. Enrolling the existing backlog of one-time buyers at launch allows for a first read in 90 days, and in the shorter term, tracking on click-through rates would give some indication on whether the programme is having an effect.

## Assumptions, caveats, and data quality
### Assumptions
- Source data is static, so every stage overwrites. Each layer of the pipeline rebuilds on every run. Re-runs are therefore safe, and the same code produces identical results in dev and prod catalogs.
- Missing order timestamps are genunine NULL values, not missing data. E.g. a cancelled order has a NULL delivery date because it was never delivered.
- Revenue is item price + freight, on fulfilled orders only. This was prefered over 'payments' which only existed at order level, not order item level, so couldn't be linked to products or categories. Payment figures were still included at the order level in case needed.
- A postcode prefix has one city/state/location. In reality a postcode prefix could straddle a boundary, in this case most common value was used. Coordinates are the median of all points in the prefix (didn't end up using this for analysis). City and state come from the customer and seller records rather than the geolocation file, as the geolocation file would need normalisation for things like letter accents (e.g. sau paulo vs são paulo)

### What I would flag as a limitation
- The data is seven years old and covers roughly a 2 year span - probably not an accurate representation of the business today.
- Retention is measured over a 90 day window. This keeps cohorts running to May 2018, with a broader dataset we could potentially use a longer window to capture more returns.
- The insight on freight vs demand is a correlation, but the data can't show us that reducing freight costs would increase sales in certain regions. A pricing test would be needed to provide more evidence here.
- The revenue estimate is not a projection. There's not much of a baseline to compare against, so its hard to tell what to expect with a campaign like this.

### Data quality checks I would run in prod
- Value ranges and permitted ranges in the form of constraints. E.g. review scores between 1 and 5, prices and freight non-negative, delivery time is after order time, order status is one of the 8 known values. A violation of any of these checks fails loudly rather than landing bad data.
- Referential integrity between order tables. Every order item, payment, review etc. must resolve to an order, and every order must resolve to a customer. Count orphans at each join and fail the run if any appear.
- Row counts and totals reconciled between layers. Bronze/silver/gold layers should all agree on order counts and total item value on every run (accounting for transformations such as deduplication in bronze -> silver). This would catch things like joins duplicating rows, or filters applied at the wrong point, which would be invisible if totals were not compared.

### Source going stale or changing shape
- Raw layer writes without schema overwrite enabled, so a new or renamed column would cause a failure. The cleaning layer converts types strictly, so if a column changes format, it will error instead of filling with nulls.
- Staleness could be monitored by checking the age of the newest file in the landing error, and the latest order date. If these figures don't advance between runs, throw an alert/failure.
- Compare each run's row count against an expected amount (e.g. rolling average) to catch a change in volume, and check null rates of key columns.

## Client memo
We have created a data pipeline using Databricks. Raw order files land, are cleaned and validated against defined rules, then modelled into tables that can be queried directly. Nine source files are now one governed source of truth, rebuilt reproducibly on every pipeline run and deployed automatically to both a test and a production environment.

The key finding surface in the data is related to customer repeat purchase rate. Only 1.3% of customers place a second order within 90 days, and roughly a third of what previously looked like repeat business turns out to be single shopping trips recorded as two orders. Furthermore, better service barely helps: even customers who rate their experience five stars return only 1.4% of the time. This implies that customers are not leaving unhappy, but nothing is asking them to come back.

My recommended next step would be a post-purchase reactivation campaign, involving reaching out to customers and offering discounted shipping. A realistic improvement could be worth upwards of $400,000 per year, and we would know within a quarter whether it is having an impact.