select * from df_orders

drop table df_orders

CREATE TABLE df_orders (
    order_id INT PRIMARY KEY,
    order_date DATE,
    ship_mode VARCHAR(20),
    segment VARCHAR(20),
    country VARCHAR(20),
    city VARCHAR(20),
    state VARCHAR(20),
    postal_code VARCHAR(20),
    region VARCHAR(20),
    category VARCHAR(20),
    sub_category VARCHAR(20),
    product_id VARCHAR(50),
    quantity INT,
    discount DECIMAL(7,2),
    sale_price DECIMAL(7,2),
    profit DECIMAL(7,2)
);


-- Find top 10 highest revenue generating product
select * from df_orders

select TOP(10) product_id, sum(sale_price) as sales 
from df_orders
group by product_id	
order by sales DESC;


-- Find top 5 highest saling product in each region
select distinct region from df_orders;

with cte as (
select region, product_id, sum(sale_price) as sales
from df_orders
group by region, product_id	)
select * from(
select *,
ROW_NUMBER() over(partition by region order by sales desc) as rn
from cte) A
where rn <=5;


-- Find month over month growth comparison for 2022 and 2023 sales  eg: jan 2022 vs jan 2023
with cte as(
select year(order_date) as order_year, month(order_date) as order_month, sum(sale_price) as sales
from df_orders
group by year(order_date), month(order_date)
--order by year(order_date), month(order_date)
)
select order_month,
	sum(case when order_year = 2022 then sales else 0 end) as sales_2022,
	sum(case when order_year = 2023 then sales else 0 end) as sales_2023
from cte
group by order_month
order by order_month



-- For each category which month had highest sales
with cte as(
select 
	category,
	format(order_date,'yyyyMM') as order_year_month,
	sum(sale_price) as sales
from df_orders
group by category, format(order_date,'yyyyMM')
--order by category, format(order_date,'yyyyMM')
)
select * from(
select *,
ROW_NUMBER() over(partition by category order by sales desc) as rn
from cte) A
where rn = 1



-- Which sub category had highest growth by profit in 2023 compare to 2022		 

-- solved with cte and sub query
with cte as(
select sub_category, year(order_date) as order_year, sum(sale_price) as sales
from df_orders
group by sub_category,year(order_date)
--order by year(order_date), month(order_date)
)
select top 1
	sub_category,
	sales_2022,
	sales_2023,
	((sales_2023 - sales_2022) / sales_2022) * 100 as grownth_sub_category from(
select sub_category,
	sum(case when order_year = 2022 then sales else 0 end) as sales_2022,
	sum(case when order_year = 2023 then sales else 0 end) as sales_2023
from cte
group by sub_category) A
order by grownth_sub_category desc;

-- solved with cte
with cte as(
select sub_category, year(order_date) as order_year, sum(sale_price) as sales
from df_orders
group by sub_category,year(order_date)
--order by year(order_date), month(order_date)
)
, cte2 as(
select sub_category,
	sum(case when order_year = 2022 then sales else 0 end) as sales_2022,
	sum(case when order_year = 2023 then sales else 0 end) as sales_2023
from cte
group by sub_category) 

select Top(1) 
	*,
	((sales_2023 - sales_2022) / sales_2022) * 100 as grownth_sub_category
from cte2
order by grownth_sub_category desc;