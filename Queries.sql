/*
Q1
For each user_id, find the difference between the last action and the second last action. Action
here is defined as visiting a page.

The table below shows for each user all the pages she visited and the corresponding
timestamp.

Table 1 (actions)
ColumnName          Value           Description
user_id             6684            this is id of the user
page                home_page       the page visited
unix_timestamp      1451640067      unix timestamp in seconds
*/


select user_id,
	   page,
       unix_timestamp - second_last as time_difference_last_actions
from
(select lag(unix_timestamp, 1) over (partition by user_id order by unix_timestamp) as second_last,
       row_number() over (partition by user_id order by unix_timestamp desc) as rn,
       *
from actions) sub
where rn = 1
order by user_id;

/*
Q2
Write a query that returns the percentage of users who only visited mobile, only web and both.
That is, the percentage of users who are only in the mobile table, only in the web table and in
both tables.

Table 1 (mobile)
ColumnName  Value           Description
user_id     128             this is id of the user who visited a given page on mobile
page        page_5_mobile   page visited by that user on mobile

Table 2 (web)
ColumnName  Value           Description
user_id     1210            this is id of the user who visited a given page on web
page        page_1_web      page visited by that user on web
*/


select 100 * count(case when web.user_id is null and mobile.user_id is not null then 1 end) / count(*) as mobile_only,
       100 * count(case when web.user_id is not null and mobile.user_id is null then 1 end) / count(*) as web_only,
       100 * count(case when web.user_id is not null and mobile.user_id is not null then 1 end) / count(*) as both
from
    (select distinct user_id from web) web
    full outer join
    (select distinct user_id from mobile) mobile
    using(user_id)

/*
Q3
Power users are those who bought at least 10 products. Write a query that
returns for each user on which day they became a power user. That is, for each user, on which
day they bought the 10th item.

The table below represents transactions. That is, each row means that the corresponding user
has bought something on that date.

Table 1 (users)
ColumnName  Value                   Description
user_id     675                     this is id of the user
date        2014-12-31 16:16:12     user 675 bought something on Dec 31, 2014 at 4:16:12 PM
*/


select user_id, date
from
(select user_id,
	   date,
       row_number() over (partition by user_id order by date) as purchase_number
from users) sub
where purchase_number = 10;

/*Q4.1, 4.2
4.1) Write a query that returns the total amount of money spent by each user. That is, the sum
of the column transaction_amount for each user over both tables.
4.2) Write a query that returns day by day the cumulative sum of money spent by each user.
That is, each day a user had a transcation, we should have how much money she has
spent in total until that day. Obviously, the last day cumulative sum should match the
numbers from the previous bullet point

Table 1 (march)
ColumnName          Value           Description
user_id             13399           this is id of the user who had the corresponding transaction
date                2015-03-01      the transaction happened on March 1st.
transaction_amount  18              the user spent 18$ in that transaction

Table 2 (april)
ColumnName          Value           Description
user_id             15895           this is id of the user who had the corresponding transaction
date                2015-04-01      the transaction happened on April 1st.
transaction_amount  66              the user spent 66$ in that transaction
*/


select user_id, sum(transaction_amount) as total_spent
from
(select * from march
 union all
 select * from april) sub
group by user_id
order by user_id;

select user_id, date, sum(daily_spent) over (partition by user_id order by date) as running_total
from
(select user_id, date, sum(transaction_amount) as daily_spent
 from march
 group by user_id, date
 union all
 select user_id, date, sum(transaction_amount) as daily_spent
 from april
 group by user_id, date) sub;


 /*
Q5.
Find the average and median transaction amount only considering those transactions that
happen on the same date as that user signed-up.

Table 1 (signup)
ColumnName          Value           Description
user_id             121             this is id of the user
sign_up_date        2015-01-02      user_id 121 signed up on Jan, 2.

Table 2 (transactions)
ColumnName          Value           Description
user_id             856898          this is id of the user
date                2015-08-02      transaction happened on Aug, 2 at almost 4AM.
                    03:56:08      
transaction_amount  49              transaction amount was 49$.
*/


select round(avg(transaction_amount),2),
       avg(case when rnasc >= rndesc - 1 and rnasc <= rndesc + 1 then transaction_amount else null end)::int
from
(
select transaction_amount,
       row_number() over (order by transaction_amount asc) as rnasc,
       count(*) over () - row_number() over (order by transaction_amount asc) + 1 as rndesc
from transactions t, signup s
where t.user_id=s.user_id
and t.transaction_date::date = sign_up_date
order by rnasc
) sub

/*
Q6.1, 6.2
6.1) Find the country with the largest and smallest number of users
6.2) Write a query that returns for each country the first and the last user who signed up (if that
country has just one user, it should just return that single user

Table 1 (countries)
ColumnName          Value           Description
user_id             2               this is id of the user
created_at          2015-02-28      user 2 created her account on Feb, 2 around 4PM
                    16:00:40
country             China           She is based in China
*/

--Q1
select country, count(distinct user_id)
from countries
where country = (select country
                 from countries
                 group by country
                 order by count(distinct user_id) desc
                 limit 1)
or country = (select country
              from countries
              group by country
              order by count(distinct user_id)
              limit 1)
group by country;

--Q2
select country, user_id, created_at
from
(select country,
       user_id,
       created_at,
       row_number() over (partition by country order by created_at) as rn_asc,
       row_number() over (partition by country order by created_at desc) as rn_desc
from countries) sub
where rn_asc = 1 or rn_desc = 1
order by country