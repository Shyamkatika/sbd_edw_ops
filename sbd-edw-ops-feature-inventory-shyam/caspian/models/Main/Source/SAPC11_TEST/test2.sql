with test as(
    select * from {{source('SAPC11','EMP')}}
)
SELECT * FROM TEST 