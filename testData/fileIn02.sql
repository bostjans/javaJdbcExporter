select 1


select 2 as TestResult;


select 3;
select 4 as TestResult;

select 5 as TestResult;

-- First comment
select 6 as TestResult;
-- Comment
select 7 as TestResult;

-- Check NULL
select 8 as TestResult, NULL as NullField, 9 as TestResult2;

-- Select ..
--select * from partner;
--select * from cl_state order by 1;
--select * from cl_zip order by 1;
select count(*) from cl_partner;
select * from cl_partner;
