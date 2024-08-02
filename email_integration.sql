-- Create a notification integration for email alerts
CREATE OR REPLACE NOTIFICATION INTEGRATION email_int
TYPE=EMAIL
ENABLED=TRUE
ALLOWED_RECIPIENTS=('insert-recipients-emails-here')
COMMENT='Integration for sending email alerts';


--------------create procedure---------------------------------

create or replace procedure SEND_TASK_FAILURE()
returns varchar
language sql
execute as caller
as
declare
    row_cnt int;
    task_detail varchar;
    integration_name varchar default 'email_int';
    email_to varchar default 'insert_email_here@example.com';
    email_subject varchar default 'Task Failure Alert';
    email_content varchar;
    exception_no_data exception (-20002, 'No Failed Task');
    exception_bad_row exception (-20003, '1 or more tasks have failed in the last hour');    
begin
    -- No failed tasks
    row_cnt := (SELECT count(*)
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp()),
    ERROR_ONLY => TRUE)) t
    where t.name IN ('task_name_1', 'task_name_2'));

    
    
    if (row_cnt = 0) then
        email_content := current_timestamp() ||'No Failed Task';
        call system$send_email(
            :integration_name,
            :email_to,
            :email_subject,
            :email_content);
            
        raise exception_no_data;
    end if;
    
    -- Failed tasks
    row_cnt := (SELECT count(*)
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp()),
    ERROR_ONLY => TRUE)) t
    where t.name IN ('task_name_1', 'task_name_2'));

    task_detail := (SELECT LISTAGG(CONCAT ( 'TASK NAME - ', NAME, ', TASK ERROR - ',ERROR_MESSAGE, ', TASK SCHEDULED TIME- ', SCHEDULED_TIME),'\n') as detail
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp()),
    ERROR_ONLY => TRUE)) t
    where t.name IN ('task_name_1', 'task_name_2'));
    
    if (row_cnt > 0) then
        email_content :=  row_cnt||' task/s have failed in the last hour.\n TASK DETAILS \n ' ||task_detail;
        call system$send_email(
            :integration_name,
            :email_to,
            :email_subject,
            :email_content);
            
        raise exception_bad_row;
    end if;
   
    return 'SUCCESS';   
end;


        
------------------------create alert ---------------------------------
CREATE OR REPLACE ALERT TASK_FAILURE_ALERT
WAREHOUSE = 'WH_name'
SCHEDULE = '60 minute'
IF (EXISTS
(
      SELECT count(*)
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp()),
    ERROR_ONLY => TRUE)) t
    where t.name IN ('task_name_1', 'task_name_2')
))
THEN CALL SEND_TASK_FAILURE();



EXECUTE ALERT TASK_FAILURE_ALERT;