--
-- :name get-denoms :? :*
-- :doc SELECT denomination
SELECT loan_type::text, loanable_amount, nullif(denom_serviceq::int, -1),
       nullif(denom_serviceq_amount::int, -1) as nullif_2,charging_method
FROM tbl_decisioning_denom
ORDER BY loan_type,loanable_amount;

-- :name get-loan-periods :? :*
select loan_type::text, loanable_amount, extract(epoch from denom_grace_period)::integer,advance_name,charging_method
from tbl_decisioning_denom
order by loanable_amount;

-- :name proc-get-subscriber-info :? :1
-- :doc select proc-get-subscriber-info
SELECT * FROM proc_get_subscriber_info(:sub::bigint);


-- :name check-unreconciled-sub :? :*
SELECT request_amount from tbl_vtop_credit_req_new
WHERE subscriber_fk = :sub and request_status in (-1,2);

-- :name get-loan-max-bals :? :*
-- :doc get loan maximums
select loan_type::text, loanable_amount, denom_max_bal,advance_name
from tbl_decisioning_denom
where denom_max_bal >= 0
order by loan_type, loanable_amount;


-- :name updateLoanReq :? :1
-- update :i:tbl
-- set error_message = nullif(:error, ''), status_flags = status_flags | :status_flag
-- where loan_request_id = :request-id

-- :name insertTblLoanReq :! :n
insert into tbl_loan_req
(loan_request_id,subscriber_fk,cedis_requested,cedis_serviceq,max_loanable,loan_flags,loan_type,channel,ml_loan_counter, flag_done)
values (:request-id,:subscriber,:amount,:serviceq,:max_loanable,:loan_flags,:loan_type::typ_loan_type,:channel,:loan_count, :flag_done);


-- :name insertVtopCreditReq :! :n
insert into tbl_vtop_credit_req_new
(request_fk, subscriber_fk, loan_type, request_amount)
values (:request-id, :subscriber, :loan-type::typ_loan_type, :amount);


-- :name procUpdateVtopReqStatus :? :1
-- :doc selects from proc_update_vtop_req_status
select * from proc_update_vtop_req_status
    (:request-id::bigint,:status::smallint,:error::text,:time_processed::timestamp,:external-response-code::text,:external-txn-id::text,:post-event-balance::typ_money_unit);

-- :name proc_update_data_status :? :1
-- :doc updates profit guru data status
select * from proc_update_data_status
    (:loan_id::bigint,:status::smallint, :flag_done::boolean);

--:name select-loan :? :1
select * from tbl_loans_new
where subscriber_fk=:subscriber::bigint and loan_type ='data'
and cedis_paid = 0
order by loan_time desc;

-- :name proc_payment_gateway_reconcile :? :1
-- :doc run lending reconcilation
select * from proc_payment_gateway_reconcile();


-- :name new-session! :? :1
-- :doc Creates a new session entry in the database.
select * from proc_new_session(:session-id, :subscriber::bigint, :session-data::bytea, :max-age::text, :allow-resume?);

-- :name %get-session-data :? :1
-- :doc Retrieve session data from the database.
select subscriber_no, encode (session_data,'escape') as session_data
from tbl_ussd_session
where session_id = :session-id;

-- :name %update-session! :? :1
-- :doc Update session data.
select * from proc_update_session(:session-id, :subscriber::bigint, :session-data::bytea);

-- :name close-session! :? :1
-- :doc Terminate a session.
select * from proc_terminate_session(:session-id, :subscriber::bigint)

-- :name clear-expired-sessions! :! :n
delete from tbl_ussd_session
where time_end < now();
