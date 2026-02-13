from ..pipeline import config_btk

def get_ar_data_query(identity_tag: str):
    return f"select * from AR where batch_no = '{identity_tag}'"


def get_enquiry_data_query(identity_tag: str):
    return f"select * from ENQ where batch_no = '{identity_tag}'"


def get_score_data_query(identity_tag: str):
    return f"select * from score where batch_no = '{identity_tag}'"


def get_pii_master_query(identity_tag:str) -> str:
    
    return f"""

 select 
distinct id.customer_id,
case when ref.phone is null then c.latest_mobile else ref.phone end as mobile_number,
c.latest_mobile as latest_mobile,
concat(substr(mobile_number,1,1),lpad(substr(mobile_number,-4,4),9,'X')) as masked_phone,
nb.full_name,
case when substr(upper(a.pan::STRING),4,1) = 'P' then a.pan else null end as pan_number,
case when a.pan is null then 'invalid_pan' else 'valid_pan' end as pan_flag,
lpad(substr(pan_number,-5,5),10,'X') as masked_pan,
case
when nb.gender = '1' then 'Female'
when nb.gender = '2' then 'Male'
when nb.gender = '3' then 'Transgender'
else NULL
end as gender,
case 
when (datediff('year',nb.dob ,nb.created_at) < 100) and (datediff('year',nb.dob ,nb.created_at) > 0) then nb.dob
when (substr(nb.dob,1,4) < year(current_date())) and (year(current_date()) - (substr(nb.dob,1,4)) < 100) then nb.dob
else null 
end as DOB,
b.mobile_list, 
e.latest_pincode as pincode, 
d.pin_list as pincode_list,
f.address_list,
g.latest_address,
id.score_v3 as score
from {config_btk.database}.{config_btk.schema}.score as id
join {config_btk.database}.bureau_data_exchange.equifax_data as ref on ref.bureau_id = id.customer_id and ref.scrub_batch_no='{identity_tag}'
left join {config_btk.database}.{config_btk.schema}.id as a on a.customer_id = id.customer_id
left join {config_btk.database}.{config_btk.schema}.name_dob as nb on id.customer_id = nb.customer_id
left join (
--- aggregated list of mobile along with reported date
select * from (select customer_id,
listagg(concat(substr(phone,-10,10),'|',date_reported),',') as mobile_list 
from  {config_btk.database}.{config_btk.schema}.phone as p
group by 1)) b on id.customer_id = b.customer_id
left join (

--- fetching latest reported mobile
select * from (select 
    customer_id,date_reported,
    (regexp_replace(substr(phone,-10,10),',?[0-5]{1}[0-9]{9}','')) as latest_mobile, 
    row_number() over(partition by customer_id order by date_reported desc) as rnk 
    from {config_btk.database}.{config_btk.schema}.phone
    where length(latest_mobile) = 10) where rnk = 1) c
on id.customer_id = c.customer_id
left join (
--- aggregated list of pincodes along with reported date
select * from (select customer_id,
listagg(concat(substr(pincode,-6,6),'|',date_reported),',') as pin_list 
from  {config_btk.database}.{config_btk.schema}.address
group by 1)) d on id.customer_id = d.customer_id
left join (
--- fetching latest reported pincode
select * from (select 
    customer_id,
    substr(pincode,-6,6) as latest_pincode, 
    row_number() over(partition by customer_id order by date_reported desc) as rnk 
    from {config_btk.database}.{config_btk.schema}.address
    where length(latest_pincode) = 6) where rnk = 1) e
on id.customer_id = e.customer_id
left join (
--- aggregated list of address along with reported date
select * from (select customer_id,
listagg(concat(address,'|',date_reported),',') as address_list 
from {config_btk.database}.{config_btk.schema}.address
group by 1)) f 
on id.customer_id = f.customer_id
left join (
--- fetching latest reported address
select * from (select 
    customer_id,
    address as latest_address, 
    row_number() over(partition by customer_id order by date_reported desc) as rnk 
    from {config_btk.database}.{config_btk.schema}.address
    ) where rnk = 1) g
on id.customer_id = g.customer_id
where id.batch_no = '{identity_tag}'
    """






def get_additional_attr_tli(identity_tag:str) -> str:
        return f"""



with base as(select * from(
(select distinct dm.customer_id, 
score,
dob,
dm.batch_no,
FLOOR(DATEDIFF('day', dob, CURRENT_DATE) / 365.25) AS age,
row_number() over(partition by dm.customer_id order by dm.created_at desc) as rnk
from {config_btk.database}.bureau_data_exchange.equifax_data n
join {config_btk.database}.{config_btk.schema}.pii_master dm on dm.customer_id=n.bureau_id
where dm.batch_no='{identity_tag}'))where rnk=1),


acct as (select *, 
case when tot_active_pl_loan_amt>0 then (tot_active_pl_bal/tot_active_pl_loan_amt)*100 else 0 end as current_pl_util,
case when tot_active_gld_loan_amt>0 then (tot_active_gld_bal/tot_active_gld_loan_amt)*100 else 0 end as current_gld_util,
case when tot_active_bl_loan_amt>0 then (tot_active_bl_bal/tot_active_bl_loan_amt)*100 else 0 end as current_bl_util,
case when tot_active_cc_loan_amt>0 then (tot_active_cc_bal/tot_active_cc_loan_amt)*100 else 0 end as current_cc_util,

case when tot_active_pl_loan_amt_nbfc>0 then (tot_active_pl_bal_nbfc/tot_active_pl_loan_amt_nbfc)*100 else 0 end as current_pl_util_nbfc,
case when tot_active_pl_loan_amt_BANK>0 then (tot_active_pl_bal_BANK/tot_active_pl_loan_amt_BANK)*100 else 0 end as current_pl_util_BANK,
case when tot_active_pl_loan_amt_MFI>0 then (tot_active_pl_bal_MFI/tot_active_pl_loan_amt_MFI)*100 else 0 end as current_pl_util_MFI,
case when tot_active_pl_loan_amt_SFB>0 then (tot_active_pl_bal_SFB/tot_active_pl_loan_amt_SFB)*100 else 0 end as current_pl_util_SFB,
case when tot_active_pl_loan_amt_OTHER>0 then (tot_active_pl_bal_OTHER/tot_active_pl_loan_amt_OTHER)*100 else 0 end as current_pl_util_OTHER,

case when tot_active_cc_loan_amt_nbfc>0 then (tot_active_cc_bal_nbfc/tot_active_cc_loan_amt_nbfc)*100 else 0 end as current_cc_util_nbfc,
case when tot_active_cc_loan_amt_BANK>0 then (tot_active_cc_bal_BANK/tot_active_cc_loan_amt_BANK)*100 else 0 end as current_cc_util_BANK,
case when tot_active_cc_loan_amt_MFI>0 then (tot_active_cc_bal_MFI/tot_active_cc_loan_amt_MFI)*100 else 0 end as current_cc_util_MFI,
case when tot_active_cc_loan_amt_SFB>0 then (tot_active_cc_bal_SFB/tot_active_cc_loan_amt_SFB)*100 else 0 end as current_cc_util_SFB,
case when tot_active_cc_loan_amt_OTHER>0 then (tot_active_cc_bal_OTHER/tot_active_cc_loan_amt_OTHER)*100 else 0 end as current_cc_util_OTHER,


case when tot_active_gld_loan_amt_nbfc>0 then (tot_active_gld_bal_nbfc/tot_active_gld_loan_amt_nbfc)*100 else 0 end as current_gld_util_nbfc,
case when tot_active_gld_loan_amt_BANK>0 then (tot_active_gld_bal_BANK/tot_active_gld_loan_amt_BANK)*100 else 0 end as current_gld_util_BANK,

case when tot_active_bl_loan_amt_nbfc>0 then (tot_active_bl_bal_nbfc/tot_active_bl_loan_amt_nbfc)*100 else 0 end as current_bl_util_nbfc,
case when tot_active_bl_loan_amt_BANK>0 then (tot_active_bl_bal_BANK/tot_active_bl_loan_amt_BANK)*100 else 0 end as current_bl_util_BANK,

from((select customer_id,
    sum(case when acct_type_cd=123 then 1 else 0 end) as nbr_pl,
    sum(case when acct_type_cd=123 and closed_dt is null then 1 else 0 end) as nbr_active_pl,
    max(case when acct_type_cd=123 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_pl_loan_amt,
    max(case when acct_type_cd=123 then try_cast(balance_am as number(38,2)) else 0 end) as max_pl_bal,
    sum(case when acct_type_cd=123 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_pl_loan_amt,
    sum(case when acct_type_cd=123 then try_cast(balance_am as number(38,2)) else 0 end) as tot_pl_bal,
    sum(case when acct_type_cd=123 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_pl_loan_amt,
    sum(case when acct_type_cd=123 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_pl_bal,
    max(case when acct_type_cd=123 then datediff('month', open_dt , created_at) else 0 end) as cb_pl_tenure,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_pl_opened_l1m,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_pl_opened_l2m,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_opened_l3m,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_opened_l6m,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_pl_opened_l9m,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_opened_l12m,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_pl_opened_l18m,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_opened_l24m,
    sum(case when acct_type_cd=123 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_opened_l36m,

    max(case 
        when acct_type_cd=123 and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        else 0 end) as max_pl_util,

    
    sum(case when acct_type_cd=189 then 1 else 0 end) as nbr_cd,
    sum(case when acct_type_cd=189 and closed_dt is null then 1 else 0 end) as nbr_active_cd,
    max(case when acct_type_cd=189 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_cd_loan_amt,
    max(case when acct_type_cd=189 then try_cast(balance_am as number(38,2)) else 0 end) as max_cd_bal,
    sum(case when acct_type_cd=189 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_cd_loan_amt,
    sum(case when acct_type_cd=189 then try_cast(balance_am as number(38,2)) else 0 end) as tot_cd_bal,
    sum(case when acct_type_cd=189 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_cd_loan_amt,
    sum(case when acct_type_cd=189 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cd_bal,
    max(case when acct_type_cd=189 then datediff('month', open_dt , created_at) else 0 end) as cb_cd_tenure,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cd_opened_l1m,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cd_opened_l2m,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cd_opened_l3m,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cd_opened_l6m,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cd_opened_l9m,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cd_opened_l12m,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cd_opened_l18m,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cd_opened_l24m,
    sum(case when acct_type_cd=189 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cd_opened_l36m,


    sum(case when acct_type_cd=195 then 1 else 0 end) as nbr_lap,
    sum(case when acct_type_cd=195 and closed_dt is null then 1 else 0 end) as nbr_active_lap,
    max(case when acct_type_cd=195 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_lap_loan_amt,
    max(case when acct_type_cd=195 then try_cast(balance_am as number(38,2)) else 0 end) as max_lap_bal,
    sum(case when acct_type_cd=195 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_lap_loan_amt,
    sum(case when acct_type_cd=195 then try_cast(balance_am as number(38,2)) else 0 end) as tot_lap_bal,
    sum(case when acct_type_cd=195 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_lap_loan_amt,
    sum(case when acct_type_cd=195 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_lap_bal,
    max(case when acct_type_cd=195 then datediff('month', open_dt , created_at) else 0 end) as cb_lap_tenure,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_lap_opened_l1m,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_lap_opened_l2m,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_lap_opened_l3m,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_lap_opened_l6m,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_lap_opened_l9m,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_lap_opened_l12m,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_lap_opened_l18m,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_lap_opened_l24m,
    sum(case when acct_type_cd=195 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_lap_opened_l36m,


    sum(case when acct_type_cd=58 then 1 else 0 end) as nbr_hl,
    sum(case when acct_type_cd=58 and closed_dt is null then 1 else 0 end) as nbr_active_hl,
    max(case when acct_type_cd=58 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_hl_loan_amt,
    max(case when acct_type_cd=58 then try_cast(balance_am as number(38,2)) else 0 end) as max_hl_bal,
    sum(case when acct_type_cd=58 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_hl_loan_amt,
    sum(case when acct_type_cd=58 then try_cast(balance_am as number(38,2)) else 0 end) as tot_hl_bal,
    sum(case when acct_type_cd=58 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_hl_loan_amt,
    sum(case when acct_type_cd=58 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_hl_bal,
    max(case when acct_type_cd=58 then datediff('month', open_dt , created_at) else 0 end) as cb_hl_tenure,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_hl_opened_l1m,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_hl_opened_l2m,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_hl_opened_l3m,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_hl_opened_l6m,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_hl_opened_l9m,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_hl_opened_l12m,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_hl_opened_l18m,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_hl_opened_l24m,
    sum(case when acct_type_cd=58 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_hl_opened_l36m,
    

    sum(case when acct_type_cd=47 then 1 else 0 end) as nbr_atl,
    sum(case when acct_type_cd=47 and closed_dt is null then 1 else 0 end) as nbr_active_atl,
    max(case when acct_type_cd=47 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_atl_loan_amt,
    max(case when acct_type_cd=47 then try_cast(balance_am as number(38,2)) else 0 end) as max_atl_bal,
    sum(case when acct_type_cd=47 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_atl_loan_amt,
    sum(case when acct_type_cd=47 then try_cast(balance_am as number(38,2)) else 0 end) as tot_atl_bal,
    sum(case when acct_type_cd=47 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_atl_loan_amt,
    sum(case when acct_type_cd=47 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_atl_bal,
    max(case when acct_type_cd=47 then datediff('month', open_dt , created_at) else 0 end) as cb_atl_tenure,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_atl_opened_l1m,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_atl_opened_l2m,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_atl_opened_l3m,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_atl_opened_l6m,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_atl_opened_l9m,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_atl_opened_l12m,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_atl_opened_l18m,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_atl_opened_l24m,
    sum(case when acct_type_cd=47 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_atl_opened_l36m,

    
    sum(case when acct_type_cd=191 then 1 else 0 end) as nbr_gld,
    sum(case when acct_type_cd=191 and closed_dt is null then 1 else 0 end) as nbr_active_gld,
    max(case when acct_type_cd=191 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_gld_loan_amt,
    max(case when acct_type_cd=191 then try_cast(balance_am as number(38,2)) else 0 end) as max_gld_bal,
    sum(case when acct_type_cd=191 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_gld_loan_amt,
    sum(case when acct_type_cd=191 then try_cast(balance_am as number(38,2)) else 0 end) as tot_gld_bal,
    sum(case when acct_type_cd=191 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_gld_loan_amt,
    sum(case when acct_type_cd=191 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_gld_bal,
    max(case when acct_type_cd=191 then datediff('month', open_dt , created_at) else 0 end) as cb_gld_tenure,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_gld_opened_l1m,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_gld_opened_l2m,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_gld_opened_l3m,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_gld_opened_l6m,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_gld_opened_l9m,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_gld_opened_l12m,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_gld_opened_l18m,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_gld_opened_l24m,
    sum(case when acct_type_cd=191 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_gld_opened_l36m,
    
    max(case 
        when acct_type_cd=191 and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        else 0 end) as max_gld_util,
    

    sum(case when acct_type_cd=240 then 1 else 0 end) as nbr_pmay,
    sum(case when acct_type_cd=240 and closed_dt is null then 1 else 0 end) as nbr_active_pmay,
    max(case when acct_type_cd=240 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_pmay_loan_amt,
    max(case when acct_type_cd=240 then try_cast(balance_am as number(38,2)) else 0 end) as max_pmay_bal,
    sum(case when acct_type_cd=240 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_pmay_loan_amt,
    sum(case when acct_type_cd=240 then try_cast(balance_am as number(38,2)) else 0 end) as tot_pmay_bal,
    sum(case when acct_type_cd=240 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_pmay_loan_amt,
    sum(case when acct_type_cd=240 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_pmay_bal,
    max(case when acct_type_cd=240 then datediff('month', open_dt , created_at) else 0 end) as cb_pmay_tenure,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_pmay_opened_l1m,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_pmay_opened_l2m,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pmay_opened_l3m,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pmay_opened_l6m,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_pmay_opened_l9m,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pmay_opened_l12m,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_pmay_opened_l18m,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pmay_opened_l24m,
    sum(case when acct_type_cd=240 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pmay_opened_l36m,

    
    sum(case when acct_type_cd=130 then 1 else 0 end) as nbr_edul,
    sum(case when acct_type_cd=130 and closed_dt is null then 1 else 0 end) as nbr_active_edul,
    max(case when acct_type_cd=130 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_edul_loan_amt,
    max(case when acct_type_cd=130 then try_cast(balance_am as number(38,2)) else 0 end) as max_edul_bal,
    sum(case when acct_type_cd=130 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_edul_loan_amt,
    sum(case when acct_type_cd=130 then try_cast(balance_am as number(38,2)) else 0 end) as tot_edul_bal,
    sum(case when acct_type_cd=130 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_edul_loan_amt,
    sum(case when acct_type_cd=130 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_edul_bal,
    max(case when acct_type_cd=130 then datediff('month', open_dt , created_at) else 0 end) as cb_edul_tenure,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_edul_opened_l1m,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_edul_opened_l2m,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_edul_opened_l3m,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_edul_opened_l6m,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_edul_opened_l9m,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_edul_opened_l12m,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_edul_opened_l18m,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_edul_opened_l24m,
    sum(case when acct_type_cd=130 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_edul_opened_l36m,

    
    sum(case when acct_type_cd=227 then 1 else 0 end) as nbr_mudl,
    sum(case when acct_type_cd=227 and closed_dt is null then 1 else 0 end) as nbr_active_mudl,
    max(case when acct_type_cd=227 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_mudl_loan_amt,
    max(case when acct_type_cd=227 then try_cast(balance_am as number(38,2)) else 0 end) as max_mudl_bal,
    sum(case when acct_type_cd=227 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_mudl_loan_amt,
    sum(case when acct_type_cd=227 then try_cast(balance_am as number(38,2)) else 0 end) as tot_mudl_bal,
    sum(case when acct_type_cd=227 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_mudl_loan_amt,
    sum(case when acct_type_cd=227 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_mudl_bal,
    max(case when acct_type_cd=227 then datediff('month', open_dt , created_at) else 0 end) as cb_mudl_tenure,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_mudl_opened_l1m,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_mudl_opened_l2m,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_mudl_opened_l3m,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_mudl_opened_l6m,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_mudl_opened_l9m,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_mudl_opened_l12m,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_mudl_opened_l18m,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_mudl_opened_l24m,
    sum(case when acct_type_cd=227 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_mudl_opened_l36m,


    sum(case when acct_type_cd=121 then 1 else 0 end) as nbr_od,
    sum(case when acct_type_cd=121 and closed_dt is null then 1 else 0 end) as nbr_active_od,
    max(case when acct_type_cd=121 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_od_loan_amt,
    max(case when acct_type_cd=121 then try_cast(balance_am as number(38,2)) else 0 end) as max_od_bal,
    sum(case when acct_type_cd=121 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_od_loan_amt,
    sum(case when acct_type_cd=121 then try_cast(balance_am as number(38,2)) else 0 end) as tot_od_bal,
    sum(case when acct_type_cd=121 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_od_loan_amt,
    sum(case when acct_type_cd=121 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_od_bal,
    max(case when acct_type_cd=121 then datediff('month', open_dt , created_at) else 0 end) as cb_od_tenure,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_od_opened_l1m,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_od_opened_l2m,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_od_opened_l3m,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_od_opened_l6m,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_od_opened_l9m,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_od_opened_l12m,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_od_opened_l18m,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_od_opened_l24m,
    sum(case when acct_type_cd=121 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_od_opened_l36m,


    sum(case when acct_type_cd=999 then 1 else 0 end) as nbr_others,
    sum(case when acct_type_cd=999 and closed_dt is null then 1 else 0 end) as nbr_active_others,
    max(case when acct_type_cd=999 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_others_loan_amt,
    max(case when acct_type_cd=999 then try_cast(balance_am as number(38,2)) else 0 end) as max_others_bal,
    sum(case when acct_type_cd=999 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_others_loan_amt,
    sum(case when acct_type_cd=999 then try_cast(balance_am as number(38,2)) else 0 end) as tot_others_bal,
    sum(case when acct_type_cd=999 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_others_loan_amt,
    sum(case when acct_type_cd=999 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_others_bal,
    max(case when acct_type_cd=999 then datediff('month', open_dt , created_at) else 0 end) as cb_others_tenure,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_others_opened_l1m,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_others_opened_l2m,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_others_opened_l3m,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_others_opened_l6m,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_others_opened_l9m,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_others_opened_l12m,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_others_opened_l18m,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_others_opened_l24m,
    sum(case when acct_type_cd=999 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_others_opened_l36m,

    sum(case when acct_type_cd in (175,176,241,228) then 1 else 0 end) as nbr_bl,
    sum(case when acct_type_cd in (175,176,241,228) and closed_dt is null then 1 else 0 end) as nbr_active_bl,
    max(case when acct_type_cd in (175,176,241,228) then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_bl_loan_amt,
    max(case when acct_type_cd in (175,176,241,228) then try_cast(balance_am as number(38,2)) else 0 end) as max_bl_bal,
    sum(case when acct_type_cd in (175,176,241,228) then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_bl_loan_amt,
    sum(case when acct_type_cd in (175,176,241,228) then try_cast(balance_am as number(38,2)) else 0 end) as tot_bl_bal,
    sum(case when acct_type_cd in (175,176,241,228) and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_bl_loan_amt,
    sum(case when acct_type_cd in (175,176,241,228) and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_bl_bal,
    max(case when acct_type_cd in (175,176,241,228) then datediff('month', open_dt , created_at) else 0 end) as cb_bl_tenure,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_bl_opened_l1m,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_bl_opened_l2m,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_bl_opened_l3m,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_bl_opened_l6m,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_bl_opened_l9m,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_bl_opened_l12m,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_bl_opened_l18m,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_bl_opened_l24m,
    sum(case when acct_type_cd in (175,176,241,228) and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_bl_opened_l36m,
    
    max(case 
        when acct_type_cd in (175,176,241,228) and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        else 0 end) as max_bl_util,

        

    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then 1 else 0 end) as nbr_secured_acct,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and closed_dt is null then 1 else 0 end) as nbr_active_secured_acct,
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_secured_acct_loan_amt,
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then try_cast(balance_am as number(38,2)) else 0 end) as max_secured_acct_bal,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_secured_acct_loan_amt,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then try_cast(balance_am as number(38,2)) else 0 end) as tot_secured_acct_bal,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_secured_acct_loan_amt,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_secured_acct_bal,
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then datediff('month', open_dt , created_at) else 0 end) as cb_secured_acct_tenure,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_secured_acct_opened_l1m,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_secured_acct_opened_l2m,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_secured_acct_opened_l3m,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_secured_acct_opened_l6m,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_secured_acct_opened_l9m,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_secured_acct_opened_l12m,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_secured_acct_opened_l18m,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_secured_acct_opened_l24m,
    sum(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_secured_acct_opened_l36m,
    

    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) then 1 else 0 end) as nbr_unsecured_acct,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and closed_dt is null then 1 else 0 end) as nbr_active_unsecured_acct,
    max(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_unsecured_acct_loan_amt,
    max(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) then try_cast(balance_am as number(38,2)) else 0 end) as max_unsecured_acct_bal,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_unsecured_acct_loan_amt,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) then try_cast(balance_am as number(38,2)) else 0 end) as tot_unsecured_acct_bal,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_unsecured_acct_loan_amt,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_unsecured_acct_bal,
    max(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) then datediff('month', open_dt , created_at) else 0 end) as cb_unsecured_acct_tenure,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_unsecured_acct_opened_l1m,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_unsecured_acct_opened_l2m,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_unsecured_acct_opened_l3m,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_unsecured_acct_opened_l6m,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_unsecured_acct_opened_l9m,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_unsecured_acct_opened_l12m,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_unsecured_acct_opened_l18m,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_unsecured_acct_opened_l24m,
    sum(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245, 249) and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_unsecured_acct_opened_l36m,

    sum(case when acct_type_cd=221 then 1 else 0 end) as nbr_usedcar,
    sum(case when acct_type_cd=221 and closed_dt is null then 1 else 0 end) as nbr_active_usedcar,
    max(case when acct_type_cd=221 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_usedcar_loan_amt,
    max(case when acct_type_cd=221 then try_cast(balance_am as number(38,2)) else 0 end) as max_usedcar_bal,
    sum(case when acct_type_cd=221 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_usedcar_loan_amt,
    sum(case when acct_type_cd=221 then try_cast(balance_am as number(38,2)) else 0 end) as tot_usedcar_bal,
    sum(case when acct_type_cd=221 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_usedcar_loan_amt,
    sum(case when acct_type_cd=221 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_usedcar_bal,
    max(case when acct_type_cd=221 then datediff('month', open_dt , created_at) else 0 end) as cb_usedcar_tenure,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_usedcar_opened_l1m,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_usedcar_opened_l2m,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_usedcar_opened_l3m,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_usedcar_opened_l6m,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_usedcar_opened_l9m,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_usedcar_opened_l12m,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_usedcar_opened_l18m,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_usedcar_opened_l24m,
    sum(case when acct_type_cd=221 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_usedcar_opened_l36m,

    sum(case when acct_type_cd=245 then 1 else 0 end) as nbr_plp2p,
    sum(case when acct_type_cd=245 and closed_dt is null then 1 else 0 end) as nbr_active_plp2p,
    max(case when acct_type_cd=245 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_plp2p_loan_amt,
    max(case when acct_type_cd=245 then try_cast(balance_am as number(38,2)) else 0 end) as max_plp2p_bal,
    sum(case when acct_type_cd=245 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_plp2p_loan_amt,
    sum(case when acct_type_cd=245 then try_cast(balance_am as number(38,2)) else 0 end) as tot_plp2p_bal,
    sum(case when acct_type_cd=245 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_plp2p_loan_amt,
    sum(case when acct_type_cd=245 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_plp2p_bal,
    max(case when acct_type_cd=245 then datediff('month', open_dt , created_at) else 0 end) as cb_plp2p_tenure,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_plp2p_opened_l1m,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_plp2p_opened_l2m,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_plp2p_opened_l3m,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_plp2p_opened_l6m,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_plp2p_opened_l9m,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_plp2p_opened_l12m,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_plp2p_opened_l18m,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_plp2p_opened_l24m,
    sum(case when acct_type_cd=245 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_plp2p_opened_l36m,

    
    sum(case when acct_type_cd=5 then 1 else 0 end) as nbr_cc,
    sum(case when acct_type_cd=5 and closed_dt is null then 1 else 0 end) as nbr_active_cc,
    max(case when acct_type_cd=5 then try_cast(balance_am as number(38,2)) else 0 end) as max_cc_bal,
    sum(case when acct_type_cd=5 then try_cast(balance_am as number(38,2)) else 0 end) as tot_cc_bal,
    
    max(case 
        when acct_type_cd=5 and (try_cast(credit_limit_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(credit_limit_am as number(38,2)))*100
        when acct_type_cd=5 and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        when acct_type_cd=5 and (try_cast(balance_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(balance_am as number(38,2)))*100
        else 0 end) as max_cc_util,

    max(case
        when acct_type_cd=5 and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_limit,

    sum(case 
        when acct_type_cd=5 and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as total_active_cc_limit,

    sum(case 
        when acct_type_cd=5 and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_loan_amt,

    max(case
        when acct_type_cd=5 and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_loan_amt,
        
    sum(case when acct_type_cd=5 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cc_bal,
    max(case when acct_type_cd=5 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as max_active_cc_bal,
    
   

    max(case 
         when acct_type_cd=5 and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_limit,

    sum(case 
         when acct_type_cd=5 and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as total_cc_limit,

     max(case 
         when acct_type_cd=5 and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_loan_amt,

    sum(case 
         when acct_type_cd=5 and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_loan_amt,
        
    max(case when acct_type_cd=5 then datediff('month', open_dt , created_at) else 0 end) as cb_cc_tenure,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cc_opened_l1m,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cc_opened_l2m,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cc_opened_l3m,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cc_opened_l6m,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cc_opened_l9m,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cc_opened_l12m,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cc_opened_l18m,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cc_opened_l24m,
    sum(case when acct_type_cd=5 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cc_opened_l36m,



    sum(case when acct_type_cd is not null then 1 else 0 end) as nbr_acct,
    sum(case when acct_type_cd is not null and closed_dt is null then 1 else 0 end) as nbr_active_acct,
    max(case when acct_type_cd is not null then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_acct_loan_amt,
    max(case when acct_type_cd is not null then try_cast(balance_am as number(38,2)) else 0 end) as max_acct_bal,
    sum(case when acct_type_cd is not null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_acct_loan_amt,
    sum(case when acct_type_cd is not null then try_cast(balance_am as number(38,2)) else 0 end) as tot_acct_bal,
    sum(case when acct_type_cd is not null and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_acct_loan_amt,
    sum(case when acct_type_cd is not null and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_acct_bal,
    max(case when acct_type_cd is not null then datediff('month', open_dt , created_at) else 0 end) as cb_acct_tenure,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_acct_opened_l1m,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_acct_opened_l2m,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_acct_opened_l3m,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_acct_opened_l6m,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_acct_opened_l9m,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_acct_opened_l12m,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_acct_opened_l18m,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_acct_opened_l24m,
    sum(case when acct_type_cd is not null and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_acct_opened_l36m,


    max(case when acct_type_cd not in (5) and closed_dt is null then past_due_am else 0 end) as current_max_overdue_non_cc,
    max(case when acct_type_cd=5 and closed_dt is null then past_due_am else 0 end) as current_max_overdue_cc,

    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) and closed_dt is null then past_due_am else 0 
    end) as current_max_overdue_secured,
    max(case when acct_type_cd in (5,121,123,169,225,187,189,196,213,214,245,249)then past_due_am else 0 end) as current_max_overdue_unsecured,

    sum(case when acct_type_cd=200 then 1 else 0 end) as nbr_restructured_loan,
    sum(case when acct_type_cd =200 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_restructured_opened_l24m,
    sum(case when acct_type_cd =200 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_restructured_opened_l36m,
    sum(case when acct_type_cd =200 and datediff('month', open_dt, created_at) <=48 then 1 else 0 end) as nbr_restructured_opened_l48m,
    
    sum(case when acct_type_cd in (204,06,02,08,206) then 1 else 0 end) as nbr_written_off,
    sum(case when acct_type_cd in (204,06,02,08,206) and closed_dt is null then 1 else 0 end) as nbr_active_written_off,
    max(case when acct_type_cd in (204,06,02,08,206) then 1 else 0 end) as ever_written_off_status,
    
    sum(case when acct_type_cd in (203,201,01,1) then 1 else 0 end) as nbr_suit_filed,
    sum(case when acct_type_cd in (203,201,01,1) and closed_dt is null then 1 else 0 end) as nbr_active_suit_filed,
    sum(case when acct_type_cd in (203,201,01,1) then 1 else 0 end) as ever_suit_filed_status,

    sum(case when acct_type_cd in (205,207) then 1 else 0 end) as nbr_wrt_sf,
    sum(case when acct_type_cd in (205,207) and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_wrt_sf_opened_l24m,
    sum(case when acct_type_cd in (205,207) and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_wrt_sf_opened_l36m,
    sum(case when acct_type_cd in (205,207) and datediff('month', open_dt, created_at) <=48 then 1 else 0 end) as nbr_wrt_sf_opened_l48m,



    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) is not null then 1 else 0 end) as nbr_pl_ticket_size_ever,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) <= 5000 then 1 else 0 end) as nbr_pl_ts_lt5k,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) <= 5000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_lt5k_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) <= 5000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_lt5k_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) <= 5000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_lt5k_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) <= 5000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_lt5k_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) <= 5000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_lt5k_opened_l36m,
    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 5000 and try_cast(orig_loan_am as number(38,2)) <= 10000 then 1 else 0 end) as nbr_pl_ts_5k_10k,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 5000 and try_cast(orig_loan_am as number(38,2)) <= 10000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_5k_10k_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 5000 and try_cast(orig_loan_am as number(38,2)) <= 10000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_5k_10k_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 5000 and try_cast(orig_loan_am as number(38,2)) <= 10000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_5k_10k_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 5000 and try_cast(orig_loan_am as number(38,2)) <= 10000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_5k_10k_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 5000 and try_cast(orig_loan_am as number(38,2)) <= 10000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_5k_10k_opened_l36m,

    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 10000 and try_cast(orig_loan_am as number(38,2)) <= 20000 then 1 else 0 end) as nbr_pl_ts_10k_20k,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 10000 and try_cast(orig_loan_am as number(38,2)) <= 20000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_10k_20k_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 10000 and try_cast(orig_loan_am as number(38,2)) <= 20000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_10k_20k_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 10000 and try_cast(orig_loan_am as number(38,2)) <= 20000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_10k_20k_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 10000 and try_cast(orig_loan_am as number(38,2)) <= 20000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_10k_20k_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 10000 and try_cast(orig_loan_am as number(38,2)) <= 20000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_10k_20k_opened_l36m,
    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 20000 and try_cast(orig_loan_am as number(38,2)) <= 50000 then 1 else 0 end) as nbr_pl_ts_20k_50k,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 20000 and try_cast(orig_loan_am as number(38,2)) <= 50000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_20k_50k_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 20000 and try_cast(orig_loan_am as number(38,2)) <= 50000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_20k_50k_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 20000 and try_cast(orig_loan_am as number(38,2)) <= 50000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_20k_50k_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 20000 and try_cast(orig_loan_am as number(38,2)) <= 50000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_20k_50k_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 20000 and try_cast(orig_loan_am as number(38,2)) <= 50000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_20k_50k_opened_l36m,



----------------------------------------------------------------------------------------------------------------------------------------New created features


------------------------------------------------------------------------------------------------------------------------------------PL

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 50000 and try_cast(orig_loan_am as number(38,2)) <= 100000 then 1 else 0 end) as nbr_pl_ts_50k_1L,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 50000 and try_cast(orig_loan_am as number(38,2)) <= 100000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_50k_1L_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 50000 and try_cast(orig_loan_am as number(38,2)) <= 100000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_50k_1L_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 50000 and try_cast(orig_loan_am as number(38,2)) <= 100000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_50k_1L_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 50000 and try_cast(orig_loan_am as number(38,2)) <= 100000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_50k_1L_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 50000 and try_cast(orig_loan_am as number(38,2)) <= 100000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_50k_1L_opened_l36m,
    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 100000 and try_cast(orig_loan_am as number(38,2)) <= 200000 then 1 else 0 end) as nbr_pl_ts_1L_2L,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 100000 and try_cast(orig_loan_am as number(38,2)) <= 200000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_1L_2L_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 100000 and try_cast(orig_loan_am as number(38,2)) <= 200000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_1L_2L_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 100000 and try_cast(orig_loan_am as number(38,2)) <= 200000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_1L_2L_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 100000 and try_cast(orig_loan_am as number(38,2)) <= 200000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_1L_2L_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 100000 and try_cast(orig_loan_am as number(38,2)) <= 200000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_1L_2L_opened_l36m,
    
 
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 200000 and try_cast(orig_loan_am as number(38,2)) <= 300000 then 1 else 0 end) as nbr_pl_ts_2L_3L,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 200000 and try_cast(orig_loan_am as number(38,2)) <= 300000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_2L_3L_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 200000 and try_cast(orig_loan_am as number(38,2)) <= 300000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_2L_3L_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 200000 and try_cast(orig_loan_am as number(38,2)) <= 300000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_2L_3L_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 200000 and try_cast(orig_loan_am as number(38,2)) <= 300000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_2L_3L_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 200000 and try_cast(orig_loan_am as number(38,2)) <= 300000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_2L_3L_opened_l36m,
    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 300000 and try_cast(orig_loan_am as number(38,2)) <= 400000 then 1 else 0 end) as nbr_pl_ts_3L_4L,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 300000 and try_cast(orig_loan_am as number(38,2)) <= 400000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_3L_4L_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 300000 and try_cast(orig_loan_am as number(38,2)) <= 400000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_3L_4L_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 300000 and try_cast(orig_loan_am as number(38,2)) <= 400000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_3L_4L_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 300000 and try_cast(orig_loan_am as number(38,2)) <= 400000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_3L_4L_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 300000 and try_cast(orig_loan_am as number(38,2)) <= 400000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_3L_4L_opened_l36m,
    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 400000 and try_cast(orig_loan_am as number(38,2)) <= 500000 then 1 else 0 end) as nbr_pl_ts_4L_5L,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 400000 and try_cast(orig_loan_am as number(38,2)) <= 500000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_4L_5L_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 400000 and try_cast(orig_loan_am as number(38,2)) <= 500000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_4L_5L_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 400000 and try_cast(orig_loan_am as number(38,2)) <= 500000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_4L_5L_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 400000 and try_cast(orig_loan_am as number(38,2)) <= 500000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_4L_5L_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 400000 and try_cast(orig_loan_am as number(38,2)) <= 500000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_4L_5L_opened_l36m,
    
    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 500000 and try_cast(orig_loan_am as number(38,2)) <= 700000 then 1 else 0 end) as nbr_pl_ts_5L_7L,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 500000 and try_cast(orig_loan_am as number(38,2)) <= 700000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_5L_7L_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 500000 and try_cast(orig_loan_am as number(38,2)) <= 700000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_5L_7L_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 500000 and try_cast(orig_loan_am as number(38,2)) <= 700000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_5L_7L_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 500000 and try_cast(orig_loan_am as number(38,2)) <= 700000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_5L_7L_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 500000 and try_cast(orig_loan_am as number(38,2)) <= 700000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_5L_7L_opened_l36m,
    
    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 700000 and try_cast(orig_loan_am as number(38,2)) <= 1000000 then 1 else 0 end) as nbr_pl_ts_7L_10L,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 700000 and try_cast(orig_loan_am as number(38,2)) <= 1000000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_7L_10L_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 700000 and try_cast(orig_loan_am as number(38,2)) <= 1000000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_7L_10L_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 700000 and try_cast(orig_loan_am as number(38,2)) <= 1000000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_7L_10L_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 700000 and try_cast(orig_loan_am as number(38,2)) <= 1000000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_7L_10L_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 700000 and try_cast(orig_loan_am as number(38,2)) <= 1000000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_7L_10L_opened_l36m,
    

    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 1000000 then 1 else 0 end) as nbr_pl_ts_gt10L,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 1000000 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_ts_gt10L_opened_l3m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 1000000 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_ts_gt10L_opened_l6m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 1000000 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_ts_gt10L_opened_l12m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 1000000 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_ts_gt10L_opened_l24m,
    sum(case when acct_type_cd in (123,245) and try_cast(orig_loan_am as number(38,2)) > 1000000 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_ts_gt10L_opened_l36m,

    sum(case when acct_type_cd=123 and m_sub_id='NBF' then 1 else 0 end) as nbr_pl_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and closed_dt is null then 1 else 0 end) as nbr_active_pl_nbfc,
    max(case when acct_type_cd=123 and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_pl_loan_amt_nbfc,
    max(case when acct_type_cd=123 and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as max_pl_bal_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_pl_loan_amt_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as tot_pl_bal_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_pl_loan_amt_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_pl_bal_nbfc,
    max(case when acct_type_cd=123 and m_sub_id='NBF' then datediff('month', open_dt , created_at) else 0 end) as cb_pl_tenure_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_pl_opened_l1m_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_pl_opened_l2m_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_opened_l3m_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_opened_l6m_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_pl_opened_l9m_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_opened_l12m_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_pl_opened_l18m_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_opened_l24m_nbfc,
    sum(case when acct_type_cd=123 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_opened_l36m_nbfc,
    
    max(case 
    when acct_type_cd=123 and m_sub_id='NBF' and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
    else 0 end) as max_pl_util_nbfc,


    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') then 1 else 0 end) as nbr_pl_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then 1 else 0 end) as nbr_active_pl_BANK,
    max(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_pl_loan_amt_BANK,
    max(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as max_pl_bal_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_pl_loan_amt_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as tot_pl_bal_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_pl_loan_amt_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_pl_bal_BANK,
    max(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') then datediff('month', open_dt , created_at) else 0 end) as cb_pl_tenure_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_pl_opened_l1m_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_pl_opened_l2m_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_opened_l3m_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_opened_l6m_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_pl_opened_l9m_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_opened_l12m_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_pl_opened_l18m_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_opened_l24m_BANK,
    sum(case when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_opened_l36m_BANK,
   
    max(case 
    when acct_type_cd=123 and m_sub_id in ('PVT','FOR','PUB') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
    else 0 end) as max_pl_util_BANK,



    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') then 1 else 0 end) as nbr_pl_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and closed_dt is null then 1 else 0 end) as nbr_active_pl_MFI,
    max(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_pl_loan_amt_MFI,
    max(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') then try_cast(balance_am as number(38,2)) else 0 end) as max_pl_bal_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_pl_loan_amt_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') then try_cast(balance_am as number(38,2)) else 0 end) as tot_pl_bal_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_pl_loan_amt_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_pl_bal_MFI,
    max(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') then datediff('month', open_dt , created_at) else 0 end) as cb_pl_tenure_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_pl_opened_l1m_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_pl_opened_l2m_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_opened_l3m_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_opened_l6m_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_pl_opened_l9m_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_opened_l12m_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_pl_opened_l18m_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_opened_l24m_MFI,
    sum(case when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_opened_l36m_MFI,
    
    max(case 
    when acct_type_cd=123 and m_sub_id in ('MFI','SFI') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
    else 0 end) as max_pl_util_MFI,


    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') then 1 else 0 end) as nbr_pl_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then 1 else 0 end) as nbr_active_pl_SFB,
    max(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_pl_loan_amt_SFB,
    max(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(balance_am as number(38,2)) else 0 end) as max_pl_bal_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_pl_loan_amt_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(balance_am as number(38,2)) else 0 end) as tot_pl_bal_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_pl_loan_amt_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_pl_bal_SFB,
    max(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') then datediff('month', open_dt , created_at) else 0 end) as cb_pl_tenure_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_pl_opened_l1m_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_pl_opened_l2m_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_opened_l3m_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_opened_l6m_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_pl_opened_l9m_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_opened_l12m_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_pl_opened_l18m_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_opened_l24m_SFB,
    sum(case when acct_type_cd=123 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_opened_l36m_SFB,
    
    max(case 
    when acct_type_cd=123  and m_sub_id in ('RRB','SFB','COB','COP') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
    else 0 end) as max_pl_util_SFB,

    

    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then 1 else 0 end) as nbr_pl_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then 1 else 0 end) as nbr_active_pl_OTHER,
    max(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_pl_loan_amt_OTHER,
    max(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(balance_am as number(38,2)) else 0 end) as max_pl_bal_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_pl_loan_amt_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(balance_am as number(38,2)) else 0 end) as tot_pl_bal_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_pl_loan_amt_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_pl_bal_OTHER,
    max(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then datediff('month', open_dt , created_at) else 0 end) as cb_pl_tenure_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_pl_opened_l1m_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_pl_opened_l2m_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_pl_opened_l3m_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_pl_opened_l6m_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_pl_opened_l9m_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_pl_opened_l12m_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_pl_opened_l18m_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_pl_opened_l24m_OTHER,
    sum(case when acct_type_cd=123 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_pl_opened_l36m_OTHER,
    
    max(case 
    when acct_type_cd=123  and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
    else 0 end) as max_pl_util_OTHER,




    
---------------------------------------------------------------------------------------------------------------------------------------------CC



    sum(case when acct_type_cd=5 and m_sub_id='NBF' then 1 else 0 end) as nbr_cc_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and closed_dt is null then 1 else 0 end) as nbr_active_cc_nbfc,
    max(case when acct_type_cd=5 and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as max_cc_bal_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as tot_cc_bal_nbfc,
    
    max(case 
        when acct_type_cd=5 and m_sub_id='NBF' and (try_cast(credit_limit_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(credit_limit_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id='NBF' and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id='NBF' and (try_cast(balance_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(balance_am as number(38,2)))*100
        else 0 end) as max_cc_util_nbfc,

    max(case
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_limit_nbfc,

    sum(case 
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_limit_nbfc,

    sum(case 
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_loan_amt_nbfc,

    max(case
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id='NBF' and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_loan_amt_nbfc,
        
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cc_bal_nbfc,
    max(case when acct_type_cd=5 and m_sub_id='NBF' and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as max_active_cc_bal_nbfc,
    
    

    max(case 
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_limit_nbfc,

    sum(case 
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_limit_nbfc,

     max(case 
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_loan_amt_nbfc,

    sum(case 
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id='NBF' and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_loan_amt_nbfc,


    max(case when acct_type_cd=5 and m_sub_id='NBF' then datediff('month', open_dt , created_at) else 0 end) as cb_cc_tenure_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cc_opened_l1m_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cc_opened_l2m_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cc_opened_l3m_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cc_opened_l6m_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cc_opened_l9m_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cc_opened_l12m_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cc_opened_l18m_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cc_opened_l24m_nbfc,
    sum(case when acct_type_cd=5 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cc_opened_l36m_nbfc,


     sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') then 1 else 0 end) as nbr_cc_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then 1 else 0 end) as nbr_active_cc_BANK,
    max(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as max_cc_bal_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as tot_cc_bal_BANK,
    
    max(case 
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (try_cast(credit_limit_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(credit_limit_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (try_cast(balance_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(balance_am as number(38,2)))*100
        else 0 end) as max_cc_util_BANK,

    max(case
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_limit_BANK,

    sum(case 
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_limit_BANK,

    sum(case 
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_loan_amt_BANK,

    max(case
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_loan_amt_BANK,
        
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cc_bal_BANK,
    max(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as max_active_cc_bal_BANK,
    
    

    max(case 
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_limit_BANK,

    sum(case 
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_limit_BANK,

     max(case 
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_loan_amt_BANK,

    sum(case 
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_loan_amt_BANK,

    max(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') then datediff('month', open_dt , created_at) else 0 end) as cb_cc_tenure_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cc_opened_l1m_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cc_opened_l2m_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cc_opened_l3m_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cc_opened_l6m_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cc_opened_l9m_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cc_opened_l12m_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cc_opened_l18m_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cc_opened_l24m_BANK,
    sum(case when acct_type_cd=5 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cc_opened_l36m_BANK,


    
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') then 1 else 0 end) as nbr_cc_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and closed_dt is null then 1 else 0 end) as nbr_active_cc_MFI,
    max(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') then try_cast(balance_am as number(38,2)) else 0 end) as max_cc_bal_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') then try_cast(balance_am as number(38,2)) else 0 end) as tot_cc_bal_MFI,
    
    max(case 
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (try_cast(credit_limit_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(credit_limit_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (try_cast(balance_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(balance_am as number(38,2)))*100
        else 0 end) as max_cc_util_MFI,

    max(case
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_limit_MFI,

    sum(case 
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_limit_MFI,

    sum(case 
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_loan_amt_MFI,

    max(case
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_loan_amt_MFI,
        
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cc_bal_MFI,
    max(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as max_active_cc_bal_MFI,
    
   

    max(case 
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_limit_MFI,

    sum(case 
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_limit_MFI,

     max(case 
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_loan_amt_MFI,

    sum(case 
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_loan_amt_MFI,

    max(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') then datediff('month', open_dt , created_at) else 0 end) as cb_cc_tenure_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cc_opened_l1m_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cc_opened_l2m_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cc_opened_l3m_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cc_opened_l6m_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cc_opened_l9m_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cc_opened_l12m_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cc_opened_l18m_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cc_opened_l24m_MFI,
    sum(case when acct_type_cd=5 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cc_opened_l36m_MFI,



    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') then 1 else 0 end) as nbr_cc_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then 1 else 0 end) as nbr_active_cc_SFB,
    max(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(balance_am as number(38,2)) else 0 end) as max_cc_bal_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(balance_am as number(38,2)) else 0 end) as tot_cc_bal_SFB,
    
    max(case 
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (try_cast(credit_limit_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(credit_limit_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (try_cast(balance_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(balance_am as number(38,2)))*100
        else 0 end) as max_cc_util_SFB,

    max(case
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_limit_SFB,

    sum(case 
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_limit_SFB,

    sum(case 
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_loan_amt_SFB,

    max(case
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_loan_amt_SFB,
        
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cc_bal_SFB,
    max(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as max_active_cc_bal_SFB,
    
    

    max(case 
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_limit_SFB,

    sum(case 
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_limit_SFB,

     max(case 
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_loan_amt_SFB,

    sum(case 
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_loan_amt_SFB,


    max(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') then datediff('month', open_dt , created_at) else 0 end) as cb_cc_tenure_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cc_opened_l1m_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cc_opened_l2m_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cc_opened_l3m_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cc_opened_l6m_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cc_opened_l9m_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cc_opened_l12m_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cc_opened_l18m_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cc_opened_l24m_SFB,
    sum(case when acct_type_cd=5 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cc_opened_l36m_SFB,



     sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then 1 else 0 end) as nbr_cc_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then 1 else 0 end) as nbr_active_cc_OTHER,
    max(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(balance_am as number(38,2)) else 0 end) as max_cc_bal_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(balance_am as number(38,2)) else 0 end) as tot_cc_bal_OTHER,
    
    max(case 
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (try_cast(credit_limit_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(credit_limit_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (try_cast(balance_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(balance_am as number(38,2)))*100
        else 0 end) as max_cc_util_OTHER,

    max(case
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_limit_OTHER,

    sum(case 
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_limit_OTHER,

    sum(case 
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as tot_active_cc_loan_amt_OTHER,

    max(case
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(credit_limit_am as number(38,2)) > 0) then try_cast(credit_limit_am as number(38,2))
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(orig_loan_am as number(38,2)) > 0) then try_cast(orig_loan_am as number(38,2))
        when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and (closed_dt is null and try_cast(balance_am as number(38,2)) > 0) then try_cast(balance_am as number(38,2)) 
        else 0 end) as max_active_cc_loan_amt_OTHER,
        
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cc_bal_OTHER,
    max(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as max_active_cc_bal_OTHER,
    
    

    max(case 
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_limit_OTHER,

    sum(case 
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_limit_OTHER,

     max(case 
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as max_cc_loan_amt_OTHER,

    sum(case 
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(credit_limit_am as number(38,2)) > 0 then try_cast(credit_limit_am as number(38,2))
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(orig_loan_am as number(38,2)) > 0 then try_cast(orig_loan_am as number(38,2))
         when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and try_cast(balance_am as number(38,2)) > 0 then try_cast(balance_am as number(38,2)) 
         else 0 end) as tot_cc_loan_amt_OTHER,


    max(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then datediff('month', open_dt , created_at) else 0 end) as cb_cc_tenure_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cc_opened_l1m_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cc_opened_l2m_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cc_opened_l3m_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cc_opened_l6m_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cc_opened_l9m_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cc_opened_l12m_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cc_opened_l18m_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cc_opened_l24m_OTHER,
    sum(case when acct_type_cd=5 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cc_opened_l36m_OTHER,

--------------------------------------------------------------------------------------------------------------------------------------------------------CD


    sum(case when acct_type_cd=189 and m_sub_id='NBF' then 1 else 0 end) as nbr_cd_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and closed_dt is null then 1 else 0 end) as nbr_active_cd_nbfc,
    max(case when acct_type_cd=189 and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_cd_loan_amt_nbfc,
    max(case when acct_type_cd=189 and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as max_cd_bal_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_cd_loan_amt_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as tot_cd_bal_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_cd_loan_amt_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cd_bal_nbfc,
    max(case when acct_type_cd=189 and m_sub_id='NBF' then datediff('month', open_dt , created_at) else 0 end) as cb_cd_tenure_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cd_opened_l1m_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cd_opened_l2m_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cd_opened_l3m_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cd_opened_l6m_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cd_opened_l9m_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cd_opened_l12m_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cd_opened_l18m_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cd_opened_l24m_nbfc,
    sum(case when acct_type_cd=189 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cd_opened_l36m_nbfc,

    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') then 1 else 0 end) as nbr_cd_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then 1 else 0 end) as nbr_active_cd_BANK,
    max(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_cd_loan_amt_BANK,
    max(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as max_cd_bal_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_cd_loan_amt_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as tot_cd_bal_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_cd_loan_amt_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cd_bal_BANK,
    max(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') then datediff('month', open_dt , created_at) else 0 end) as cb_cd_tenure_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cd_opened_l1m_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cd_opened_l2m_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cd_opened_l3m_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cd_opened_l6m_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cd_opened_l9m_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cd_opened_l12m_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cd_opened_l18m_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cd_opened_l24m_BANK,
    sum(case when acct_type_cd=189 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cd_opened_l36m_BANK,



    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') then 1 else 0 end) as nbr_cd_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and closed_dt is null then 1 else 0 end) as nbr_active_cd_MFI,
    max(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_cd_loan_amt_MFI,
    max(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') then try_cast(balance_am as number(38,2)) else 0 end) as max_cd_bal_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_cd_loan_amt_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') then try_cast(balance_am as number(38,2)) else 0 end) as tot_cd_bal_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_cd_loan_amt_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cd_bal_MFI,
    max(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') then datediff('month', open_dt , created_at) else 0 end) as cb_cd_tenure_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cd_opened_l1m_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cd_opened_l2m_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cd_opened_l3m_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cd_opened_l6m_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cd_opened_l9m_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cd_opened_l12m_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cd_opened_l18m_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cd_opened_l24m_MFI,
    sum(case when acct_type_cd=189 and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cd_opened_l36m_MFI,


    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') then 1 else 0 end) as nbr_cd_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then 1 else 0 end) as nbr_active_cd_SFB,
    max(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_cd_loan_amt_SFB,
    max(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(balance_am as number(38,2)) else 0 end) as max_cd_bal_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_cd_loan_amt_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(balance_am as number(38,2)) else 0 end) as tot_cd_bal_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_cd_loan_amt_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cd_bal_SFB,
    max(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') then datediff('month', open_dt , created_at) else 0 end) as cb_cd_tenure_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cd_opened_l1m_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cd_opened_l2m_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cd_opened_l3m_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cd_opened_l6m_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cd_opened_l9m_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cd_opened_l12m_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cd_opened_l18m_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cd_opened_l24m_SFB,
    sum(case when acct_type_cd=189 and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cd_opened_l36m_SFB,
    

    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then 1 else 0 end) as nbr_cd_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then 1 else 0 end) as nbr_active_cd_OTHER,
    max(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_cd_loan_amt_OTHER,
    max(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(balance_am as number(38,2)) else 0 end) as max_cd_bal_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_cd_loan_amt_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(balance_am as number(38,2)) else 0 end) as tot_cd_bal_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_cd_loan_amt_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_cd_bal_OTHER,
    max(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then datediff('month', open_dt , created_at) else 0 end) as cb_cd_tenure_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_cd_opened_l1m_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_cd_opened_l2m_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_cd_opened_l3m_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_cd_opened_l6m_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_cd_opened_l9m_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_cd_opened_l12m_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_cd_opened_l18m_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_cd_opened_l24m_OTHER,
    sum(case when acct_type_cd=189 and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_cd_opened_l36m_OTHER,


----------------------------------------------------------------------------------------------------------------------------------------------------------------------ALL ACCT

    sum(case when acct_type_cd is not null and m_sub_id='NBF' then 1 else 0 end) as nbr_acct_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and closed_dt is null then 1 else 0 end) as nbr_active_acct_nbfc,
    max(case when acct_type_cd is not null and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_acct_loan_amt_nbfc,
    max(case when acct_type_cd is not null and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as max_acct_bal_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_acct_loan_amt_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as tot_acct_bal_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_acct_loan_amt_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_acct_bal_nbfc,
    max(case when acct_type_cd is not null and m_sub_id='NBF' then datediff('month', open_dt , created_at) else 0 end) as cb_acct_tenure_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_acct_opened_l1m_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_acct_opened_l2m_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_acct_opened_l3m_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_acct_opened_l6m_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_acct_opened_l9m_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_acct_opened_l12m_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_acct_opened_l18m_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_acct_opened_l24m_nbfc,
    sum(case when acct_type_cd is not null and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_acct_opened_l36m_nbfc,

    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') then 1 else 0 end) as nbr_acct_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then 1 else 0 end) as nbr_active_acct_BANK,
    max(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_acct_loan_amt_BANK,
    max(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as max_acct_bal_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_acct_loan_amt_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as tot_acct_bal_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_acct_loan_amt_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_acct_bal_BANK,
    max(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') then datediff('month', open_dt , created_at) else 0 end) as cb_acct_tenure_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_acct_opened_l1m_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_acct_opened_l2m_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_acct_opened_l3m_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_acct_opened_l6m_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_acct_opened_l9m_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_acct_opened_l12m_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_acct_opened_l18m_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_acct_opened_l24m_BANK,
    sum(case when acct_type_cd is not null and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_acct_opened_l36m_BANK,



    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') then 1 else 0 end) as nbr_acct_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and closed_dt is null then 1 else 0 end) as nbr_active_acct_MFI,
    max(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_acct_loan_amt_MFI,
    max(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') then try_cast(balance_am as number(38,2)) else 0 end) as max_acct_bal_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_acct_loan_amt_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') then try_cast(balance_am as number(38,2)) else 0 end) as tot_acct_bal_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_acct_loan_amt_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_acct_bal_MFI,
    max(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') then datediff('month', open_dt , created_at) else 0 end) as cb_acct_tenure_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_acct_opened_l1m_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_acct_opened_l2m_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_acct_opened_l3m_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_acct_opened_l6m_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_acct_opened_l9m_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_acct_opened_l12m_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_acct_opened_l18m_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_acct_opened_l24m_MFI,
    sum(case when acct_type_cd is not null and m_sub_id in ('MFI','SFI') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_acct_opened_l36m_MFI,


    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') then 1 else 0 end) as nbr_acct_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then 1 else 0 end) as nbr_active_acct_SFB,
    max(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_acct_loan_amt_SFB,
    max(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(balance_am as number(38,2)) else 0 end) as max_acct_bal_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_acct_loan_amt_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') then try_cast(balance_am as number(38,2)) else 0 end) as tot_acct_bal_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_acct_loan_amt_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_acct_bal_SFB,
    max(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') then datediff('month', open_dt , created_at) else 0 end) as cb_acct_tenure_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_acct_opened_l1m_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_acct_opened_l2m_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_acct_opened_l3m_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_acct_opened_l6m_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_acct_opened_l9m_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_acct_opened_l12m_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_acct_opened_l18m_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_acct_opened_l24m_SFB,
    sum(case when acct_type_cd is not null and m_sub_id in ('RRB','SFB','COB','COP') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_acct_opened_l36m_SFB,
    

    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then 1 else 0 end) as nbr_acct_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then 1 else 0 end) as nbr_active_acct_OTHER,
    max(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_acct_loan_amt_OTHER,
    max(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(balance_am as number(38,2)) else 0 end) as max_acct_bal_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_acct_loan_amt_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then try_cast(balance_am as number(38,2)) else 0 end) as tot_acct_bal_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_acct_loan_amt_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_acct_bal_OTHER,
    max(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') then datediff('month', open_dt , created_at) else 0 end) as cb_acct_tenure_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_acct_opened_l1m_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_acct_opened_l2m_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_acct_opened_l3m_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_acct_opened_l6m_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_acct_opened_l9m_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_acct_opened_l12m_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_acct_opened_l18m_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_acct_opened_l24m_OTHER,
    sum(case when acct_type_cd is not null and m_sub_id not in ('RRB','SFB','COB','COP','MFI','SFI','PVT','FOR','PUB','NBF') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_acct_opened_l36m_OTHER,


    sum(case when acct_type_cd=191 and m_sub_id='NBF' then 1 else 0 end) as nbr_gld_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and closed_dt is null then 1 else 0 end) as nbr_active_gld_nbfc,
    max(case when acct_type_cd=191 and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_gld_loan_amt_nbfc,
    max(case when acct_type_cd=191 and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as max_gld_bal_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_gld_loan_amt_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as tot_gld_bal_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_gld_loan_amt_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_gld_bal_nbfc,
    max(case when acct_type_cd=191 and m_sub_id='NBF' then datediff('month', open_dt , created_at) else 0 end) as cb_gld_tenure_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_gld_opened_l1m_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_gld_opened_l2m_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_gld_opened_l3m_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_gld_opened_l6m_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_gld_opened_l9m_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_gld_opened_l12m_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_gld_opened_l18m_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_gld_opened_l24m_nbfc,
    sum(case when acct_type_cd=191 and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_gld_opened_l36m_nbfc,
   
    max(case 
        when acct_type_cd=191  and m_sub_id='NBF' and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        else 0 end) as max_gld_util_nbfc,



    
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') then 1 else 0 end) as nbr_gld_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then 1 else 0 end) as nbr_active_gld_BANK,
    max(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_gld_loan_amt_BANK,
    max(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as max_gld_bal_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_gld_loan_amt_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as tot_gld_bal_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_gld_loan_amt_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_gld_bal_BANK,
    max(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') then datediff('month', open_dt , created_at) else 0 end) as cb_gld_tenure_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_gld_opened_l1m_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_gld_opened_l2m_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_gld_opened_l3m_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_gld_opened_l6m_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_gld_opened_l9m_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_gld_opened_l12m_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_gld_opened_l18m_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_gld_opened_l24m_BANK,
    sum(case when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_gld_opened_l36m_BANK,
    
    max(case 
        when acct_type_cd=191 and m_sub_id in ('PVT','FOR','PUB') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        else 0 end) as max_gld_util_BANK,



    
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' then 1 else 0 end) as nbr_bl_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and closed_dt is null then 1 else 0 end) as nbr_active_bl_nbfc,
    max(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_bl_loan_amt_nbfc,
    max(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as max_bl_bal_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_bl_loan_amt_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' then try_cast(balance_am as number(38,2)) else 0 end) as tot_bl_bal_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_bl_loan_amt_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_bl_bal_nbfc,
    max(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' then datediff('month', open_dt , created_at) else 0 end) as cb_bl_tenure_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_bl_opened_l1m_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_bl_opened_l2m_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_bl_opened_l3m_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_bl_opened_l6m_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_bl_opened_l9m_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_bl_opened_l12m_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_bl_opened_l18m_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_bl_opened_l24m_nbfc,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id='NBF' and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_bl_opened_l36m_nbfc,
    
    max(case 
        when acct_type_cd in (175,176,241,228)  and m_sub_id='NBF' and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        else 0 end) as max_bl_util_nbfc,




    
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') then 1 else 0 end) as nbr_bl_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then 1 else 0 end) as nbr_active_bl_BANK,
    max(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_bl_loan_amt_BANK,
    max(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as max_bl_bal_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_bl_loan_amt_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') then try_cast(balance_am as number(38,2)) else 0 end) as tot_bl_bal_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_bl_loan_amt_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_bl_bal_BANK,
    max(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') then datediff('month', open_dt , created_at) else 0 end) as cb_bl_tenure_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_bl_opened_l1m_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_bl_opened_l2m_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_bl_opened_l3m_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_bl_opened_l6m_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_bl_opened_l9m_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_bl_opened_l12m_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_bl_opened_l18m_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_bl_opened_l24m_BANK,
    sum(case when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_bl_opened_l36m_BANK,
   
    max(case 
        when acct_type_cd in (175,176,241,228) and m_sub_id in ('PVT','FOR','PUB') and (try_cast(orig_loan_am as number(38,2)) > 0) then (try_cast(balance_am as number(38,2))/try_cast(orig_loan_am as number(38,2)))*100
        else 0 end) as max_bl_util_BANK,


    
    
    sum(case when acct_type_cd=173 then 1 else 0 end) as nbr_twl,
    sum(case when acct_type_cd=173 and closed_dt is null then 1 else 0 end) as nbr_active_twl,
    max(case when acct_type_cd=173 then try_cast(orig_loan_am as number(38,2)) else 0 end) as max_twl_loan_amt,
    max(case when acct_type_cd=173 then try_cast(balance_am as number(38,2)) else 0 end) as max_twl_bal,
    sum(case when acct_type_cd=173 then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_twl_loan_amt,
    sum(case when acct_type_cd=173 then try_cast(balance_am as number(38,2)) else 0 end) as tot_twl_bal,
    sum(case when acct_type_cd=173 and closed_dt is null then try_cast(orig_loan_am as number(38,2)) else 0 end) as tot_active_twl_loan_amt,
    sum(case when acct_type_cd=173 and closed_dt is null then try_cast(balance_am as number(38,2)) else 0 end) as tot_active_twl_bal,
    max(case when acct_type_cd=173 then datediff('month', open_dt , created_at) else 0 end) as cb_twl_tenure,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=1 then 1 else 0 end) as nbr_twl_opened_l1m,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=2 then 1 else 0 end) as nbr_twl_opened_l2m,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=3 then 1 else 0 end) as nbr_twl_opened_l3m,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=6 then 1 else 0 end) as nbr_twl_opened_l6m,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=9 then 1 else 0 end) as nbr_twl_opened_l9m,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=12 then 1 else 0 end) as nbr_twl_opened_l12m,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=18 then 1 else 0 end) as nbr_twl_opened_l18m,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=24 then 1 else 0 end) as nbr_twl_opened_l24m,
    sum(case when acct_type_cd=173 and datediff('month', open_dt, created_at) <=36 then 1 else 0 end) as nbr_twl_opened_l36m,

    max(case when acct_type_cd is not null then datediff('month', open_dt , created_at) else 0 end) as credit_bureau_tenure,
from {config_btk.database}.{config_btk.schema}.ar  
where customer_id in (select customer_id from base )
group by 1))),

enq as (select customer_id, 
    sum(case when inq_purp_cd=13 then 1 else 0 end) as nbr_pl_enquiry_ever,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=1 then 1 else 0 end) as nbr_pl_enquiry_l1m,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=2 then 1 else 0 end) as nbr_pl_enquiry_l2m,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=3 then 1 else 0 end) as nbr_pl_enquiry_l3m,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=6 then 1 else 0 end) as nbr_pl_enquiry_l6m,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=9 then 1 else 0 end) as nbr_pl_enquiry_l9m,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=12 then 1 else 0 end) as nbr_pl_enquiry_l12m,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=18 then 1 else 0 end) as nbr_pl_enquiry_l18m,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=24 then 1 else 0 end) as nbr_pl_enquiry_l24m,
    sum(case when inq_purp_cd=13 and datediff('month', inq_date, created_at) <=36 then 1 else 0 end) as nbr_pl_enquiry_l36m,

    sum(case when inq_purp_cd=18 then 1 else 0 end) as nbr_cd_enquiry_ever,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=1 then 1 else 0 end) as nbr_cd_enquiry_l1m,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=2 then 1 else 0 end) as nbr_cd_enquiry_l2m,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=3 then 1 else 0 end) as nbr_cd_enquiry_l3m,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=6 then 1 else 0 end) as nbr_cd_enquiry_l6m,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=9 then 1 else 0 end) as nbr_cd_enquiry_l9m,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=12 then 1 else 0 end) as nbr_cd_enquiry_l12m,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=18 then 1 else 0 end) as nbr_cd_enquiry_l18m,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=24 then 1 else 0 end) as nbr_cd_enquiry_l24m,
    sum(case when inq_purp_cd=18 and datediff('month', inq_date, created_at) <=36 then 1 else 0 end) as nbr_cd_enquiry_l36m,

    sum(case when inq_purp_cd=3 then 1 else 0 end) as nbr_bl_enquiry_ever,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=1 then 1 else 0 end) as nbr_bl_enquiry_l1m,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=2 then 1 else 0 end) as nbr_bl_enquiry_l2m,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=3 then 1 else 0 end) as nbr_bl_enquiry_l3m,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=6 then 1 else 0 end) as nbr_bl_enquiry_l6m,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=9 then 1 else 0 end) as nbr_bl_enquiry_l9m,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=12 then 1 else 0 end) as nbr_bl_enquiry_l12m,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=18 then 1 else 0 end) as nbr_bl_enquiry_l18m,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=24 then 1 else 0 end) as nbr_bl_enquiry_l24m,
    sum(case when inq_purp_cd=3 and datediff('month', inq_date, created_at) <=36 then 1 else 0 end) as nbr_bl_enquiry_l36m,

    sum(case when inq_purp_cd=14 then 1 else 0 end) as nbr_lap_enquiry_ever,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=1 then 1 else 0 end) as nbr_lap_enquiry_l1m,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=2 then 1 else 0 end) as nbr_lap_enquiry_l2m,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=3 then 1 else 0 end) as nbr_lap_enquiry_l3m,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=6 then 1 else 0 end) as nbr_lap_enquiry_l6m,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=9 then 1 else 0 end) as nbr_lap_enquiry_l9m,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=12 then 1 else 0 end) as nbr_lap_enquiry_l12m,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=18 then 1 else 0 end) as nbr_lap_enquiry_l18m,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=24 then 1 else 0 end) as nbr_lap_enquiry_l24m,
    sum(case when inq_purp_cd=14 and datediff('month', inq_date, created_at) <=36 then 1 else 0 end) as nbr_lap_enquiry_l36m,

    sum(case when inq_purp_cd=2 then 1 else 0 end) as nbr_atl_enquiry_ever,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=1 then 1 else 0 end) as nbr_atl_enquiry_l1m,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=2 then 1 else 0 end) as nbr_atl_enquiry_l2m,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=3 then 1 else 0 end) as nbr_atl_enquiry_l3m,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=6 then 1 else 0 end) as nbr_atl_enquiry_l6m,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=9 then 1 else 0 end) as nbr_atl_enquiry_l9m,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=12 then 1 else 0 end) as nbr_atl_enquiry_l12m,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=18 then 1 else 0 end) as nbr_atl_enquiry_l18m,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=24 then 1 else 0 end) as nbr_atl_enquiry_l24m,
    sum(case when inq_purp_cd=2 and datediff('month', inq_date, created_at) <=36 then 1 else 0 end) as nbr_atl_enquiry_l36m,

    sum(case when inq_purp_cd=16 then 1 else 0 end) as nbr_tw_enquiry_ever,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=1 then 1 else 0 end) as nbr_tw_enquiry_l1m,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=2 then 1 else 0 end) as nbr_tw_enquiry_l2m,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=3 then 1 else 0 end) as nbr_tw_enquiry_l3m,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=6 then 1 else 0 end) as nbr_tw_enquiry_l6m,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=9 then 1 else 0 end) as nbr_tw_enquiry_l9m,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=12 then 1 else 0 end) as nbr_tw_enquiry_l12m,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=18 then 1 else 0 end) as nbr_tw_enquiry_l18m,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=24 then 1 else 0 end) as nbr_tw_enquiry_l24m,
    sum(case when inq_purp_cd=16 and datediff('month', inq_date, created_at) <=36 then 1 else 0 end) as nbr_tw_enquiry_l36m,

    sum(case when inq_purp_cd in (7,13,18) then 1 else 0 end) as nbr_unsecured_enquiry_ever,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=1 then 1 else 0 end) as nbr_unsecured_enquiry_l1m,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=2 then 1 else 0 end) as nbr_unsecured_enquiry_l2m,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=3 then 1 else 0 end) as nbr_unsecured_enquiry_l3m,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=6 then 1 else 0 end) as nbr_unsecured_enquiry_l6m,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=9 then 1 else 0 end) as nbr_unsecured_enquiry_l9m,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=12 then 1 else 0 end) as nbr_unsecured_enquiry_l12m,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=18 then 1 else 0 end) as nbr_unsecured_enquiry_l18m,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=24 then 1 else 0 end) as nbr_unsecured_enquiry_l24m,
    sum(case when inq_purp_cd in (7,13,18) and datediff('month', inq_date, created_at) <=36 then 1 else 0 end) as nbr_unsecured_enquiry_l36m,
    
from {config_btk.database}.{config_btk.schema}.enq 
where customer_id in (select customer_id from base )
group by 1),


dpd as (select customer_id ,
 max(greatest(DAYS_PAST_DUE_01)) as max_dpd_l1m_all,
    max(greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03)) as max_dpd_l3m_all,
    
    max(greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06)) as max_dpd_l6m_all,
    
    max(greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06,
                DAYS_PAST_DUE_07,DAYS_PAST_DUE_08,DAYS_PAST_DUE_09,DAYS_PAST_DUE_10,DAYS_PAST_DUE_11,DAYS_PAST_DUE_12)) as max_dpd_l12m_all,
                                                
    max(greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06,
                DAYS_PAST_DUE_07,DAYS_PAST_DUE_08,DAYS_PAST_DUE_09,DAYS_PAST_DUE_10,DAYS_PAST_DUE_11,DAYS_PAST_DUE_12,
                DAYS_PAST_DUE_13,DAYS_PAST_DUE_14,DAYS_PAST_DUE_15,DAYS_PAST_DUE_16,DAYS_PAST_DUE_17,DAYS_PAST_DUE_18)) as max_dpd_l18m_all,
                                                
    max(greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06,
                DAYS_PAST_DUE_07,DAYS_PAST_DUE_08,DAYS_PAST_DUE_09,DAYS_PAST_DUE_10,DAYS_PAST_DUE_11,DAYS_PAST_DUE_12,
                DAYS_PAST_DUE_13,DAYS_PAST_DUE_14,DAYS_PAST_DUE_15,DAYS_PAST_DUE_16,DAYS_PAST_DUE_17,DAYS_PAST_DUE_18,
                DAYS_PAST_DUE_19,DAYS_PAST_DUE_20,DAYS_PAST_DUE_21,DAYS_PAST_DUE_22,DAYS_PAST_DUE_23,DAYS_PAST_DUE_24)) as max_dpd_l24m_all,
                                                
    max(greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06,
                DAYS_PAST_DUE_07,DAYS_PAST_DUE_08,DAYS_PAST_DUE_09,DAYS_PAST_DUE_10,DAYS_PAST_DUE_11,DAYS_PAST_DUE_12,
                DAYS_PAST_DUE_13,DAYS_PAST_DUE_14,DAYS_PAST_DUE_15,DAYS_PAST_DUE_16,DAYS_PAST_DUE_17,DAYS_PAST_DUE_18,
                DAYS_PAST_DUE_19,DAYS_PAST_DUE_20,DAYS_PAST_DUE_21,DAYS_PAST_DUE_22,DAYS_PAST_DUE_23,DAYS_PAST_DUE_24,
                DAYS_PAST_DUE_25,DAYS_PAST_DUE_26,DAYS_PAST_DUE_27,DAYS_PAST_DUE_28,DAYS_PAST_DUE_29,DAYS_PAST_DUE_30,
                DAYS_PAST_DUE_31,DAYS_PAST_DUE_32,DAYS_PAST_DUE_33,DAYS_PAST_DUE_34,DAYS_PAST_DUE_35,DAYS_PAST_DUE_36)) as max_dpd_l36m_all,

    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then greatest(DAYS_PAST_DUE_01) else 0 end) as max_dpd_l1m_secured_acct,
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03) else 0 end) as max_dpd_l3m_secured_acct,
    
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06) else 0 end) as max_dpd_l6m_secured_acct,
    
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06,
                DAYS_PAST_DUE_07,DAYS_PAST_DUE_08,DAYS_PAST_DUE_09,DAYS_PAST_DUE_10,DAYS_PAST_DUE_11,DAYS_PAST_DUE_12) else 0 end) as max_dpd_l12m_secured_acct,
                                                
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06,
                DAYS_PAST_DUE_07,DAYS_PAST_DUE_08,DAYS_PAST_DUE_09,DAYS_PAST_DUE_10,DAYS_PAST_DUE_11,DAYS_PAST_DUE_12,
                DAYS_PAST_DUE_13,DAYS_PAST_DUE_14,DAYS_PAST_DUE_15,DAYS_PAST_DUE_16,DAYS_PAST_DUE_17,DAYS_PAST_DUE_18) else 0 end) as max_dpd_l18m_secured_acct,
                                                
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06,
                DAYS_PAST_DUE_07,DAYS_PAST_DUE_08,DAYS_PAST_DUE_09,DAYS_PAST_DUE_10,DAYS_PAST_DUE_11,DAYS_PAST_DUE_12,
                DAYS_PAST_DUE_13,DAYS_PAST_DUE_14,DAYS_PAST_DUE_15,DAYS_PAST_DUE_16,DAYS_PAST_DUE_17,DAYS_PAST_DUE_18,
                DAYS_PAST_DUE_19,DAYS_PAST_DUE_20,DAYS_PAST_DUE_21,DAYS_PAST_DUE_22,DAYS_PAST_DUE_23,DAYS_PAST_DUE_24) else 0 end) as max_dpd_l24m_secured_acct,
                                                
    max(case when acct_type_cd in (47,58,168,172,173,175,184,185, 191,195,246,248,220,221,241,243) then greatest(DAYS_PAST_DUE_01,DAYS_PAST_DUE_02,DAYS_PAST_DUE_03,DAYS_PAST_DUE_04,DAYS_PAST_DUE_05,DAYS_PAST_DUE_06,
                DAYS_PAST_DUE_07,DAYS_PAST_DUE_08,DAYS_PAST_DUE_09,DAYS_PAST_DUE_10,DAYS_PAST_DUE_11,DAYS_PAST_DUE_12,
                DAYS_PAST_DUE_13,DAYS_PAST_DUE_14,DAYS_PAST_DUE_15,DAYS_PAST_DUE_16,DAYS_PAST_DUE_17,DAYS_PAST_DUE_18,
                DAYS_PAST_DUE_19,DAYS_PAST_DUE_20,DAYS_PAST_DUE_21,DAYS_PAST_DUE_22,DAYS_PAST_DUE_23,DAYS_PAST_DUE_24,
                DAYS_PAST_DUE_25,DAYS_PAST_DUE_26,DAYS_PAST_DUE_27,DAYS_PAST_DUE_28,DAYS_PAST_DUE_29,DAYS_PAST_DUE_30,
                DAYS_PAST_DUE_31,DAYS_PAST_DUE_32,DAYS_PAST_DUE_33,DAYS_PAST_DUE_34,DAYS_PAST_DUE_35,DAYS_PAST_DUE_36) else 0 end) as max_dpd_l36m_secured_acct,
                
from (select customer_id, acct_type_cd,
 CASE WHEN DAYS_PAST_DUE_01 is NULL then -999 else DAYS_PAST_DUE_01 end as DAYS_PAST_DUE_01,
 CASE WHEN DAYS_PAST_DUE_02 is NULL then -999 else DAYS_PAST_DUE_02 end as DAYS_PAST_DUE_02,
 CASE WHEN DAYS_PAST_DUE_03 is NULL then -999 else DAYS_PAST_DUE_03 end as DAYS_PAST_DUE_03,
 CASE WHEN DAYS_PAST_DUE_04 is NULL then -999 else DAYS_PAST_DUE_04 end as DAYS_PAST_DUE_04,
 CASE WHEN DAYS_PAST_DUE_05 is NULL then -999 else DAYS_PAST_DUE_05 end as DAYS_PAST_DUE_05,
 CASE WHEN DAYS_PAST_DUE_06 is NULL then -999 else DAYS_PAST_DUE_06 end as DAYS_PAST_DUE_06,
 CASE WHEN DAYS_PAST_DUE_07 is NULL then -999 else DAYS_PAST_DUE_07 end as DAYS_PAST_DUE_07,
 CASE WHEN DAYS_PAST_DUE_08 is NULL then -999 else DAYS_PAST_DUE_08 end as DAYS_PAST_DUE_08,
 CASE WHEN DAYS_PAST_DUE_09 is NULL then -999 else DAYS_PAST_DUE_09 end as DAYS_PAST_DUE_09,
 CASE WHEN DAYS_PAST_DUE_10 is NULL then -999 else DAYS_PAST_DUE_10 end as DAYS_PAST_DUE_10,
 CASE WHEN DAYS_PAST_DUE_11 is NULL then -999 else DAYS_PAST_DUE_11 end as DAYS_PAST_DUE_11,
 CASE WHEN DAYS_PAST_DUE_12 is NULL then -999 else DAYS_PAST_DUE_12 end as DAYS_PAST_DUE_12,
 CASE WHEN DAYS_PAST_DUE_13 is NULL then -999 else DAYS_PAST_DUE_13 end as DAYS_PAST_DUE_13,
 CASE WHEN DAYS_PAST_DUE_14 is NULL then -999 else DAYS_PAST_DUE_14 end as DAYS_PAST_DUE_14,
 CASE WHEN DAYS_PAST_DUE_15 is NULL then -999 else DAYS_PAST_DUE_15 end as DAYS_PAST_DUE_15,
 CASE WHEN DAYS_PAST_DUE_16 is NULL then -999 else DAYS_PAST_DUE_16 end as DAYS_PAST_DUE_16,
 CASE WHEN DAYS_PAST_DUE_17 is NULL then -999 else DAYS_PAST_DUE_17 end as DAYS_PAST_DUE_17,
 CASE WHEN DAYS_PAST_DUE_18 is NULL then -999 else DAYS_PAST_DUE_18 end as DAYS_PAST_DUE_18,
 CASE WHEN DAYS_PAST_DUE_19 is NULL then -999 else DAYS_PAST_DUE_19 end as DAYS_PAST_DUE_19,
 CASE WHEN DAYS_PAST_DUE_20 is NULL then -999 else DAYS_PAST_DUE_20 end as DAYS_PAST_DUE_20,
 CASE WHEN DAYS_PAST_DUE_21 is NULL then -999 else DAYS_PAST_DUE_21 end as DAYS_PAST_DUE_21,
 CASE WHEN DAYS_PAST_DUE_22 is NULL then -999 else DAYS_PAST_DUE_22 end as DAYS_PAST_DUE_22,
 CASE WHEN DAYS_PAST_DUE_23 is NULL then -999 else DAYS_PAST_DUE_23 end as DAYS_PAST_DUE_23,
 CASE WHEN DAYS_PAST_DUE_24 is NULL then -999 else DAYS_PAST_DUE_24 end as DAYS_PAST_DUE_24,
 CASE WHEN DAYS_PAST_DUE_25 is NULL then -999 else DAYS_PAST_DUE_25 end as DAYS_PAST_DUE_25,
 CASE WHEN DAYS_PAST_DUE_26 is NULL then -999 else DAYS_PAST_DUE_26 end as DAYS_PAST_DUE_26,
 CASE WHEN DAYS_PAST_DUE_27 is NULL then -999 else DAYS_PAST_DUE_27 end as DAYS_PAST_DUE_27,
 CASE WHEN DAYS_PAST_DUE_28 is NULL then -999 else DAYS_PAST_DUE_28 end as DAYS_PAST_DUE_28,
 CASE WHEN DAYS_PAST_DUE_29 is NULL then -999 else DAYS_PAST_DUE_29 end as DAYS_PAST_DUE_29,
 CASE WHEN DAYS_PAST_DUE_30 is NULL then -999 else DAYS_PAST_DUE_30 end as DAYS_PAST_DUE_30,
 CASE WHEN DAYS_PAST_DUE_31 is NULL then -999 else DAYS_PAST_DUE_31 end as DAYS_PAST_DUE_31,
 CASE WHEN DAYS_PAST_DUE_32 is NULL then -999 else DAYS_PAST_DUE_32 end as DAYS_PAST_DUE_32,
 CASE WHEN DAYS_PAST_DUE_33 is NULL then -999 else DAYS_PAST_DUE_33 end as DAYS_PAST_DUE_33,
 CASE WHEN DAYS_PAST_DUE_34 is NULL then -999 else DAYS_PAST_DUE_34 end as DAYS_PAST_DUE_34,
 CASE WHEN DAYS_PAST_DUE_35 is NULL then -999 else DAYS_PAST_DUE_35 end as DAYS_PAST_DUE_35,
 CASE WHEN DAYS_PAST_DUE_36 is NULL then -999 else DAYS_PAST_DUE_36 end as DAYS_PAST_DUE_36
from {config_btk.database}.{config_btk.schema}.ar) 
where customer_id in (select customer_id from base )
 group by 1)


select 
b.*,
a.* exclude customer_id,
q.* exclude customer_id,
d.* exclude customer_id,CASE
-- CAT-A conditions
    WHEN score >= 760 
         AND credit_bureau_tenure >= 48 
         AND nbr_active_cc >= 1 AND max_cc_loan_amt >= 100000
         AND nbr_cc >= 1 AND current_cc_util < 80
         AND max_dpd_l36m_all <= 10
         AND nbr_pl_opened_l6m <= 0 THEN 'CAT-A1'

    WHEN score >= 760
         AND credit_bureau_tenure >= 48
         AND max_hl_loan_amt >= 4000000
         AND cb_hl_tenure >= 12
         AND max_dpd_l36m_all <= 10
         AND max_dpd_l24m_all <= 0
         AND nbr_pl_opened_l12m <= 0 THEN 'CAT-A2'

    WHEN score >= 740
         AND credit_bureau_tenure >= 36
         AND nbr_active_cc >= 1 AND max_cc_loan_amt >= 100000
         AND nbr_cc >= 1 AND current_cc_util < 80
         AND max_dpd_l36m_all <= 10
         AND nbr_pl_opened_l6m <= 0 THEN 'CAT-A3'

    WHEN score >= 740
         AND credit_bureau_tenure >= 36
         AND max_hl_loan_amt >= 3000000
         AND cb_hl_tenure >= 12
         AND max_dpd_l36m_all <= 10
         AND max_dpd_l24m_all <= 0
         AND nbr_pl_opened_l12m <= 0 THEN 'CAT-A4'

    -- CAT-B conditions
    WHEN score >= 760
         AND credit_bureau_tenure >= 48
         AND max_pl_loan_amt >= 250000
         AND nbr_pl >= 1 AND current_pl_util <= 50
         AND max_dpd_l36m_all <= 10
         AND max_dpd_l24m_all <= 0
         AND nbr_pl_opened_l6m <= 0 THEN 'CAT-B1'

    WHEN score >= 740
         AND credit_bureau_tenure >= 36
         AND max_pl_loan_amt >= 200000
         AND nbr_pl >= 1 AND current_pl_util <= 50
         AND max_dpd_l36m_all <= 10
         AND max_dpd_l24m_all <= 0
         AND nbr_pl_opened_l6m <= 0 THEN 'CAT-B2'

    WHEN score >= 720
         AND credit_bureau_tenure >= 24
         AND nbr_active_cc >= 1 AND max_cc_loan_amt >= 100000
         AND nbr_cc >= 1 AND current_cc_util < 80
         AND max_dpd_l24m_all <= 10
         AND nbr_pl_opened_l3m <= 0 THEN 'CAT-B3'

    -- CAT-C conditions
    WHEN score >= 720
         AND credit_bureau_tenure >= 24
         AND max_pl_loan_amt >= 150000
         AND nbr_pl >= 1 AND current_pl_util <= 50
         AND max_dpd_l24m_all <= 10
         AND max_dpd_l12m_all <= 0
         AND nbr_pl_opened_l3m <= 0 THEN 'CAT-C1'

    WHEN score >= 720
         AND credit_bureau_tenure >= 24
         AND max_hl_loan_amt >= 2000000
         AND cb_hl_tenure >= 6
         AND max_dpd_l24m_all <= 10
         AND max_dpd_l12m_all <= 0
         AND nbr_pl_opened_l6m <= 0 THEN 'CAT-C2'

    WHEN score >= 700
         AND credit_bureau_tenure >= 12
         AND nbr_active_cc >= 1 AND max_cc_loan_amt >= 50000
         AND nbr_cc >= 1 AND current_cc_util < 80
         AND max_dpd_l12m_all <= 10
         AND nbr_pl_opened_l3m <= 0 THEN 'CAT-C3'

    WHEN score >= 700
         AND credit_bureau_tenure >= 12
         AND max_pl_loan_amt >= 75000
         AND max_dpd_l12m_all <= 10
         AND nbr_pl_opened_l3m <= 0 THEN 'CAT-C4'

         WHEN score >= 700
         AND credit_bureau_tenure >= 12
         AND nbr_active_cc >= 1 AND max_cc_loan_amt >= 50000
         AND nbr_cc >= 1 AND current_cc_util < 80
         AND max_dpd_l12m_all <= 10 THEN 'CAT-C5'


    -- CAT-D conditions
    WHEN score >= 700
         AND credit_bureau_tenure >= 12
         AND max_pl_loan_amt >= 75000
         AND max_dpd_l12m_all <= 10
         AND max_dpd_l12m_all <= 0 THEN 'CAT-D1'

    when score>=10
    then'CAT-D2'
    else 
    'NO-HIT'
END AS customer_category
from   base b
left join acct a on b.customer_id=a.customer_id
left join enq q on b.customer_id=q.customer_id
left join dpd d on d.customer_id=b.customer_id




            """
