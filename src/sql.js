export default {
    clearRunFromArchive: 'DELETE FROM arch_files WHERE run_id = $1 AND run_version_id = $2 AND doc_type = \'PAY\'',
    checkTasks: `SELECT task_id, run_id, run_version FROM calc_tasks WHERE status='GENR' AND payslip_exists = false
     ORDER BY task_id LIMIT 1`,
    writeToArchive: `INSERT INTO arch_files(ee_id, run_id, run_version_id, wc_id, le_id, contract_id,
                    file_name, file_path, doc_type) VALUES `,
    finishTask: 'UPDATE calc_tasks SET payslip_exists = true WHERE task_id = $1',
    getStaticFields: `SELECT position_x, position_y, type, null as VALUE FROM payslip_data_positioning pdp
JOIN payslip_layout_le pll ON 
pll.payslip_layout_id = pdp.payslip_layout_id OR pll.payslip_layout_term_id = pdp.payslip_layout_id
JOIN payslip_pictures pp ON pp.le_id = pll.le_id`,
    getConfigurationData: `SELECT DISTINCT co.payslip_id, ec.le_id, ec.ee_id, ec.wc_id , co.run_id, co.run_version,
CASE 
WHEN ec.termination = TRUE THEN pll.payslip_layout_term_id
ELSE pll.payslip_layout_id END as payslip_layout_id, ec.contract_id, ec.termination
    FROM calculation_output co
JOIN calc_tasks ct ON ct.run_id = co.run_id AND ct.run_version = co.run_version
JOIN ee_contract ec ON ec.contract_id = co.contract_id
JOIN payslip_layout_le pll ON pll.le_id = ec.le_id
WHERE ct.task_id = $1
ORDER BY payslip_id`,
    test: `SELECT * FROM (
SELECT field, position_x, null as position_y, position_y_start,
font, alignment, size, type, null as position_x_end, null as position_y_end, row_height, wt_sequence
  FROM (
SELECT
CASE 
WHEN pw.amount_convert_to_percent = false AND (co.wt_id NOT BETWEEN 83 AND 124)
THEN to_char(SUM(DISTINCT co.amount), '999,999.' || repeat('9', pw.amount_format_decimals))
WHEN pw.amount_convert_to_percent = false AND (co.wt_id BETWEEN 83 AND 124)
THEN to_char(SUM(DISTINCT co.amount) / 2, '999,999.' || repeat('9', pw.amount_format_decimals))
ELSE to_char(SUM(DISTINCT co.amount), '999,999.' || repeat('9', pw.amount_format_decimals) || '%') 
END as field, pw.amount_alignment as alignment, pw.amount_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = $6 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND co.amount != 0 AND co.amount IS NOT NULL
GROUP BY pw.amount_convert_to_percent, pw.amount_alignment, pw.amount_position_x, pdr.position_y_start, 
pw.font, pdr.size, pdr.row_height
,pw.amount_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
CASE 
WHEN pw.base_convert_to_percent = false AND (co.wt_id NOT BETWEEN 83 AND 124)
THEN to_char(SUM(DISTINCT co.base), '999,999.' || repeat('9', pw.base_format_decimals))
WHEN pw.base_convert_to_percent = false AND (co.wt_id BETWEEN 83 AND 124)
THEN to_char(SUM(DISTINCT co.base) / 2, '999,999.' || repeat('9', pw.base_format_decimals))
ELSE to_char(SUM(DISTINCT co.base), '999,999.' || repeat('9', pw.base_format_decimals) || '%') 
END as field, pw.base_alignment as alignment, pw.base_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = $6 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND co.base != 0 AND co.base IS NOT NULL
GROUP BY pw.base_convert_to_percent, pw.base_alignment, pw.base_position_x, pdr.position_y_start, 
pw.font, pdr.size, pdr.row_height
,pw.base_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
CASE 
WHEN pw.rate_convert_to_percent = false AND (co.wt_id NOT BETWEEN 83 AND 124)
THEN to_char(SUM(DISTINCT co.rate), '999,9990.' || repeat('9', pw.rate_format_decimals))
WHEN pw.rate_convert_to_percent = false AND (co.wt_id BETWEEN 83 AND 124)
THEN to_char(SUM(DISTINCT co.rate) / 2, '999,9990.' || repeat('9', pw.rate_format_decimals))
ELSE to_char(SUM(DISTINCT co.rate), '999,9990.' || repeat('9', pw.rate_format_decimals) || '%') 
END as field, pw.rate_alignment as alignment, pw.rate_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = $6 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND co.rate != 0 AND co.rate IS NOT NULL
GROUP BY pw.rate_convert_to_percent, pw.rate_alignment, pw.rate_position_x, pdr.position_y_start, 
pw.font, pdr.size, pdr.row_height
,pw.rate_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
wt.payslip_text as field, pw.text_alignment as alignment, pw.text_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = $6 AND pdr.position_id = pw.position_id
JOIN wt_text wt ON wt.wt_id = pw.wt_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3
GROUP BY wt.payslip_text, pw.text_alignment, pw.text_position_x, pdr.position_y_start, pw.font, 
pdr.size, pdr.row_height,
co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
null as field, pw.ref_alignment as alignment, pw.ref_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = $6 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND co.ref_period_id is not null
GROUP BY  pw.ref_alignment, pw.ref_position_x, pdr.position_y_start, pw.font, pdr.size, pdr.row_height,
co.wt_id,  pw.wt_sequence, co.ref_period_id
) AS st 
UNION ALL
SELECT
CASE
WHEN field = 'payslip_date' THEN TO_CHAR((SELECT payslip_date FROM payroll_runs WHERE run_id = $2 
AND run_version = $3 AND le_id = $5), (SELECT date_format FROM payslip_layout_le WHERE le_id = $5))
WHEN field = 'ee_id_text' THEN (SELECT ee_id_text FROM ee_id_le WHERE ee_id = $4 AND le_id = $5)
WHEN field = 'ss_id' THEN (SELECT ss_id FROM ee_id_universal_data WHERE ee_id = $4)
WHEN field = 'ee_name' THEN (SELECT CASE WHEN last_name2 IS NOT NULL THEN
                last_name || ' ' || last_name2 || ', ' || first_name 
                ELSE last_name || ' ' || first_name END FROM ee_id_universal_data WHERE ee_id = $4)
WHEN field = 'address_line1_ee' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE (now() BETWEEN ea.from_date AND ea.to_date) AND ec.ee_id = $4 AND ec.le_id = $5)
WHEN field = 'address_line1_le' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id WHERE le_id = $5)
WHEN field = 'address_zip_city' THEN (SELECT DISTINCT ad.address_zip_code || ' ' || ad.address_city  
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE (now() BETWEEN ea.from_date AND ea.to_date) AND co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = $4 AND ec.le_id = $5
)
WHEN field = 'address_country' THEN (SELECT DISTINCT ad.address_country 
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE (now() BETWEEN ea.from_date AND ea.to_date) AND co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = $4 AND ec.le_id = $5 
)
WHEN field = 'periods_code' THEN (SELECT p.code FROM periods p JOIN payroll_runs pr ON pr.period_id = p.period_id
WHERE pr.run_id = $2 AND pr.run_version = $3 AND pr.le_id = $5)
WHEN field = 'payment_date' THEN TO_CHAR((SELECT payment_date FROM payroll_runs WHERE run_id = $2 
AND run_version = $3 AND le_id = $5), (SELECT date_format FROM payslip_layout_le WHERE le_id = $5))
WHEN field = 'banks_bank_iban' THEN (SELECT DISTINCT b.bank || ' ' || b.iban
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE (now() BETWEEN eb.from_date AND eb.to_date) AND  co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = $4 AND ec.le_id = $5 
LIMIT 1)

WHEN (SELECT SUBSTRING(field, 1, 4)) = 'tag_' THEN (SELECT DISTINCT ptt.tag_translation FROM tags_translation ptt 
JOIN ee_id_le eil ON eil.leg_language = ptt.language_id
WHERE tag_id = (SELECT SUBSTRING(field, 5, length(field))::int) AND eil.ee_id = $4 AND eil.le_id = $5)

WHEN field='wt_146' THEN (SELECT to_char(SUM(co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 146 AND co.payslip_id = $1
AND run_id = $2 AND run_version = $3 GROUP BY pw.amount_format_decimals )::text

WHEN field='wt_162' THEN (SELECT to_char(co.amount, '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 162 AND co.payslip_id = $1
AND run_id = $2 AND run_version = $3 )::text


WHEN field='wt_51' THEN (SELECT to_char(SUM(co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 51 AND co.payslip_id = $1
AND run_id = $2 AND run_version = $3 GROUP BY pw.amount_format_decimals )::text
WHEN field='text_wt_51' THEN (SELECT payslip_text FROM wt_text WHERE wt_id = 51 AND
lang_id = (SELECT leg_language FROM ee_id_le WHERE ee_id = $4 AND le_id = $5))
WHEN field='le_name' THEN (SELECT le_name FROM legal_entity WHERE le_id = $5)
WHEN field='address_zip_region_city' THEN (SELECT ad.address_zip_code || ' ' || ad.address_region || ' (' || ad.address_city || ')' 
FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id
WHERE le.le_id = $5 AND (NOW() BETWEEN le.from_date AND le.to_date))
WHEN field='tax_id_le' THEN (SELECT le.tax_id  FROM legal_entity le WHERE le.le_id = $5 AND (NOW() BETWEEN le.from_date AND le.to_date))
WHEN field='ccc' THEN (SELECT wc.ccc FROM work_center wc 
JOIN ee_contract ec ON ec.wc_id = wc.wc_id AND ec.le_id = wc.le_id 
WHERE ec.ee_id = $4 AND ec.le_id = $5)
WHEN field='seniority_date' THEN (SELECT TO_CHAR(ec.seniority_date, pll.date_format) 
FROM ee_contract ec
JOIN payslip_layout_le pll ON pll.le_id = ec.le_id
WHERE ec.le_id = $5 AND ec.ee_id = $4)
WHEN field='contract_type' THEN (SELECT ec.contract_type from ee_contract ec 
WHERE ec.le_id = $5 AND ec.ee_id = $4)
WHEN field='ss_group' THEN (SELECT ec.ss_group from ee_contract ec WHERE ec.le_id = $5 AND ec.ee_id = $4)::text
WHEN field='tax_id_ee' THEN (SELECT tax_id FROM ee_id_universal_data
WHERE ee_id = $4)
WHEN field='wc_name' THEN (
SELECT wc.wc_name FROM work_center wc 
WHERE wc.wc_id = 1 AND wc.le_id = $5 AND (NOW() BETWEEN wc.from_date AND wc.to_date))
WHEN field='lev_sub_name' THEN (SELECT cal.lev_sub_name FROM coll_agree_lev cal 
LEFT JOIN ee_contract ec ON ec.coll_id = cal.coll_id AND ec.coll_version = cal.coll_version AND ec.level = cal.level 
AND ec.ss_h_d_m = cal.ss_h_d_m AND (ec.sublevel = cal.sublevel OR cal.sublevel = 0)
WHERE ec.ee_id = $4 AND ec.le_id = $5)
WHEN field='desde_hasta' THEN (SELECT 'desde ' || TO_CHAR(min(pso.from_date), pll.date_format) || ' , hasta ' || TO_CHAR(max(pso.to_date),pll.date_format)
FROM pay_subperiod_output pso 
JOIN calculation_output co ON co.contract_id = pso.contract_id AND co.run_id = pso.run_id AND co.run_version = pso.run_version 
JOIN ee_contract ec ON ec.contract_id = pso.contract_id JOIN payslip_layout_le pll ON pll.le_id = ec.le_id 
JOIN payroll_runs pr ON pr.run_id = co.run_id AND pr.run_version= co.run_version AND pr.run_type_id!=2
WHERE ec.le_id = $5 AND ec.ee_id = $4 AND pr.run_id = $2 AND pr.run_version = $3
GROUP BY pll.date_format
)
WHEN field='emp_descr' THEN (SELECT ec.employment_descrip from ee_contract ec WHERE ec.ee_id =3 AND ec.le_id = $5)
WHEN field='dn' THEN (SELECT 
CASE 
WHEN (ec.ss_h_d_m = 'D') THEN (sum(pso.dn) OVER(PARTITION BY pso.contract_id)) 
ELSE (sum(pso.dn30) OVER(PARTITION BY pso.contract_id)) 
END as dn 
FROM pay_subperiod_output pso 
JOIN  ee_contract ec  ON ec.contract_id = pso.contract_id 
WHERE ec.ee_id = $4 AND ec.le_id = $5 AND pso.run_id = $2 AND pso.run_version = $3)::text
WHEN field='banks_branch_tax_iban' THEN (With query as ( SELECT 
CASE WHEN ec.payment_type = 'bank_transfer' 
THEN ( ' ' || '' || b.branch_code || ' (' || b.tax_id || ') ' || overlay(b.iban placing '**********'  from length(b.iban) - 10 for length(b.iban)) ) 
ELSE ' ' END 
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN periods p ON p.period_id = pr.period_id AND p.code >= TO_CHAR(eb.from_date,'YYYY MM') AND p.code <= TO_CHAR(eb.to_date,'YYYY MM') 
WHERE ec.ee_id = $4 AND ec.le_id = $5 AND (NOW() BETWEEN eb.from_date AND eb.to_date) AND pr.run_id = $2 AND pr.run_version = $3) SELECT * FROM query)
ELSE field
END AS field, 
position_x, position_y, null as position_y_start, font, alignment, size, type, position_x_end, position_y_end,
 null as row_height, null as wt_sequence
FROM payslip_data_positioning WHERE payslip_layout_id = $6
) sa
order by case when position_y_start is not null then wt_sequence else position_y end, position_x`,
    testFixed: ` SELECT * FROM (
SELECT field, position_x, null as position_y, position_y_start,
font, alignment, size, type, null as position_x_end, null as position_y_end, row_height, wt_sequence
  FROM (
SELECT
CASE 
WHEN pw.amount_convert_to_percent = false 
THEN to_char(SUM(DISTINCT co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
ELSE to_char(SUM(DISTINCT co.amount), '999,999.' || repeat('9', pw.amount_format_decimals) || '%') 
END as field, pw.amount_alignment as alignment, pw.amount_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 1 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = 3 AND co.run_id = 1 AND co.run_version = 1 AND co.amount != 0 AND co.amount IS NOT NULL
GROUP BY pw.amount_convert_to_percent, pw.amount_alignment, pw.amount_position_x, pdr.position_y_start, 
pw.font, pdr.size, pdr.row_height
,pw.amount_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
CASE 
WHEN pw.base_convert_to_percent = false 
THEN to_char(SUM(DISTINCT co.base), '999,999.' || repeat('9', pw.base_format_decimals)) 
ELSE to_char(SUM(DISTINCT co.base), '999,999.' || repeat('9', pw.base_format_decimals) || '%') 
END as field, pw.base_alignment as alignment, pw.base_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 1 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = 3 AND co.run_id = 1 AND co.run_version = 1 AND co.base != 0 AND co.base IS NOT NULL
GROUP BY pw.base_convert_to_percent, pw.base_alignment, pw.base_position_x, pdr.position_y_start, 
pw.font, pdr.size, pdr.row_height
,pw.base_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
CASE 
WHEN pw.rate_convert_to_percent = false 
THEN to_char(SUM(DISTINCT co.rate), '999,9990.' || repeat('9', pw.rate_format_decimals)) 
ELSE to_char(SUM(DISTINCT co.rate), '999,9990.' || repeat('9', pw.rate_format_decimals) || '%') 
END as field, pw.rate_alignment as alignment, pw.rate_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 1 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = 3 AND co.run_id = 1 AND co.run_version = 1 AND co.rate != 0 AND co.rate IS NOT NULL
GROUP BY pw.rate_convert_to_percent, pw.rate_alignment, pw.rate_position_x, pdr.position_y_start, 
pw.font, pdr.size, pdr.row_height
,pw.rate_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
wt.payslip_text as field, pw.text_alignment as alignment, pw.text_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 1 AND pdr.position_id = pw.position_id
JOIN wt_text wt ON wt.wt_id = pw.wt_id
WHERE co.payslip_id = 3 AND co.run_id = 1 AND co.run_version = 1
GROUP BY wt.payslip_text, pw.text_alignment, pw.text_position_x, pdr.position_y_start, pw.font, 
pdr.size, pdr.row_height,
co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
null as field, pw.ref_alignment as alignment, pw.ref_position_x as position_x,
pdr.position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 1 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = 3 AND co.run_id = 1 AND co.run_version = 1 AND co.ref_period_id is not null
GROUP BY  pw.ref_alignment, pw.ref_position_x, pdr.position_y_start, pw.font, pdr.size, pdr.row_height,
co.wt_id,  pw.wt_sequence, co.ref_period_id
) AS st 
UNION ALL
SELECT
CASE
WHEN field = 'payslip_date' THEN TO_CHAR((SELECT payslip_date FROM payroll_runs WHERE run_id = 1 
AND run_version = 1 AND le_id = 1), (SELECT date_format FROM payslip_layout_le WHERE le_id = 1))
WHEN field = 'ee_id_text' THEN (SELECT ee_id_text FROM ee_id_le WHERE ee_id = 3 AND le_id = 1)
WHEN field = 'ss_id' THEN (SELECT ss_id FROM ee_id_universal_data WHERE ee_id = 3)
WHEN field = 'ee_name' THEN (SELECT CASE WHEN last_name2 IS NOT NULL THEN
                last_name || ' ' || last_name2 || ', ' || first_name 
                ELSE last_name || ' ' || first_name END FROM ee_id_universal_data WHERE ee_id = 3)
WHEN field = 'address_line1_ee' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE (now() BETWEEN ea.from_date AND ea.to_date) AND ec.ee_id = 3 AND ec.le_id = 1)
WHEN field = 'address_line1_le' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id WHERE le_id = 1)
WHEN field = 'address_zip_city' THEN (SELECT DISTINCT ad.address_zip_code || ' ' || ad.address_city  
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE (now() BETWEEN ea.from_date AND ea.to_date) AND co.payslip_id = 3 AND co.run_id = 1 AND co.run_version = 1 AND ec.ee_id = 3 AND ec.le_id = 1
)
WHEN field = 'address_country' THEN (SELECT DISTINCT ad.address_country 
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE (now() BETWEEN ea.from_date AND ea.to_date) AND co.payslip_id = 3 AND co.run_id = 1 AND co.run_version = 1 AND ec.ee_id = 3 AND ec.le_id = 1 
)
WHEN field = 'periods_code' THEN (SELECT p.code FROM periods p JOIN payroll_runs pr ON pr.period_id = p.period_id
WHERE pr.run_id = 1 AND pr.run_version = 1 AND pr.le_id = 1)
WHEN field = 'payment_date' THEN TO_CHAR((SELECT payment_date FROM payroll_runs WHERE run_id = 1 
AND run_version = 1 AND le_id = 1), (SELECT date_format FROM payslip_layout_le WHERE le_id = 1))
WHEN field = 'banks_bank_iban' THEN (SELECT DISTINCT b.bank || ' ' || b.iban
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE (now() BETWEEN eb.from_date AND eb.to_date) AND  co.payslip_id = 3 AND co.run_id = 1 AND co.run_version = 1 AND ec.ee_id = 3 AND ec.le_id = 1 
LIMIT 1)

WHEN (SELECT SUBSTRING(field, 1, 4)) = 'tag_' THEN (SELECT DISTINCT ptt.tag_translation FROM tags_translation ptt 
JOIN ee_id_le eil ON eil.leg_language = ptt.language_id
WHERE tag_id = (SELECT SUBSTRING(field, 5, length(field))::int) AND eil.ee_id = 3 AND eil.le_id = 1)

WHEN field='wt_146' THEN (SELECT to_char(SUM(co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 146 AND co.payslip_id = 3
AND run_id = 1 AND run_version = 1 GROUP BY pw.amount_format_decimals )::text

WHEN field='wt_162' THEN (SELECT to_char(co.amount, '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 162 AND co.payslip_id = 3
AND run_id = 1 AND run_version = 1 )::text


WHEN field='wt_51' THEN (SELECT to_char(SUM(co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 51 AND co.payslip_id = 3
AND run_id = 1 AND run_version = 1 GROUP BY pw.amount_format_decimals )::text
WHEN field='text_wt_51' THEN (SELECT payslip_text FROM wt_text WHERE wt_id = 51 AND
lang_id = (SELECT leg_language FROM ee_id_le WHERE ee_id = 3 AND le_id = 1))
WHEN field='le_name' THEN (SELECT le_name FROM legal_entity WHERE le_id = 1)
WHEN field='address_zip_region_city' THEN (SELECT ad.address_zip_code || ' ' || ad.address_region || ' (' || ad.address_city || ')' 
FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id
WHERE le.le_id = 1 AND (NOW() BETWEEN le.from_date AND le.to_date))
WHEN field='tax_id_le' THEN (SELECT le.tax_id  FROM legal_entity le WHERE le.le_id = 1 AND (NOW() BETWEEN le.from_date AND le.to_date))
WHEN field='ccc' THEN (SELECT wc.ccc FROM work_center wc 
JOIN ee_contract ec ON ec.wc_id = wc.wc_id AND ec.le_id = wc.le_id 
WHERE ec.ee_id = 3 AND ec.le_id = 1)
WHEN field='seniority_date' THEN (SELECT TO_CHAR(ec.seniority_date, pll.date_format) 
FROM ee_contract ec
JOIN payslip_layout_le pll ON pll.le_id = ec.le_id
WHERE ec.le_id = 1 AND ec.ee_id = 3)
WHEN field='contract_type' THEN (SELECT ec.contract_type from ee_contract ec 
WHERE ec.le_id = 1 AND ec.ee_id = 3)
WHEN field='ss_group' THEN (SELECT ec.ss_group from ee_contract ec WHERE ec.le_id = 1 AND ec.ee_id = 3)::text
WHEN field='tax_id_ee' THEN (SELECT tax_id FROM ee_id_universal_data
WHERE ee_id = 3)
WHEN field='wc_name' THEN (
SELECT wc.wc_name FROM work_center wc 
WHERE wc.wc_id = 1 AND wc.le_id = 1 AND (NOW() BETWEEN wc.from_date AND wc.to_date))
WHEN field='lev_sub_name' THEN (SELECT cal.lev_sub_name FROM coll_agree_lev cal 
LEFT JOIN ee_contract ec ON ec.coll_id = cal.coll_id AND ec.coll_version = cal.coll_version AND ec.level = cal.level 
AND ec.ss_h_d_m = cal.ss_h_d_m AND (ec.sublevel = cal.sublevel OR cal.sublevel = 0)
WHERE ec.ee_id = 3 AND ec.le_id = 1)
WHEN field='desde_hasta' THEN (SELECT 'desde ' || TO_CHAR(min(pso.from_date), pll.date_format) || ' , hasta ' || TO_CHAR(max(pso.to_date),pll.date_format)
FROM pay_subperiod_output pso 
JOIN calculation_output co ON co.contract_id = pso.contract_id AND co.run_id = pso.run_id AND co.run_version = pso.run_version 
JOIN ee_contract ec ON ec.contract_id = pso.contract_id JOIN payslip_layout_le pll ON pll.le_id = ec.le_id 
JOIN payroll_runs pr ON pr.run_id = co.run_id AND pr.run_version= co.run_version AND pr.run_type_id!=2
WHERE ec.le_id = 1 AND ec.ee_id = 3 AND pr.run_id = 1 AND pr.run_version = 1
GROUP BY pll.date_format
)
WHEN field='emp_descr' THEN (SELECT ec.employment_descrip from ee_contract ec WHERE ec.ee_id =3 AND ec.le_id = 1)
WHEN field='dn' THEN (SELECT 
CASE 
WHEN (ec.ss_h_d_m = 'D') THEN (sum(pso.dn) OVER(PARTITION BY pso.contract_id)) 
ELSE (sum(pso.dn30) OVER(PARTITION BY pso.contract_id)) 
END as dn 
FROM pay_subperiod_output pso 
JOIN  ee_contract ec  ON ec.contract_id = pso.contract_id 
WHERE ec.ee_id = 3 AND ec.le_id = 1 AND pso.run_id = 1 AND pso.run_version = 1)::text
WHEN field='banks_branch_tax_iban' THEN (With query as ( SELECT 
CASE WHEN ec.payment_type = 'bank_transfer' 
THEN ( ' ' || '' || b.branch_code || ' (' || b.tax_id || ') ' || overlay(b.iban placing '**********'  from length(b.iban) - 10 for length(b.iban)) ) 
ELSE ' ' END 
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN periods p ON p.period_id = pr.period_id AND p.code >= TO_CHAR(eb.from_date,'YYYY MM') AND p.code <= TO_CHAR(eb.to_date,'YYYY MM') 
WHERE ec.ee_id = 3 AND ec.le_id = 1 AND (NOW() BETWEEN eb.from_date AND eb.to_date) AND pr.run_id = 1 AND pr.run_version = 1) SELECT * FROM query)
ELSE field
END AS field, 
position_x, position_y, null as position_y_start, font, alignment, size, type, position_x_end, position_y_end,
 null as row_height, null as wt_sequence
FROM payslip_data_positioning WHERE payslip_layout_id = 1
) sa
order by case when position_y_start is not null then wt_sequence else position_y end, position_x`,
};
