export default {
    leConcatFiles: `SELECT ARRAY((SELECT af.file_path || '/' || af.file_name FROM arch_files af
JOIN work_center wc ON wc.wc_id = af.wc_id AND wc.le_id = af.le_id
    WHERE run_id = $1 AND run_version_id = $2 ORDER BY wc.wc_short_name, ee_id)) as files`,
    wcConcatFiles: `SELECT  wc.wc_short_name as wc_name, ARRAY_AGG(af.file_path || '/' || af.file_name) as files 
    FROM arch_files af
    JOIN work_center wc ON wc.wc_id = af.wc_id AND wc.le_id = af.le_id
    WHERE run_id = $1 AND run_version_id = $2 AND af.wc_id = $3 GROUP BY wc.wc_short_name`,
    checkWorkCenters: 'SELECT DISTINCT wc_id FROM arch_files WHERE run_id = $1 AND run_version_id = $2',
    clearRunFromArchive: 'DELETE FROM arch_files WHERE run_id = $1 AND run_version_id = $2 AND doc_type = \'PAY\'',
    checkTasks: `SELECT ct.task_id, ct.run_id, ct.run_version, li.payslip_password, pr.le_id, p.code FROM calc_tasks ct
JOIN payroll_runs pr ON pr.run_id = ct.run_id AND pr.run_version = ct.run_version
JOIN le_ids li ON li.le_id = pr.le_id
JOIN periods p ON p.period_id = pr.period_id
WHERE ct.status='GENR' AND ct.payslip_exists = false
     ORDER BY ct.task_id LIMIT 1`,
    writeToArchive: `INSERT INTO arch_files(ee_id, run_id, run_version_id, wc_id, le_id, contract_id,
                    file_name, file_path, doc_type) VALUES `,
    finishTask: 'UPDATE calc_tasks SET payslip_exists = true WHERE task_id = $1',
    getConfigurationData: `SELECT DISTINCT co.payslip_id, ec.le_id, ec.ee_id, ec.wc_id, co.run_id, co.run_version,
CASE 
WHEN ec.termination = TRUE THEN pll.payslip_layout_term_id
ELSE pll.payslip_layout_id END as payslip_layout_id, ec.termination, ec.contract_id
    FROM calculation_output co
JOIN calc_tasks ct ON ct.run_id = co.run_id AND ct.run_version = co.run_version
JOIN ee_contract ec ON ec.contract_id = co.contract_id AND exclude_payslip = false
JOIN payslip_layout_le pll ON pll.le_id = ec.le_id
WHERE ct.task_id = $1
ORDER BY payslip_id`,
    test: `SELECT * FROM (
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
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = $6 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND co.amount != 0 AND co.amount IS NOT NULL
AND co.wt_id NOT IN (51, 162)
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
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = $6 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND co.base != 0 AND co.base IS NOT NULL
AND co.wt_id NOT IN (51, 162)
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
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = $6 AND pdr.position_id = pw.position_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND co.rate != 0 AND co.rate IS NOT NULL
AND co.wt_id NOT IN (51, 162)
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
AND co.wt_id NOT IN (51, 162)
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
AND co.wt_id NOT IN (51, 162)
GROUP BY  pw.ref_alignment, pw.ref_position_x, pdr.position_y_start, pw.font, pdr.size, pdr.row_height,
co.wt_id,  pw.wt_sequence, co.ref_period_id
) AS st 
UNION ALL
SELECT
CASE
WHEN field = 'payslip_date' THEN TO_CHAR((SELECT payslip_date FROM payroll_runs WHERE run_id = $2 
AND run_version = $3 AND le_id = $5 LIMIT 1), (SELECT date_format FROM payslip_layout_le WHERE le_id = $5))

WHEN field = 'ee_id_text' THEN (SELECT ee_id_text FROM ee_id_le WHERE ee_id = $4 AND le_id = $5
AND ($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')) LIMIT 1)

WHEN field = 'ss_id' THEN (SELECT ss_id FROM ee_id_universal_data WHERE ee_id = $4 
AND ($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')) LIMIT 1)

WHEN field = 'ee_name' THEN (SELECT CASE WHEN last_name2 IS NOT NULL THEN
                last_name || ' ' || last_name2 || ', ' || first_name 
                ELSE last_name || ' ' || first_name END FROM ee_id_universal_data WHERE ee_id = $4 
                AND ($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')) LIMIT 1)

WHEN field = 'address_line1_ee' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id JOIN calculation_output co ON co.contract_id = ec.contract_id 
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
WHERE ($8 BETWEEN TO_CHAR(ea.from_date, 'YYYY MM') AND TO_CHAR(ea.to_date, 'YYYY MM')) 
AND ec.ee_id = $4 AND ec.le_id = $5 LIMIT 1)

WHEN field = 'address_line1_le' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id WHERE le_id = $5 
AND ($8 BETWEEN TO_CHAR(le.from_date, 'YYYY MM') AND TO_CHAR(le.to_date, 'YYYY MM')) LIMIT 1)

WHEN field = 'address_zip_city' THEN (SELECT DISTINCT ad.address_zip_code || ' ' || ad.address_city  
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE ($8 BETWEEN TO_CHAR(ea.from_date, 'YYYY MM') AND TO_CHAR(ea.to_date, 'YYYY MM')) AND co.payslip_id = $1 
AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = $4 AND ec.le_id = $5
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
LIMIT 1)

WHEN field = 'address_country' THEN (SELECT DISTINCT ad.address_country 
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE ($8 BETWEEN TO_CHAR(ea.from_date, 'YYYY MM') AND TO_CHAR(ea.to_date, 'YYYY MM')) AND co.payslip_id = $1 
AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = $4 AND ec.le_id = $5
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
)
WHEN field = 'periods_code' THEN (SELECT p.code FROM periods p JOIN payroll_runs pr ON pr.period_id = p.period_id
WHERE pr.run_id = $2 AND pr.run_version = $3 AND pr.le_id = $5 LIMIT 1)

WHEN field = 'payment_date' THEN TO_CHAR((SELECT payment_date FROM payroll_runs WHERE run_id = $2 
AND run_version = $3 AND le_id = $5 LIMIT 1), (SELECT date_format FROM payslip_layout_le WHERE le_id = $5))

WHEN field = 'banks_bank_iban' THEN (SELECT DISTINCT b.bank || ' ' || b.iban
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE ($8 BETWEEN TO_CHAR(eb.from_date, 'YYYY MM') AND TO_CHAR(eb.to_date, 'YYYY MM')) AND  co.payslip_id = $1 
AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = $4 AND ec.le_id = $5 
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM')) LIMIT 1
)

WHEN (SELECT SUBSTRING(field, 1, 4)) = 'tag_' THEN (SELECT DISTINCT ptt.tag_translation FROM tags_translation ptt 
JOIN ee_id_le eil ON eil.leg_language = ptt.language_id
WHERE tag_id = (SELECT SUBSTRING(field, 5, length(field))::int) AND eil.ee_id = $4 AND eil.le_id = $5)

WHEN field='wt_146' THEN (SELECT to_char(SUM(co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 146 AND co.payslip_id = $1
AND run_id = $2 AND run_version = $3 GROUP BY pw.amount_format_decimals)::text

WHEN field='wt_162' THEN (SELECT to_char(co.amount, '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 162 AND co.payslip_id = $1
AND run_id = $2 AND run_version = $3)::text


WHEN field='wt_51' THEN (SELECT to_char(SUM(co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 51 AND co.payslip_id = $1
AND run_id = $2 AND run_version = $3 GROUP BY pw.amount_format_decimals)::text

WHEN field='text_wt_51' THEN (SELECT payslip_text FROM wt_text WHERE wt_id = 51 AND
lang_id = (SELECT leg_language FROM ee_id_le WHERE ee_id = $4 AND le_id = $5))

WHEN field='le_name' THEN (SELECT le_name FROM legal_entity WHERE le_id = $5 AND 
($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')) LIMIT 1)

WHEN field='address_zip_region_city' THEN 
(SELECT ad.address_zip_code || ' ' || ad.address_region || ' (' || ad.address_city || ')' 
FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id
WHERE le.le_id = $5 AND ($8 BETWEEN TO_CHAR(le.from_date, 'YYYY MM') AND TO_CHAR(le.to_date, 'YYYY MM')) LIMIT 1)

WHEN field='tax_id_le' THEN (SELECT le.tax_id  FROM legal_entity le WHERE le.le_id = $5
 AND ($8 BETWEEN TO_CHAR(le.from_date, 'YYYY MM') AND TO_CHAR(le.to_date, 'YYYY MM')) LIMIT 1)

WHEN field='ccc' THEN (SELECT wc.ccc FROM work_center wc 
JOIN ee_contract ec ON ec.wc_id = wc.wc_id AND ec.le_id = wc.le_id 
WHERE ec.ee_id = $4 AND ec.le_id = $5 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
AND TO_CHAR(ec.contract_end_date, 'YYYY MM')) LIMIT 1)

WHEN field='seniority_date' THEN (SELECT TO_CHAR(ec.seniority_date, pll.date_format) 
FROM ee_contract ec
JOIN payslip_layout_le pll ON pll.le_id = ec.le_id
WHERE ec.le_id = $5 AND ec.ee_id = $4 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
AND TO_CHAR(ec.contract_end_date, 'YYYY MM')) LIMIT 1)

WHEN field='contract_type' THEN (SELECT ec.contract_type from ee_contract ec 
WHERE ec.le_id = $5 AND ec.ee_id = $4 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
AND TO_CHAR(ec.contract_end_date, 'YYYY MM')) LIMIT 1)

WHEN field='ss_group' THEN (SELECT ec.ss_group from ee_contract ec WHERE ec.le_id = $5 AND ec.ee_id = $4 
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM')) LIMIT 1)::text

WHEN field='tax_id_ee' THEN (SELECT tax_id FROM ee_id_universal_data
WHERE ee_id = $4 AND ($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')) LIMIT 1)

WHEN field='wc_name' THEN (
SELECT wc.wc_name FROM work_center wc 
WHERE wc.wc_id = $7 AND wc.le_id = $5 AND ($8 BETWEEN TO_CHAR(wc.from_date, 'YYYY MM') 
AND TO_CHAR(wc.to_date, 'YYYY MM')) LIMIT 1)

WHEN field='lev_sub_name' THEN (SELECT cal.lev_sub_name FROM coll_agree_lev cal 
LEFT JOIN ee_contract ec ON ec.coll_id = cal.coll_id AND ec.coll_version = cal.coll_version AND ec.level = cal.level 
AND ec.ss_h_d_m = cal.ss_h_d_m AND (ec.sublevel = cal.sublevel OR cal.sublevel = 0)
 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
WHERE ec.ee_id = $4 AND ec.le_id = $5 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
AND TO_CHAR(ec.contract_end_date, 'YYYY MM')) LIMIT 1)

WHEN field='desde_hasta' THEN (SELECT 'desde ' || TO_CHAR(min(pso.from_date), pll.date_format) || ' , hasta ' || 
TO_CHAR(max(pso.to_date),pll.date_format)
FROM pay_subperiod_output pso 
JOIN calculation_output co ON co.contract_id = pso.contract_id AND co.run_id = pso.run_id
 AND co.run_version = pso.run_version 
JOIN ee_contract ec ON ec.contract_id = pso.contract_id JOIN payslip_layout_le pll ON pll.le_id = ec.le_id 
JOIN payroll_runs pr ON pr.run_id = co.run_id AND pr.run_version= co.run_version AND pr.run_type_id!=2
WHERE ec.le_id = $5 AND ec.ee_id = $4 AND pr.run_id = $2 AND pr.run_version = $3 
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
GROUP BY pll.date_format LIMIT 1
)

WHEN field='emp_descr' THEN (SELECT ec.employment_descrip from ee_contract ec WHERE ec.ee_id = $4 
AND ec.le_id = $5 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND 
TO_CHAR(ec.contract_end_date, 'YYYY MM')) LIMIT 1)

WHEN field='dn' THEN (SELECT 
CASE 
WHEN (ec.ss_h_d_m = 'D') THEN sum(pso.dn)
ELSE sum(pso.dn30)
END as dn 
FROM pay_subperiod_output pso 
JOIN  ee_contract ec  ON ec.contract_id = pso.contract_id 
WHERE ec.ee_id = $4 AND ec.le_id = $5 AND pso.run_id = $2 AND pso.run_version = $3 AND 
($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
GROUP BY ss_h_d_m LIMIT 1)::text

WHEN field = 'uranterm1' THEN (SELECT REPLACE((SELECT REPLACE(tag_translation, 
'{1}',(SELECT le_name FROM legal_entity WHERE le_id = $5)) 
FROM tags_translation WHERE tag_id = 65 AND language_id = 
(SELECT leg_language FROM ee_id_le WHERE ee_id = $4)), '{2}', (SELECT to_char(SUM(co.amount),
 '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 146 AND co.payslip_id = $1
AND run_id = $2 AND run_version = $3 GROUP BY pw.amount_format_decimals )) LIMIT 1)

WHEN field = 'uranterm2' THEN (SELECT REPLACE((SELECT tag_translation FROM tags_translation WHERE tag_id = 66
AND language_id = 
(SELECT leg_language FROM ee_id_le WHERE ee_id = $4)), '{3}', (SELECT lr.text_code_int from leave_reason lr
JOIN ee_contract ec ON ec.leave_reason_code_int = lr.leave_reason_code_int
WHERE ec.ee_id = $4 AND ec.le_id = $5 AND ec.wc_id = $7)) LIMIT 1)

WHEN field = 'uranterm3' THEN (SELECT REPLACE((SELECT REPLACE((SELECT tag_translation 
FROM tags_translation WHERE tag_id = 64 AND language_id = 
(SELECT leg_language FROM ee_id_le WHERE ee_id = $4)), '{4}', (SELECT DISTINCT ad.address_region  
FROM address ad 
JOIN work_center wc ON wc.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.wc_id = wc.wc_id AND ec.le_id = wc.le_id
WHERE ec.le_id = $5 AND ec.ee_id = $4 AND ec.wc_id = $7)) ), '{5}',
(SELECT DISTINCT TO_CHAR(ec.contract_end_date, pll.date_format) 
FROM ee_contract ec 
JOIN payslip_layout_le pll ON pll.le_id = ec.le_id
WHERE ec.ee_id = $4 AND ec.le_id = $5 AND ec.wc_id = $7)) LIMIT 1)

WHEN field='banks_branch_tax_iban' THEN (With query as ( SELECT 
CASE WHEN ec.payment_type = 'bank_transfer' 
THEN ( ' ' || '' || b.branch_code || ' (' || b.tax_id || ') ' || overlay(b.iban placing '**********' 
 from length(b.iban) - 10 for length(b.iban)) ) 
ELSE ' ' END 
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN periods p ON p.period_id = pr.period_id AND p.code >= TO_CHAR(eb.from_date,'YYYY MM') AND p.code <=
 TO_CHAR(eb.to_date,'YYYY MM') 
WHERE ec.ee_id = $4 AND ec.le_id = $5 AND ($8 BETWEEN TO_CHAR(eb.from_date, 'YYYY MM') AND 
TO_CHAR(eb.to_date, 'YYYY MM')) AND pr.run_id = 
$2 AND pr.run_version = $3
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))) 
SELECT * FROM query LIMIT 1)
ELSE field
END AS field, 
position_x, position_y, null as position_y_start, font, alignment, size, type, position_x_end, position_y_end,
 null as row_height, null as wt_sequence
FROM payslip_data_positioning WHERE payslip_layout_id = $6
) sa
order by case when position_y_start is null then position_y else null end desc, 
case when position_y_start is not null then wt_sequence else null end asc,
position_x`,
    testFixed: `SELECT * FROM (
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
WHERE co.payslip_id = 76 AND co.run_id = 3 AND co.run_version = 1 AND co.amount != 0 AND co.amount IS NOT NULL
AND co.wt_id NOT IN (51, 162)
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
WHERE co.payslip_id = 76 AND co.run_id = 3 AND co.run_version = 1 AND co.base != 0 AND co.base IS NOT NULL
AND co.wt_id NOT IN (51, 162)
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
WHERE co.payslip_id = 76 AND co.run_id = 3 AND co.run_version = 1 AND co.rate != 0 AND co.rate IS NOT NULL
AND co.wt_id NOT IN (51, 162)
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
WHERE co.payslip_id = 76 AND co.run_id = 3 AND co.run_version = 1
AND co.wt_id NOT IN (51, 162)
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
WHERE co.payslip_id = 76 AND co.run_id = 3 AND co.run_version = 1 AND co.ref_period_id is not null
AND co.wt_id NOT IN (51, 162)
GROUP BY  pw.ref_alignment, pw.ref_position_x, pdr.position_y_start, pw.font, pdr.size, pdr.row_height,
co.wt_id,  pw.wt_sequence, co.ref_period_id
) AS st 
UNION ALL
SELECT
CASE
WHEN field = 'payslip_date' THEN TO_CHAR((SELECT payslip_date FROM payroll_runs WHERE run_id = 3 
AND run_version = 1 AND le_id = 1), (SELECT date_format FROM payslip_layout_le WHERE le_id = 1))

WHEN field = 'ee_id_text' THEN (SELECT ee_id_text FROM ee_id_le WHERE ee_id = 76 AND le_id = 1
AND ($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')))

WHEN field = 'ss_id' THEN (SELECT ss_id FROM ee_id_universal_data WHERE ee_id = 76 AND ($8 BETWEEN 
from_date AND to_date))

WHEN field = 'ee_name' THEN (SELECT CASE WHEN last_name2 IS NOT NULL THEN
                last_name || ' ' || last_name2 || ', ' || first_name 
                ELSE last_name || ' ' || first_name END FROM ee_id_universal_data WHERE ee_id = 76 AND 
                ($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')))

WHEN field = 'address_line1_ee' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id JOIN calculation_output co ON co.contract_id = ec.contract_id
 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
WHERE ($8 BETWEEN TO_CHAR(ea.from_date, 'YYYY MM') AND TO_CHAR(ea.to_date, 'YYYY MM')) AND ec.ee_id = 76 
AND ec.le_id = 1)

WHEN field = 'address_line1_le' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id WHERE le_id = 1 AND ($8 BETWEEN 
le.from_date AND le.to_date))

WHEN field = 'address_zip_city' THEN (SELECT DISTINCT ad.address_zip_code || ' ' || ad.address_city  
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE ($8 BETWEEN TO_CHAR(ea.from_date, 'YYYY MM') AND TO_CHAR(ea.to_date, 'YYYY MM')) AND co.payslip_id = 76 
AND co.run_id = 3 AND co.run_version = 1 
AND ec.ee_id = 76 AND ec.le_id = 1
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
)
WHEN field = 'address_country' THEN (SELECT DISTINCT ad.address_country 
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE ($8 BETWEEN TO_CHAR(ea.from_date, 'YYYY MM') AND TO_CHAR(ea.to_date, 'YYYY MM')) AND co.payslip_id = 76 
AND co.run_id = 3 AND co.run_version = 1
 AND ec.ee_id = 76 AND ec.le_id = 1
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
)
WHEN field = 'periods_code' THEN (SELECT p.code FROM periods p JOIN payroll_runs pr ON pr.period_id = p.period_id
WHERE pr.run_id = 3 AND pr.run_version = 1 AND pr.le_id = 1)

WHEN field = 'payment_date' THEN TO_CHAR((SELECT payment_date FROM payroll_runs WHERE run_id = 3 
AND run_version = 1 AND le_id = 1), (SELECT date_format FROM payslip_layout_le WHERE le_id = 1))

WHEN field = 'banks_bank_iban' THEN (SELECT DISTINCT b.bank || ' ' || b.iban
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE ($8 BETWEEN TO_CHAR(eb.from_date, 'YYYY MM') AND TO_CHAR(eb.to_date, 'YYYY MM')) AND  co.payslip_id = 76 
AND co.run_id = 3 AND co.run_version = 1 
AND ec.ee_id = 76 AND ec.le_id = 1 
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
)

WHEN (SELECT SUBSTRING(field, 1, 4)) = 'tag_' THEN (SELECT DISTINCT ptt.tag_translation FROM tags_translation ptt 
JOIN ee_id_le eil ON eil.leg_language = ptt.language_id
WHERE tag_id = (SELECT SUBSTRING(field, 5, length(field))::int) AND eil.ee_id = 76 AND eil.le_id = 1)

WHEN field='wt_146' THEN (SELECT to_char(SUM(co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 146 AND co.payslip_id = 76
AND run_id = 3 AND run_version = 1 GROUP BY pw.amount_format_decimals )::text

WHEN field='wt_162' THEN (SELECT to_char(co.amount, '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 162 AND co.payslip_id = 76
AND run_id = 3 AND run_version = 1 )::text


WHEN field='wt_51' THEN (SELECT to_char(SUM(co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
from calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
where co.wt_id = 51 AND co.payslip_id = 76
AND run_id = 3 AND run_version = 1 GROUP BY pw.amount_format_decimals )::text

WHEN field='text_wt_51' THEN (SELECT payslip_text FROM wt_text WHERE wt_id = 51 AND
lang_id = (SELECT leg_language FROM ee_id_le WHERE ee_id = 76 AND le_id = 1))

WHEN field='le_name' THEN (SELECT le_name FROM legal_entity WHERE le_id = 1 AND 
($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')))

WHEN field='address_zip_region_city' THEN 
(SELECT ad.address_zip_code || ' ' || ad.address_region || ' (' || ad.address_city || ')' 
FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id
WHERE le.le_id = 1 AND ($8 BETWEEN TO_CHAR(le.from_date, 'YYYY MM') AND TO_CHAR(le.to_date, 'YYYY MM')))

WHEN field='tax_id_le' THEN (SELECT le.tax_id  FROM legal_entity le WHERE le.le_id = 1 AND
 ($8 BETWEEN TO_CHAR(le.from_date, 'YYYY MM') AND TO_CHAR(le.to_date, 'YYYY MM')))

WHEN field='ccc' THEN (SELECT wc.ccc FROM work_center wc 
JOIN ee_contract ec ON ec.wc_id = wc.wc_id AND ec.le_id = wc.le_id 
WHERE ec.ee_id = 76 AND ec.le_id = 1 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
AND TO_CHAR(ec.contract_end_date, 'YYYY MM')))

WHEN field='seniority_date' THEN (SELECT TO_CHAR(ec.seniority_date, pll.date_format) 
FROM ee_contract ec
JOIN payslip_layout_le pll ON pll.le_id = ec.le_id
WHERE ec.le_id = 1 AND ec.ee_id = 76 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
AND TO_CHAR(ec.contract_end_date, 'YYYY MM')))

WHEN field='contract_type' THEN (SELECT ec.contract_type from ee_contract ec 
WHERE ec.le_id = 1 AND ec.ee_id = 76 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
AND TO_CHAR(ec.contract_end_date, 'YYYY MM')))

WHEN field='ss_group' THEN (SELECT ec.ss_group from ee_contract ec WHERE ec.le_id = 1 AND ec.ee_id = 76 AND 
($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM')))::text

WHEN field='tax_id_ee' THEN (SELECT tax_id FROM ee_id_universal_data
WHERE ee_id = 76 AND ($8 BETWEEN TO_CHAR(from_date, 'YYYY MM') AND TO_CHAR(to_date, 'YYYY MM')))

WHEN field='wc_name' THEN (
SELECT wc.wc_name FROM work_center wc 
WHERE wc.wc_id = 1 AND wc.le_id = 1 AND ($8 BETWEEN TO_CHAR(wc.from_date, 'YYYY MM') 
AND TO_CHAR(wc.to_date, 'YYYY MM')))

WHEN field='lev_sub_name' THEN (SELECT cal.lev_sub_name FROM coll_agree_lev cal 
LEFT JOIN ee_contract ec ON ec.coll_id = cal.coll_id AND ec.coll_version = cal.coll_version AND ec.level = cal.level 
AND ec.ss_h_d_m = cal.ss_h_d_m AND (ec.sublevel = cal.sublevel OR cal.sublevel = 0) AND 
($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
WHERE ec.ee_id = 76 AND ec.le_id = 1 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
AND TO_CHAR(ec.contract_end_date, 'YYYY MM')))

WHEN field='desde_hasta' THEN (SELECT 'desde ' || TO_CHAR(min(pso.from_date), pll.date_format) || ' , hasta ' || 
TO_CHAR(max(pso.to_date),pll.date_format)
FROM pay_subperiod_output pso 
JOIN calculation_output co ON co.contract_id = pso.contract_id AND co.run_id = pso.run_id 
AND co.run_version = pso.run_version 
JOIN ee_contract ec ON ec.contract_id = pso.contract_id JOIN payslip_layout_le pll ON pll.le_id = ec.le_id 
JOIN payroll_runs pr ON pr.run_id = co.run_id AND pr.run_version= co.run_version AND pr.run_type_id!=2
WHERE ec.le_id = 1 AND ec.ee_id = 76 AND pr.run_id = 3 AND pr.run_version = 1 AND 
($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
GROUP BY pll.date_format
)

WHEN field='emp_descr' THEN (SELECT ec.employment_descrip from ee_contract ec WHERE ec.ee_id = 76
 AND ec.le_id = 1 AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') 
 AND TO_CHAR(ec.contract_end_date, 'YYYY MM')))

WHEN field='dn' THEN (SELECT 
CASE 
WHEN (ec.ss_h_d_m = 'D') THEN sum(pso.dn)
ELSE sum(pso.dn30)
END as dn 
FROM pay_subperiod_output pso 
JOIN  ee_contract ec  ON ec.contract_id = pso.contract_id 
WHERE ec.ee_id = 76 AND ec.le_id = 1 AND pso.run_id = 3 AND pso.run_version = 1 
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))
GROUP BY ss_h_d_m)::text

WHEN field='banks_branch_tax_iban' THEN (With query as ( SELECT 
CASE WHEN ec.payment_type = 'bank_transfer' 
THEN ( ' ' || '' || b.branch_code || ' (' || b.tax_id || ') ' || 
overlay(b.iban placing '**********'  from length(b.iban) - 10 for length(b.iban)) ) 
ELSE ' ' END 
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN periods p ON p.period_id = pr.period_id AND p.code >= TO_CHAR(eb.from_date,'YYYY MM') 
AND p.code <= TO_CHAR(eb.to_date,'YYYY MM') 
WHERE ec.ee_id = 76 AND ec.le_id = 1 AND 
($8 BETWEEN TO_CHAR(eb.from_date, 'YYYY MM') AND TO_CHAR(eb.to_date, 'YYYY MM')) AND pr.run_id = 3 
AND pr.run_version = 1
AND ($8 BETWEEN TO_CHAR(ec.contract_start_date, 'YYYY MM') AND TO_CHAR(ec.contract_end_date, 'YYYY MM'))) 
SELECT * FROM query)
ELSE field
END AS field, 
position_x, position_y, null as position_y_start, font, alignment, size, type, position_x_end, position_y_end,
 null as row_height, null as wt_sequence
FROM payslip_data_positioning WHERE payslip_layout_id = 1
) sa
order by case when position_y_start is null then position_y else null end desc, 
case when position_y_start is not null then wt_sequence else null end asc,
position_x`,
};
