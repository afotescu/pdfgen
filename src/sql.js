export default {
    checkTasks: `SELECT task_id FROM calc_tasks WHERE status='GENR' AND payslip_exists = false
     ORDER BY task_id LIMIT 1`,
    writeToArchive: `INSERT INTO arch_files(ee_id, run_id, run_version_id, wc_id, le_id, contract_id,
                    file_name, file_path, doc_type) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    finishTask: 'UPDATE calc_tasks SET payslip_exists = true WHERE task_id = $1',
    getStaticFields: `SELECT position_x, position_y, type, null as VALUE FROM payslip_data_positioning pdp
JOIN payslip_layout_le pll ON 
pll.payslip_layout_id = pdp.payslip_layout_id OR pll.payslip_layout_term_id = pdp.payslip_layout_id
JOIN payslip_pictures pp ON pp.le_id = pll.le_id`,
    getConfigurationData: `SELECT DISTINCT co.payslip_id, ec.le_id, ec.ee_id, ec.wc_id , co.run_id, co.run_version, 
CASE 
WHEN ec.termination = TRUE THEN pll.payslip_layout_term_id
ELSE pll.payslip_layout_id END as payslip_layout_id
    FROM calculation_output co
JOIN calc_tasks ct ON ct.run_id = co.run_id AND ct.run_version = co.run_version
JOIN ee_contract ec ON ec.contract_id = co.contract_id
JOIN payslip_layout_le pll ON pll.le_id = ec.le_id
WHERE ct.task_id = $1
ORDER BY payslip_id`,
    test: `SELECT * FROM (
SELECT field, position_x, null as position_y, position_id,
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
AND run_version = $3 AND le_id = 1), 'DD-MM-YYYY')
WHEN field = 'ee_id_text' THEN (SELECT ee_id_text FROM ee_id_le WHERE ee_id = 3 AND le_id = 1)
WHEN field = 'ss_id' THEN (SELECT ss_id FROM ee_id_universal_data WHERE ee_id = 3)
WHEN field = 'ee_name' THEN (SELECT CASE WHEN last_name2 IS NOT NULL THEN
                last_name || ' ' || last_name2 || ', ' || first_name 
                ELSE last_name || ' ' || first_name END FROM ee_id_universal_data WHERE ee_id = 3)
WHEN field = 'address_line1_ee' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE ec.ee_id = 3 AND ec.le_id = 1)
WHEN field = 'address_line1_le' THEN (SELECT DISTINCT ad.address_line1 FROM address ad 
JOIN legal_entity le ON le.payslip_address_id = ad.address_id WHERE le_id = 1)
WHEN field = 'address_zip_city' THEN (SELECT DISTINCT ad.address_zip_code || ' ' || ad.address_city  
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id 
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = 3 AND ec.le_id = 1 
)
WHEN field = 'address_country' THEN (SELECT DISTINCT ad.address_country 
FROM address ad 
JOIN ee_addresses ea ON ea.payslip_address_id = ad.address_id
JOIN ee_contract ec ON ec.ee_id = ea.ee_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = 3 AND ec.le_id = 1 
)
WHEN field = 'periods_code' THEN (SELECT p.code FROM periods p JOIN payroll_runs pr ON pr.period_id = p.period_id
WHERE pr.run_id = $2 AND pr.run_version = $3 AND pr.le_id = 1)
WHEN field = 'payment_date' THEN TO_CHAR((SELECT payment_date FROM payroll_runs WHERE run_id = $2 
AND run_version = $3 AND le_id = 1), 'DD-MM-YYYY')
WHEN field = 'banks_bank_iban' THEN (SELECT DISTINCT b.bank || ' ' || b.iban
FROM banks b  
JOIN ee_banks eb ON eb.payroll_bank_id1 = b.bank_account_id 
JOIN ee_contract ec ON ec.le_id = eb.le_id AND ec.ee_id = eb.ee_id 
JOIN payroll_runs pr ON ec.le_id = pr.le_id 
JOIN calculation_output co ON co.contract_id = ec.contract_id
WHERE co.payslip_id = $1 AND co.run_id = $2 AND co.run_version = $3 AND ec.ee_id = 3 AND ec.le_id = 1 
LIMIT 1)
WHEN (SELECT SUBSTRING(field, 1, 4)) = 'tag_' THEN (SELECT DISTINCT ptt.tag_translation FROM tags_translation ptt 
JOIN ee_id_le eil ON eil.leg_language = ptt.language_id
WHERE tag_id = (SELECT SUBSTRING(field, 5, length(field))::int) AND eil.ee_id = 3 AND eil.le_id = 1)
WHEN field='wt_146' THEN (SELECT amount from calculation_output where wt_id = 146 AND payslip_id = $1 
AND run_id = $2 AND run_version = $3)::text
ELSE field
END AS field, 
position_x, position_y, null as position_id, font, alignment, size, type, position_x_end, position_y_end,
 null as row_height, null as wt_sequence
FROM payslip_data_positioning WHERE payslip_layout_id = $6
) sa
order by case when position_id is not null then wt_sequence else position_y end, position_x`,
};
