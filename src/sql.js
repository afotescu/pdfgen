export default {
    checkTasks: `SELECT task_id FROM calc_tasks WHERE status=\'GENR\' AND payslip_exists = false
     ORDER BY task_id LIMIT 1`,
    writeToArchive: `INSERT INTO arch_files(ee_id, run_id, run_version_id, wc_id, le_id, contract_id,
                    file_name, file_path, doc_type) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    finishTask: 'UPDATE calc_tasks SET payslip_exists = true WHERE task_id = $1',
    getStaticFields: `SELECT position_x, position_y, type, null as VALUE FROM payslip_data_positioning pdp
JOIN payslip_layout_le pll ON 
pll.payslip_layout_id = pdp.payslip_layout_id OR pll.payslip_layout_term_id = pdp.payslip_layout_id
JOIN payslip_pictures pp ON pp.le_id = pll.le_id`,
    getConfigurationData: `SELECT DISTINCT co.payslip_id, ec.le_id, ec.ee_id, ec.wc_id 
    FROM calculation_output co
JOIN calc_tasks ct ON ct.run_id = co.run_id AND ct.run_version = co.run_version
JOIN ee_contract ec ON ec.contract_id = co.contract_id
WHERE ct.task_id = $1
ORDER BY payslip_id`,
    test: `SELECT * FROM (
SELECT field, position_x, null as position_y, wt_position_y_start,
font, alignment, size, type, null as position_x_end, null as position_y_end, row_height, wt_sequence
  FROM (
SELECT
CASE 
WHEN pw.amount_convert_to_percent = false 
THEN to_char(SUM(DISTINCT co.amount), '999,999.' || repeat('9', pw.amount_format_decimals)) 
ELSE to_char(SUM(DISTINCT co.amount), '999,999.' || repeat('9', pw.amount_format_decimals) || '%') 
END as field, pw.amount_alignment as alignment, pw.amount_position_x as position_x,
pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 3 AND pdr.position_y_start = pw.wt_position_y_start
WHERE co.payslip_id = 2 AND co.run_id = 1 AND co.run_version = 1 AND co.amount != 0 AND co.amount IS NOT NULL
GROUP BY pw.amount_convert_to_percent, pw.amount_alignment, pw.amount_position_x, pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height
,pw.amount_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
CASE 
WHEN pw.base_convert_to_percent = false 
THEN to_char(SUM(DISTINCT co.base), '999,999.' || repeat('9', pw.base_format_decimals)) 
ELSE to_char(SUM(DISTINCT co.base), '999,999.' || repeat('9', pw.base_format_decimals) || '%') 
END as field, pw.base_alignment as alignment, pw.base_position_x as position_x,
pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 3 AND pdr.position_y_start = pw.wt_position_y_start
WHERE co.payslip_id = 2 AND co.run_id = 1 AND co.run_version = 1 AND co.base != 0 AND co.base IS NOT NULL
GROUP BY pw.base_convert_to_percent, pw.base_alignment, pw.base_position_x, pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height
,pw.base_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
CASE 
WHEN pw.rate_convert_to_percent = false 
THEN to_char(SUM(DISTINCT co.rate), '999,9990.' || repeat('9', pw.rate_format_decimals)) 
ELSE to_char(SUM(DISTINCT co.rate), '999,9990.' || repeat('9', pw.rate_format_decimals) || '%') 
END as field, pw.rate_alignment as alignment, pw.rate_position_x as position_x,
pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 3 AND pdr.position_y_start = pw.wt_position_y_start
WHERE co.payslip_id = 2 AND co.run_id = 1 AND co.run_version = 1 AND co.rate != 0 AND co.rate IS NOT NULL
GROUP BY pw.rate_convert_to_percent, pw.rate_alignment, pw.rate_position_x, pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height
,pw.rate_format_decimals, co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
wt.payslip_text as field, pw.text_alignment as alignment, pw.text_position_x as position_x,
pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 3 AND pdr.position_y_start = pw.wt_position_y_start
JOIN wt_text wt ON wt.wt_id = pw.wt_id
WHERE co.payslip_id = 2 AND co.run_id = 1 AND co.run_version = 1 
GROUP BY wt.payslip_text, pw.text_alignment, pw.text_position_x, pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height,
co.wt_id,  pw.wt_sequence
UNION ALL
SELECT
null as field, pw.ref_alignment as alignment, pw.ref_position_x as position_x,
pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height, 'text' as type, co.wt_id,  pw.wt_sequence
FROM calculation_output co
JOIN payslip_wt pw ON pw.wt_id = co.wt_id
JOIN payslip_dynamic_rows pdr ON pdr.payslip_layout_id = 3 AND pdr.position_y_start = pw.wt_position_y_start
WHERE co.payslip_id = 2 AND co.run_id = 1 AND co.run_version = 1 AND co.ref_period_id is not null
GROUP BY  pw.ref_alignment, pw.ref_position_x, pw.wt_position_y_start, pw.font, pdr.size, pdr.row_height,
co.wt_id,  pw.wt_sequence, co.ref_period_id
) AS st 
UNION ALL
SELECT field, position_x, position_y, null as wt_position_y_start, font, alignment, size, type, position_x_end, position_y_end, null as row_height, null as wt_sequence
FROM payslip_data_positioning
) sa
order by case when wt_position_y_start is not null then wt_sequence else position_y end, position_x`,
};
