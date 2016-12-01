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
};
