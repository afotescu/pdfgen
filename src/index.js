import Promise from 'bluebird';
import Pg from 'pg-promise';
import { EventEmitter } from 'events';
import readline from 'readline';
import mkdirp from 'mkdirp';
import fs from 'fs';
import path from 'path';
import { format } from 'util';
import helpers from './helpers';
import Timer from './timer';
import config from './config';
import pdf from './pdfGenerator';

const MOVE_LEFT = new Buffer('1b5b3130303044', 'hex').toString();
// const MOVE_UP = new Buffer('1b5b3141', 'hex').toString();
const CLEAR_LINE = new Buffer('1b5b304b', 'hex').toString();

let db = new Pg({ promiseLib: Promise });
db = db(config.connectionString);
const app = new EventEmitter();
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});
Promise.promisifyAll(mkdirp);
Promise.promisifyAll(fs);

const wrtArch = `INSERT INTO arch_files(ee_id, run_id, run_version_id, wc_id, le_id, contract_id,
                    file_name, file_path, doc_type) VALUES `;

process.on('uncaughtException', (err) => {
    console.log(err.message);
    app.emit('idle');
});

rl.on('line', (line) => {
    switch (line) {
        case 'uptime':
            console.log(new Date(1000 * process.uptime()).toISOString().substr(11, 8));
            break;
        case 'test':
            app.emit('testData');
            break;
        case 'exit':
            console.log('Shutting down the application.');
            process.exit();
            break;
        default:
            process.stdout.write('');
            break;
    }
});
// Main event which will check every second the database for tasks
app.on('idle', () => {
    const dot = '.';
    let i = 0;
    const checkTasks = new Timer(() => {
        checkTasks.stop();
        // readline.clearLine();
        // readline.cursorTo(0);
        i = (i + 1) % 4;
        db.oneOrNone(helpers.sqlFromFile(path.join(__dirname, 'sql', 'checkTasks.sql')))
            .then((task) => {
                if (task) {
                    console.log(`${MOVE_LEFT + CLEAR_LINE}Task found. Processing data.`);
                    return app.emit('processData', task);
                }
                process.stdout.write(`${MOVE_LEFT + CLEAR_LINE}Waiting for tasks${dot.repeat(i)}`);
                return checkTasks.start();
            })
            .catch((err) => {
                console.log(err.message);
                process.exit();
            });
    }, 1000);
});

// Processing the tasks
app.on('processData', (task) => {
    const folders = [];
    let generateWcPdfs = false;
    let arrOfGeneralData = null;
    let writeToArchiveQuery = wrtArch;
    const imgPath = path.join(__dirname, 'logo.jpg');
    console.log('Please wait...');
    db.oneOrNone(helpers.sqlFromFile(path.join(__dirname, 'sql', 'getLeImage.sql')), task.le_id)
        .then((data) => {
            fs.writeFileAsync(imgPath, data.picture.toString('binary'), 'base64');
        })
        .then(() => db.none(helpers.sqlFromFile(path.join(__dirname, 'sql', 'clearRunFromArchive.sql')),
            [task.run_id, task.run_version]))
        .then(() => db.any(helpers.sqlFromFile(path.join(__dirname, 'sql', 'getConfigurationData.sql')),
            [task.task_id]))
        .then((results) => {
            arrOfGeneralData = results;
            const arrOfFolders = [];
            for (let i = 0; i < results.length; i += 1) {
                const defaultPayslipName =
                    results[i].termination ? 'PAY_run%s_ver%s_%s_term.pdf' : 'PAY_run%s_ver%s_%s.pdf';
                const folderPath = path.join(config.archive,
                    `le_${results[i].le_id.toString()}`,
                    task.code,
                    `ee_${results[i].ee_id.toString()}`
                );
                const fileName = format(defaultPayslipName,
                    helpers.leftpad(results[i].run_id.toString(), 7, 0),
                    helpers.leftpad(results[i].run_version.toString(), 7, 0),
                    results[i].payslip_id.toString()
                );
                const folderRelativePath = path.join(
                    `le_${results[i].le_id.toString()}`,
                    task.code,
                    `ee_${results[i].ee_id.toString()}`
                );
                const fullPath = path.join(folderPath, fileName);
                folders.push(fullPath);
                arrOfFolders
                    .push(mkdirp(folderPath));
                if (i === results.length - 1) {
                    writeToArchiveQuery += `(${results[i].ee_id}, ${results[i].run_id}, ${results[i].run_version},
                    ${results[i].wc_id}, ${results[i].le_id}, ${results[i].contract_id},
                    '${fileName}', '${folderRelativePath}', 'PAY');`;
                } else {
                    writeToArchiveQuery += `(${results[i].ee_id}, ${results[i].run_id}, ${results[i].run_version},
                    ${results[i].wc_id}, ${results[i].le_id}, ${results[i].contract_id},
                    '${fileName}', '${folderRelativePath}', 'PAY'),`;
                }
            }
            return Promise.all(arrOfFolders);
        })
        .then(() => {
            const arrOfPdfData = [];
            for (let i = 0; i < arrOfGeneralData.length; i += 1) {
                arrOfPdfData.push(db.any(helpers.sqlFromFile(path.join(__dirname, 'sql', 'getPdfData.sql')),
                    [arrOfGeneralData[i].payslip_id, arrOfGeneralData[i].run_id, arrOfGeneralData[i].run_version,
                        arrOfGeneralData[i].ee_id, arrOfGeneralData[i].le_id, arrOfGeneralData[i].payslip_layout_id,
                        arrOfGeneralData[i].wc_id, task.code, arrOfGeneralData[i].ee_contract]));
            }
            return Promise.all(arrOfPdfData);
        })
        .then((data) => {
            for (let i = 0; i < data.length; i += 1) {
                pdf.generatePDF(folders[i], data[i], imgPath);
            }
            return Promise.resolve();
        })
        .then(() => db.none(writeToArchiveQuery))
        .then(() => db.oneOrNone(helpers.sqlFromFile(path.join(__dirname, 'sql', 'leConcatFiles.sql')),
            [task.run_id, task.run_version]))
        .then((files) => {
            const payslipName = 'PAY_run%s_ver%s.pdf';
            pdf.concatPDFs(path.join(`le_${task.le_id}`, task.code, format(payslipName,
                helpers.leftpad(task.run_id.toString(), 7, 0),
                helpers.leftpad(task.run_version.toString(), 7, 0))
            ), files.files, task.payslip_password);
            writeToArchiveQuery = wrtArch;
            writeToArchiveQuery += `(NULL,${task.run_id}, ${task.run_version}, NULL, ${task.le_id}, NULL,
            '${format(payslipName,
                helpers.leftpad(task.run_id.toString(), 7, 0),
                helpers.leftpad(task.run_version.toString(), 7, 0))}', 
                '${path.join(`le_${task.le_id}`, task.code)}', 'PAY')`;
            return db.any(helpers.sqlFromFile(path.join(__dirname, 'sql', 'checkWorkCenters.sql')),
                [task.run_id, task.run_version]);
        })
        .then((wcs) => {
            if (wcs.length > 1) {
                generateWcPdfs = true;
                const arrOfWcsFiles = [];
                for (let i = 0; i < wcs.length; i += 1) {
                    arrOfWcsFiles.push(db.oneOrNone(helpers.sqlFromFile(
                        path.join(__dirname, 'sql', 'wcConcatFiles.sql')),
                        [task.run_id, task.run_version, wcs[i].wc_id]));
                }
                return Promise.all(arrOfWcsFiles);
            }
            return Promise.resolve();
        })
        .then((results) => {
            if (generateWcPdfs) {
                const payslipName = '%s run%s_ver%s.pdf';
                for (let i = 0; i < results.length; i += 1) {
                    pdf.concatPDFs(path.join(`le_${task.le_id}`, task.code, format(payslipName,
                        results[i].wc_name,
                        helpers.leftpad(task.run_id.toString(), 7, 0),
                        helpers.leftpad(task.run_version.toString(), 7, 0))),
                        results[i].files, task.payslip_password);
                    writeToArchiveQuery += `, (NULL,${task.run_id}, ${task.run_version}, ${results[i].wc_id},
                        ${task.le_id}, NULL,
                        '${format(payslipName,
                        results[i].wc_name,
                        helpers.leftpad(task.run_id.toString(), 7, 0),
                        helpers.leftpad(task.run_version.toString(), 7, 0))}', 
                        '${path.join(`le_${task.le_id}`, task.code)}', 'PAY')`;
                }
            }
        })
        .then(() => db.none(writeToArchiveQuery))
        .then(() => db.none(helpers.sqlFromFile(path.join(__dirname, 'sql', 'closeTask.sql')), [task.task_id]))
        .then(() => {
            console.log('Task for run_id %s and run_version %s is done', task.run_id, task.run_version);
            console.log('Going back for searching tasks\n');
            return app.emit('idle');
        })
        .catch((err) => {
            console.log(err.message);
            process.exit();
        });
});


// Testing all data from the tasks
// app.on('testAllData', (task) => {
//     console.time('generated in');
//     db.any(query.getConfigurationData, [task])
//         .then((data) => {
//             for (let i = 0; i < data.length; i += 1) {
//                 db.any(query.test,
//                     [data[i].payslip_id, data[i].run_id, data[i].run_version, data[i].ee_id, data[i].le_id,
//                         data[i].payslip_layout_id, data[i].wc_id])
//                     .then((result) => {
//                         console.log('------------->', i);
//                         generate(`test${i}.pdf`, result);
//                     })
//                     .catch((err) => {
//                         console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', i);
//                         console.log(err.message);
//                     });
//             }
//             console.timeEnd('generated in');
//         })
//         .catch((err) => {
//             console.log(err.message);
//             process.exit();
//         });
// });

// Test the data
// app.on('testData', (task) => {
//     console.time('generated in');
//     db.any(query.testFixed)
//         .then((results) => {
//             generate(`generate.pdf`, results);
//             console.timeEnd('generated in');
//         })
//         .catch((err) => {
//             console.log(err.message);
//             process.exit();
//         });
// });

app.emit('idle');
