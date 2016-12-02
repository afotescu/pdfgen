import Promise from 'bluebird';
import Pg from 'pg-promise';
import { EventEmitter } from 'events';
import readline from 'readline';
import mkdirp from 'mkdirp';
import path from 'path';
import { format } from 'util';
import helpers from './helpers';
import Timer from './timer';
import config from './config';
import query from './sql';
import generate from './pdfGenerator';

let db = new Pg({ promiseLib: Promise });
db = db(config.connectionString);
const app = new EventEmitter();
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});
Promise.promisifyAll(mkdirp);

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
    const checkTasks = new Timer(() => {
        checkTasks.stop();
        db.oneOrNone(query.checkTasks)
            .then((task) => {
                if (task) {
                    console.log('Task found. Processing data.');
                    // return app.emit('processData', task.task_id);
                    return app.emit('processData', task);
                }
                console.log('Waiting for tasks...');
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
    console.time('Done');
    const folders = [];
    let arrOfGeneralData = null;
    let writeToArchiveQuery = query.writeToArchive;
    db.none(query.clearRunFromArchive, [task.run_id, task.run_version])
        .then(() => db.any(query.getConfigurationData, [task.task_id]))
        .then((results) => {
            arrOfGeneralData = results;
            const arrOfFolders = [];
            for (let i = 0; i < results.length; i += 1) {
                const defaultPayslipName =
                    results[i].termination ? 'PAY_run%s_ver%s_%s_term.pdf' : 'PAY_run%s_ver%s_%s.pdf';
                const folderPath = path.join(config.archive,
                    `le_${results[i].le_id.toString()}`,
                    `ee_${results[i].ee_id.toString()}`
                );
                const fileName = format(defaultPayslipName,
                    helpers.leftpad(results[i].run_id.toString(), 7, 0),
                    helpers.leftpad(results[i].run_version.toString(), 7, 0),
                    results[i].payslip_id.toString()
                );
                const fullPath = path.join(folderPath, fileName);
                folders.push(fullPath);
                arrOfFolders
                    .push(mkdirp(folderPath));
                if (i === results.length - 1) {
                    writeToArchiveQuery += `(${results[i].ee_id}, ${results[i].run_id}, ${results[i].run_version},
                    ${results[i].ee_id}, ${results[i].le_id}, ${results[i].contract_id},
                    '${fileName}', '${folderPath}', 'PAY');`;
                } else {
                    writeToArchiveQuery += `(${results[i].ee_id}, ${results[i].run_id}, ${results[i].run_version},
                    ${results[i].ee_id}, ${results[i].le_id}, ${results[i].contract_id},
                    '${fileName}', '${folderPath}', 'PAY'),`;
                }

            }
            return Promise.all(arrOfFolders);
        })
        .then(() => {
            const arrOfPdfData = [];
            for (let i = 0; i < arrOfGeneralData.length; i += 1) {
                arrOfPdfData.push(db.any(query.test,
                    [arrOfGeneralData[i].payslip_id, arrOfGeneralData[i].run_id, arrOfGeneralData[i].run_version,
                        arrOfGeneralData[i].ee_id, arrOfGeneralData[i].le_id, arrOfGeneralData[i].payslip_layout_id,
                        arrOfGeneralData[i].wc_id]));
            }
            return Promise.all(arrOfPdfData);
        })
        .then((data) => {
            for (let i = 0; i < data.length; i += 1) {
                generate(folders[i], data[i]);
            }
            return Promise.resolve();
        })
        .then(() => db.none(writeToArchiveQuery))
        .then(() => {
            console.timeEnd('Done');
        })
        .catch((err) => {
            console.log(err.message);
            process.exit();
        });
});


// Testing all data from the tasks
app.on('testAllData', (task) => {
    console.time('generated in');
    db.any(query.getConfigurationData, [task])
        .then((data) => {
            for (let i = 0; i < data.length; i += 1) {
                db.any(query.test,
                    [data[i].payslip_id, data[i].run_id, data[i].run_version, data[i].ee_id, data[i].le_id,
                        data[i].payslip_layout_id, data[i].wc_id])
                    .then((result) => {
                        console.log('------------->', i);
                        generate(`test${i}.pdf`, result);
                    })
                    .catch((err) => {
                        console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', i);
                        console.log(err.message);
                    });
            }
            console.timeEnd('generated in');
        })
        .catch((err) => {
            console.log(err.message);
            process.exit();
        });
});

// Test the data
app.on('testData', (task) => {
    console.time('generated in');
    db.any(query.testFixed)
        .then((results) => {
            generate(`generate.pdf`, results);
            console.timeEnd('generated in');
        })
        .catch((err) => {
            console.log(err.message);
            process.exit();
        });
});

app.emit('idle');
