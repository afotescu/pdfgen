import Promise from 'bluebird';
import Pg from 'pg-promise';
import { EventEmitter } from 'events';
import readline from 'readline';
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

process.on('uncaughtException', (err) => {
    console.log(err.message);
    app.emit('idle');
});
rl.on('line', (line) => {
    switch (line) {
        case 'uptime':
            console.log(new Date(1000 * process.uptime()).toISOString().substr(11, 8));
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
                    return app.emit('processData', task.task_id);
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
    console.time('generated in');
    db.any(query.getConfigurationData, [task])
        .then((data) => {
            const arrOfPdfData = [];
            for (let i = 0; i < data.length; i += 1) {
                arrOfPdfData.push(db.any(query.test,
                    [data[i].payslip_id, data[i].run_id, data[i].run_version, data[i].ee_id, data[i].le_id,
                        data[i].payslip_layout_id]));
            }
            return Promise.all(arrOfPdfData);
        })
        .then((results) => {
            for (let i = 0; i < results.length; i += 1) {
                generate(`test${i}.pdf`, results[i]);
            }
            console.timeEnd('generated in');
        })
        .catch((err) => {
            console.log(err.message);
            process.exit();
        });
});

app.emit('idle');
