import Promise from 'bluebird';
import Pg from 'pg-promise';
import { EventEmitter } from 'events';
import readline from 'readline';
// import hummus from 'hummus';
// import path from 'path';
import Timer from './timer';
import config from './config';
import query from './sql';

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
                app.emit('idle');
            });
    }, 1000);
});

// Processing the tasks
app.on('processData', (task) => {
    db.any(query.getConfigurationData, [task])
        .then((data) => {
            console.log(data);
        })
        .catch((err) => {
            console.log(err.message);
            app.emit('idle');
        });
});

app.emit('idle');
