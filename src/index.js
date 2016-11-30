import Promise from 'bluebird';
import Pg from 'pg-promise';
import { EventEmitter } from 'events';
import readline from 'readline';
// import hummus from 'hummus';
// import path from 'path';
import Timer from './timer';
import config from './config';

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
    console.log('Waiting for tasks...');
    const checkTasks = new Timer(() => {
        checkTasks.stop();
        db.any('SELECT task_id FROM calc_tasks WHERE status = \'REPT\'')
            .then((tasks) => {
                if (tasks.length) {
                    console.log(`Found ${tasks.length}. Processing data.`);
                    return app.emit('resolveTask', tasks);
                }
                return checkTasks.start();
            })
            .catch((err) => {
                console.log(err.message);
                app.emit('idle');
            });
    }, 1000);
});

// Processing the tasks
app.on('resolveTask', (tasks) => {
    db.any('UPDATE calc_tasks SET status = \'COMP\' WHERE task_id = $1', [tasks[0].task_id])
        .then(() => {
            setTimeout(() => {
                console.log('Finished current tasks.\n');
                console.log('\n\n');
                app.emit('idle');
            }, 2000);
        })
        .catch((err) => {
            console.log(err.message);
            app.emit('idle');
        });
});

app.emit('idle');
