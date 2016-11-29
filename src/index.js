import Promise from 'bluebird';
import Pg from 'pg-promise';
import hummus from 'hummus';
import path from 'path';
import config from './config';

let db = new Pg({ promiseLib: Promise });
db = db(config.connectionString);