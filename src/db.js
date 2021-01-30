const sqlite3 = require('sqlite3').verbose()
const path = require('path')

const pathDB = path.resolve('db', 'test.sqlite')

const connectDB = () => {
  const db = new sqlite3.Database(pathDB, sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
      return console.error(err)
    }
    console.log('Connected to the in-memory SQlite database.')
  })

  return db
}

const createTables = (db) => {
  const USERS_TABLE_SCRIPT = `
  CREATE TABLE IF NOT EXISTS users(
    id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    gender TEXT,
    ip_address TEXT
  );
  `

  const USERS_STATISTICS_TABLE_SCRIPT = `
  CREATE TABLE IF NOT EXISTS users_statistic(
    user_id INTEGER,
    clicks INTEGER,
    page_views INTEGER,
    date TEXT,
    
    FOREIGN KEY (user_id) 
      REFERENCES users (id) 
         ON DELETE CASCADE 
         ON UPDATE NO ACTION
  );
  `

  console.log('Creating tables')

  db.run(USERS_TABLE_SCRIPT)
  db.run(USERS_STATISTICS_TABLE_SCRIPT)
}

const insertUsersIfITtIsEmpty = (db) => {
  const sql = `SELECT * FROM users`

  db.get(sql, [], (err, row) => {
    if (err) {
      return console.error(err)
    }

    if (row) {
      return console.log('Skipping inserting users to DB')
    }

    const users = require('../data/users.json')

    const stmt = db.prepare('INSERT INTO users VALUES (?,?,?,?,?,?)')

    users.forEach(user => {
      stmt.run([user.id, user.first_name, user.last_name, user.email, user.gender, user.ip_address])
    })

    stmt.finalize()
  })
}

const insertUserStatisticIfITtIsEmpty = (db) => {
  const sql = `SELECT * FROM users_statistic`

  db.get(sql, [], (err, row) => {
    if (err) {
      return console.error(err)
    }

    if (row) {
      return console.log('Skipping inserting user_statistic to DB')
    }

    const userStatistics = require('../data/users_statistic.json')

    const stmt = db.prepare('INSERT INTO users_statistic VALUES (?,?,?,?)')

    userStatistics.forEach(users_statistic => {
      stmt.run([users_statistic.user_id, users_statistic.clicks, users_statistic.page_views, users_statistic.date])
    })

    stmt.finalize()
  })
}

const paginateUserStatistics = (db, params, res) => {
  const sql = `
  SELECT
       users.id,
       users.first_name,
       users.last_name,
       users.email,
       users.gender,
       users.ip_address,
       SUM(us.clicks) AS total_clicks,
       SUM(us.page_views) AS total_page_views
  FROM users
      INNER JOIN users_statistic AS us
          ON us.user_id = users.id
  GROUP BY users.id
  LIMIT ?
  OFFSET ?;
  `

  const offset = params.limit * (params.page - 1)

  db.all(sql, [params.limit, offset], (err, rows) => {
    if(err) {
      return console.error(err)
    }

    res.json(rows)
  })
}

const getUserStatistic = (db, params, res) => {
  const sql = `
  SELECT * FROM users_statistic WHERE user_id = ?;
  `

  db.all(sql, [params.user_id], (err, rows) => {
    if(err) {
      return console.error(err)
    }

    res.json(rows)
  })
}

const getUserStatisticByDate = (db, params, res) => {
  const sql = `
  SELECT * FROM users_statistic WHERE user_id = ? AND date >= DATE(?) AND date <= DATE(?);
  `

  db.all(sql, [params.user_id, params.start_date, params.end_date], (err, rows) => {
    if(err) {
      return console.error(err)
    }

    res.json(rows)
  })
}


module.exports = {
  connectDB,
  createTables,
  insertUsersIfITtIsEmpty,
  insertUserStatisticIfITtIsEmpty,
  paginateUserStatistics,
  getUserStatistic,
  getUserStatisticByDate
}
